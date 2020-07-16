---
title: "Chapter 3 - Multiple Clients"
date: 2020-07-06
lastmod: 2020-07-06T11:55:02-04:00
draft: false
keywords: []
comment: false
summary: "In this chapter we will add support for efficient handling of multiple clients connected simultaneously. We will first isolate the problematic elements of the current implementation and explore different solutions before getting to the final one using the select syscall"
---

## What we'll cover

In this chapter we will add support for efficient handling of multiple clients connected simultaneously. We will first isolate the problematic elements of the current implementation and explore different solutions before getting to the final one using the [`select`][select-syscall] syscall.

## First problem, accepting multiple clients

Let's start with the new client problem. Our goal is the following:

Regardless of the state of the server, or what it might be doing, or whether other clients are already connected, new clients should be able to establish a new connection, and keep the connection open as long as they wish, until they either disconnect on purpose or a network issue occurs.

We want our server to keep client connections alive until clients disconnect. This is what Redis does, it keeps the connection alive until the client closes the connection, either explicitly with the [QUIT][redis-documentation-quit] command or as a side effect of the process that had started the connection being stopped.

Let's do this by removing the `client.close` line, we will add it back when we add a handler for the `QUIT` command, but let's set that aside for now.

This is what the main server loop looks like now, after removing the `client.close` line:

``` ruby
loop do
  client = server.accept
  puts "New client connected: #{ client }"
  client_command_with_args = client.gets
  if client_command_with_args && client_command_with_args.strip.length > 0
    response = handle_client_command(client_command_with_args)
    client.puts response
  else
    puts "Empty request received from #{ client }"
  end
end
```

The server starts, waits for a client to connect, and then handles the requests from that client, nothing changed. Once the server is done writing the response back, it starts doing the same thing again, waiting for a new client to connect, not keeping track of the first client. As far as the server knows, this client is still connected, but we have no way of reading future commands from this one client now.

Because we're not closing the connection after writing back a response, we need the server to keep track of all the clients that are currently connected.

``` ruby
# ...
def initialize
  @clients = []
end
# ...
loop do
  client = server.accept
  @clients << client
  puts "New client connected: #{ client }"
  # ...
end
```

Every time a client connects, we add it to the `@clients` array. The rest of the loop is the same, when the first iteration ends, we go back to the beginning and wait for a new client. But what if the first client sends a request in the meantime? Future commands from the first client will be ignored for two reasons. First, the server is currently waiting, potentially forever, for a new client to connect. Even though we did save the client object in the `@clients` array, we never actually read from this array.

It is starting to look like waiting for clients to connect and trying to handle connected clients in the same loop is quite problematic, especially with all these blocking calls that potentially wait forever.

One approach could be to timebox these blocking calls, to make sure they don't block the server while there might be other things to do, such as responding to another client. We could start a second thread, make it loop until either a new client connects or  an arbitrary duration has elapsed and raise an exception when it has:

``` ruby
timeout = Time.now.to_f + 5
server_accepted = false

Thread.abort_on_exception = true
Thread.new do
  while server_accepted == false
    sleep 0.001
    if Time.now.to_f > timeout
      raise "Timeout!"
    end
  end
end

server.accept
server_accepted = true
```

We create a new thread that will loop as long as the `server.accept` has not returned or until five seconds have elapsed. This means that the call to accept will not run for more than five seconds. The `abort_on_exception` setting is necessary, otherwise an uncaught exception in a Thread does not propagate to the parent thread, the thread would silently fail, not interrupting the `accept` call.

Any clients connecting to the server within five seconds will prevent the `"Timeout!"` exception from being thrown, beecause the `server_accepted` flag will be set to `true`.

As it turns out, we don't have to write this, Ruby gives us the [`Timeout` module][ruby-doc-timeout], which does pretty much the same thing, and throws an exception if the block hasn't finished after the given timeout:

``` rubyn
require 'timeout'
Timeout.timeout(5) do
  server.accept
end
```

The Timeout module has received [a fair amount of criticism][sidekiq-timeout-blog] of the past few years. There are a few other posts out there if you search for the following keywords: "ruby timeout module dangerous" and we should absolutely follow their recommendation.

Looking back at our primitive timeout implementation above, if the second thread enters the `if Time.now.to_f > timeout` condition, it will then throw an exception, but it is entirely possible that a client would connect at the exact same time, and the exception being thrown by the second thread would interrupt the parent thread, as it is creating the connection and effectively prevent the server from completing the `accept` call. The odds would be fairly unlikely, but it would still be possible.

Another more important issue is the impact on performance. Using timeouts means that the server could end up waiting a lot, while being blocked. If a client connects and sends two commands, we would want the server to respond as fast as possible to both of these commands. Ideally in a matter of milliseconds, this is what the real Redis would do. But with the timeout approach we were looking at, the server would accept the connection, read the first command, respond to it, and then loop again, waiting for a new connection, while the client is waiting for the response to the second command.

```
+---------+              +---------+
| Client  |              | Server  |
+---------+              +---------+
     |                        |
     |                        | Accepting Clients with a timeout
     |                        |---------------------------------
     |                        |                                |
     |                        |<--------------------------------
     |                        |
     | Connects               |
     |----------------------->|
     |                        | ------------------\
     |                        |-| Client accepted |
     |                        | |-----------------|
     |                        |
     | "GET 1"                |
     |----------------------->|
     |                        |
     |                        | Processing GET command
     |                        |-----------------------
     |                        |                      |
     |                        |<----------------------
     |                        |
     |                "(nil)" |
     |<-----------------------|
     |                        | ------------------------------------\
     |                        |-| Back to the beginning of the loop |
     |                        | |-----------------------------------|
     |                        |
     | "SET name pierre"      |
     |----------------------->|
     |                        |
     |                        | Accepting Clients with a timeout
     |                        |---------------------------------
     |                        |                                |
     |                        |<--------------------------------
     |                        | ---------------------------------\
     |                        |-| No clients found after timeout |
     |                        | |--------------------------------|
     |                        |
     |                        | Processing SET command
     |                        |-----------------------
     |                        |                      |
     |                        |<----------------------
     |                        |
     |                   "OK" |
     |<-----------------------|
     |                        |
```
_figure 3.1: A sequence diagram showing the unnecessary delay introduced by the timeout approach_

We fixed the blocked problem, but the server is still inefficient. Even a short timeout on connect, like 10ms, would still add a delay of 10ms to the `SET` command in the example above. We can improve this.

Let's try another approach to allow the server to accept new clients while still being able to handle incoming requests from connected clients.

We are going to create a second thread, dedicated to accepting new clients, the main loop will now only be used to read from clients and write responses back:

``` ruby
def initialize
  @clients = []
  @data_store = {}

  server = TCPServer.new 2000
  puts "Server started at: #{ Time.now }"
  Thread.new do
    loop do
      new_client = server.accept
      @clients << new_client
    end
  end

  loop do
    @clients.each do |client|
      begin
        client_command_with_args = client.gets
        if client_command_with_args.nil?
          @clients.delete(client)
        elsif client_command_with_args.strip.empty?
          puts "Empty request received from #{ client }"
        else
          response = handle_client_command(client_command_with_args)
          client.puts response
        end
      rescue Errno::ECONNRESET
        @clients.delete(client)
      end
    end
  end
end
```


Let's go through the main changes:

### `Thread.new` in the constructor

As soon as the server starts, we create a new thread, which does only one thing, accept new clients. This second thread starts an infinite loop, inside the loop we call `accept`, and block until it returns a new client. When we do receive a new client, we add it to the `@clients` instance variable, so that it can be used from the main thread, in the main loop.

By moving the blocking call to `accept` to a different thread, we're not blocking the main loop with the `accept` call anymore. There are still issues with this implementation, `gets` is also a blocking call. We're improving things one step at a time.

### `client_command_with_args.nil?`

The main loop is pretty different now. We start by iterating through the `@clients` array. The idea being that on each iteration of `loop`, we want to give each of the connected clients a chance to be handled.

A `nil` value returned by gets means that we reached the end of the file, often called `EOF`. We can learn more about this in the documentation of the `eof?` method defined on [`IO`][ruby-doc-io-eof?], it is describes as:

> Returns true if ios is at end of file that means there are no more data to read. The stream must be opened for reading or an IOError will be raised.

In our case, we will see a `nil` value if the client either explicitly closed the connection with the `close` method on `IO` or if the process that started the connection was killed.

This condition is essentially a first check to make sure that the client referenced by the `client` variable is still connected.

One way to think about it is to imagine a phone call, if you started a phone call, left your phone on your desk to go pick up a pen and came back, you would probably resume by asking something like: "Are you still there?" and only if the person on the other end says yes, you would proceed to continue the conversation, if you don't hear anything, you would assume they hung up. If you only know smartphones, then this analogy might not make a lot of sense, because the screen would tell you if the call is still on. Believe me, there were phones without screens at some point, but you could also imagine that the screen was locked when you picked up the phone. Work with me here, please!

If `gets` returns `nil`, there's no one on the other end anymore, the client hung up, we remove the entry from the list of connected clients.

### rescue ECONNRESET

I am honestly not entirely sure about all the conditions that can cause this error, but I was able to trigger it if the client disconnects while we're blocked on the `gets` call, but only once some data was previously sent. In this case an `Errno::ECONNRESET` exception is raised. We catch it and remove the client we were handling when this happens, as it means that the connection cannot be used anymore.

To reproduce this error, you can start the server with `ruby -r"./server_accept_thread" -e "BasicServer.new"` and run the following in an `irb` shell:

```
irb(main):051:0> socket = TCPSocket.new 'localhost', 2000
irb(main):052:0> socket.puts "GET 1"
=> nil
irb(main):053:0> socket.close
```

{{% admonition info "Clients, Servers and failures" %}}

When dealing with clients & servers, that is, code running in different processes, and potentially not running on the same machine, it is important to remember that a piece of code running on one machine can never really be sure that the other ones are in the state that they expect. The main difference with running code in a single process is that when two pieces of code run in difference processes, they do not share memory, you can't create a variable in one, and read its value from the other. On top of that, each process has its own life cycle, one process might be stopped, for various reasons, while the other might still be running.

In concrete terms, it means that when we write code that will run on the server part, which is what we're doing here, we always have to keep in mind that a client that has connected in the past, may have disconnected by the time the server tries to communicate with it. There might be various reasons, to name a few, the client may have explicitly closed the connection, a network issue may have happened, causing the connection to be accidentally closed, or maybe the client code had an internal error, such as an exception being thrown and the process died.

After creating the `client` variable, we have absolutely no guarantee that the client process on the other side is still connected. It is reasonable to assume that the client is still connected soon after when we call `client.gets`, and while unlikely, it's still important to keep in mind that the network communication might still fail.

But what about later on, on the next iteration, and so on? We always have to expect that things might fail if we want our server to handle all the possible scenarios it might find itself in. This is what the check for `nil` and what the `rescue Errno::ECONNRESET` do.

{{% /admonition %}}


### The rest

The `else` branch inside the main loop is identical to what we started this chapter with, we use the blocking method `gets` to read from the client, and we write back a response.

### Still problematic

We made a lot progress but there are still many issues with the last version we looked at. `gets` is a blocking call, and we iterate over the connected clients sequentially. If two clients connect to the server, client1 first and client2 second, but client1 never sends a command, client2 will never get a chance to communicate with the server. The server will wait forever on the `client.gets` call for client1.

We need to fix this.

## Trying timeouts again

There are different ways to make sure that all the connected clients get a chance to communicate with the server and to send their commands. Let's start with an approach we looked at earlier, timeouts.

The pros and cons of using timeouts here are the same as they were when explored it as an option to prevent `accept` from blocking the server.

It would be fairly inefficient to do so, even with a short timeout, we would wait for the timeout duration on each client, even when there's nothing to read. It might be fine with a handful of clients, but with a hundred clients, even a short timeout would be problematic.

Even with a timeout of 10ms, if all the clients are waiting, not sending any commands, and only the 100th connected client sends a command, it would have to wait 990ms (99 clients * 10 ms) before its command is read by the server.

I don't think it is that interesting to spend that much time with this approach since we've already established that it wasn't a good one, but you can experiment with it if you're interested. It is [in the `code` folder on GitHub][gets-with-timeout-gh]

## Read without blocking

The title of this section says it all, we are going to use a non-blocking alternative, the explicitly named [`read_nonblock`][ruby-doc-io-read-nonblock]. A key difference is that it requires an int argument to set the maximum number of bytes that will be read from the socket. For reasons that I can't explain, it seems to be common practice to set it as a power of two. We could set it to a very low value, like 4, but then we wouldn't be able to read a whole `SET` command in one call. `SET 1 2` is seven bytes long. We could also set it to a very high value, like 4,294,967,296 (2^32), but then we would expose ourselves to instantiating a String of up to that length if a client decided to send one that large.

As a quick non-scientific example, this would require about 4GB of RAM on the machine running the server. I confirmed this by opening an `irb` shell and monitoring its memory usage, either with `ps aux <pid>`, `top -o MEM` on macOS (`top -o %MEM` on linux) or the Activity Monitor app on macOS, creating a 1,000,000,000 byte long string, with `"a" * 1_000_000_000;`. The semi-colon is important, it returns `nil` and does not try to print the string to the terminal, which would take a little while. I then watched the memory consumption jump from a few megabytes to about one gigabyte.

It seems to be common to choose an arbitrary length, one that is "long enough". Let's pick 256 for now, because we never expect commands to be longer than seven bytes for now, 256 gives us a lot to play with for now.

``` ruby
def initialize
  # ...

  loop do
    @clients.each do |client|
      client_command_with_args = client.read_nonblock(256, exception: false)
      if client_command_with_args.nil?
        @clients.delete(client)
      elsif client_command_with_args == :wait_readable
        # There's nothing to read from the client, we don't have to do anything
        next
      elsif client_command_with_args.strip.empty?
        puts "Empty request received from #{ client }"
      else
        response = handle_client_command(client_command_with_args.strip)
        client.puts response
      end
    end
  end
end
```

Only the content of the main loop changed. It starts the same way, by iterating through the `@clients` array, but the content of the `each` loop is different.

We start by calling `read_nonblock`, with 256 as the `maxlen` argument. The default behavior of `read_nonblock` is to throw different exceptions when encountering eof and when nothing can be read, the `exception: false` argument allows us to instead only rely on the return value:

- If the value is `nil`, we reached `EOF`. It would have raised `EOFError` without the `exception: false` argument
- If the value is the symbol `:wait_readable`, there is nothing to read at the moment. It would have raised `IO::WaitReadable` without the `exception: false` argument.
- Otherwise, it returns up to 256 bytes read from the socket

A major improvement! No more explicit timeouts. That being said, we're still being fairly inefficient by manually cycling through all the clients. Sure, we're not doing that much if there's nothing to read, but we're still doing _something_, calling `read_nonblock`, when we would ideally not do anything given that there's nothing to read.

There's a syscall for that! `select`, described in `man 2 select` as:

> select() examines the I/O descriptor sets whose addresses are passed in readfds, writefds, and errorfds to see if some of their descriptors are ready for reading, are ready for writing, or have an exceptional condition pending, respectively

## Let's use `select`

The `select` syscall is available in Ruby as a class method on the `IO` class : [`IO.select`][ruby-doc-io-select]. It is *blocking by default* and accepts between one and four arguments and returns an array containing three items, each of them being an array as well. Let's look closely at what all these arrays mean:

- The first argument is mandatory, it is an array of sockets, `select` will look through all of them and, for each socket that has _something_ that can be read, will return it in the first of the three arrays returned.
- The second argument is optional, it is also an array of sockets. `select` will look through all of them and, for each socket that can be written to, will return it in the second of the three arrays returned.
- The third argument is optional as well, it is again an array of sockets. `select` will look through all of them and, for each socket that have pending exceptions, will return it in the third of the three arrays returned.
- The fourth argument is optional. By default `select` is blocking, this argument is an integer telling `select` the maximum duration to wait for, and it will return `nil` if it wasn't able to return anything in time.

{{% admonition warning "Third argument to select" %}}

As of this writing, I am not aware of any conditions that would cause a socket to "have a pending exception". I will update this chapter if I learn more about it.

{{% /admonition %}}

The main use case we're interested in is the one related to the first argument. Our `@clients` array is a list of socket. If we pass it to `select` as the first argument, it will return a list of sockets that can be read from.

In Chapter 1 we mentioned how setting timeouts is often a best practice. Here is a good example of when a timeout is not needed. It does not matter if our server waits forever to read for clients, it would only happen if no clients are sending commands. By blocking forever here we're not preventing the server from doing else, we're waiting because there is nothing else to do.

``` ruby
def initialize
  # ...
  loop do
    # Selecting blocks, so if there's no client, we don't have to call it, which would
    # block, we can just keep looping
    if @clients.empty?
      next
    end
    result = IO.select(@clients)
    result[0].each do |client|
      client_command_with_args = client.read_nonblock(1024, exception: false)
      if client_command_with_args.nil?
        @clients.delete(client)
      elsif client_command_with_args == :wait_readable
        # There's nothing to read from the client, we don't have to do anything
        next
      elsif client_command_with_args.strip.empty?
        puts "Empty request received from #{ client }"
      else
        response = handle_client_command(client_command_with_args.strip)
        client.puts response
      end
    end
  end
end
```

It's worth mentioning that the `each` loop that we removed did not exactly disappear, we delegated the iteration to the operating system. We have to assume that `select` does something similar internally, it has to iterate over the given array of file descriptors and do something with them. The difference is that by delegating such operation to the OS, we're not reinventing the wheel but we're also relying on an implementation that we can assume is well optimized.

There's one more problem, and I swear, the next version will be the last one in this chapter. As previously mentioned, `select` blocks by default. If one clients connects, and never sends a command, the call to `IO.select` will never return. Meanwhile the thread dedicated to accepting new clients is still accepting clients, appending them to the `@clients` array.

We could use the timeout argument, handle the case where the return value is nil, but as we discussed through the chapter, using a timeout would be inefficient. Imagine that two clients connect around the same time, the first one to connect does not send a command, the second one does. Regardless of the timeout, the second client would have to wait for the timeout to ellapse until the server acknowledges it. It would be great if the server could be more reactive, and not wait for timeouts.

And the solution is ... `select`, again! I know, I know, this was anticlimactic, but `select` is very versatile.

## `select` everything

Accepting a client is actually a different form of reading from a socket, so if we pass a server socket in the first array to `IO.select`, it will be returned if a new client attempted to connect.

Let's demonstrate this in `irb`:

```
irb(main):001:0> require 'socket'
=> true
irb(main):002:0> server = TCPServer.new(2000)
irb(main):003:0> IO.select [server]
=> [[#<TCPServer:fd 10, AF_INET6, ::, 2000>], [], []]
```

The `select` call will only return after a client connects to the server. In the previous example, I used our good friend `nc` from Chapter 1: `nc -v localhost 2000`.

Let's use this to remove the `accept` thread:

``` ruby
def initialize
  @clients = []
  @data_store = {}

  server = TCPServer.new 2000
  puts "Server started at: #{ Time.now }"

  loop do
    result = IO.select(@clients + [server])
    result[0].each do |socket|
      if socket.is_a?(TCPServer)
        @clients << server.accept
      elsif socket.is_a?(TCPSocket)
        client_command_with_args = socket.read_nonblock(1024, exception: false)
        if client_command_with_args.nil?
          puts "Found a client at eof, closing and removing"
          @clients.delete(socket)
        elsif client_command_with_args == :wait_readable
          # There's nothing to read from the client, we don't have to do anything
          next
        elsif client_command_with_args.strip.empty?
          puts "Empty request received from #{ client }"
        else
          response = handle_client_command(client_command_with_args.strip)
          socket.puts response
        end
      else
        raise "Unknown socket type: #{ socket }"
      end
    end
  end
end
```

And we finally have it, on each iteration we check if any of the connected clients has sent anything as well as whether or not there are new clients attempting to connect.


## But what about the real Redis?

I'm glad you asked, we haven't mentioned Redis in a while, you know, the thing we're trying to replicate. So, how does Redis handle its clients?

Well, I don't know if you're going to like the answer, but ... it depends.

Redis uses different multiplexers (`select` is described in the man page as doing "synchronous I/O mutiplexing"), and tries to find the most efficient one. `select` is [apparently known to have limitations][select-problems] and seems to be limited to 1024 sockets. While it is not a problem for us to be limited to 1023 connected clients (keeping one for the server), it is reasonable to imagine that Redis would want to support more.

It turns out that there are better alternatives, [kqueue][kqueue] on macOS and BSD, [epoll][epoll] on linux and evport on Solaris (I could not find a link for it).

Redis defines its own even library, [`ae`][redis-ae], in the [`ae.c` file][redis-source-ae]. The interface for `ae` is then implemented with each of the libraries mentioned above, [in `ae_epoll.c`][ae-epoll], [in `ae_kqueue.c`][ae-kqueue], [in `ae_evport.c`][ae-evport] and [in `ae_select.c`][ae-select].

Redis [defines constants depending on what is available at compile time][redis-source-multiplexer-constants] and chooses the implementation [in server.c][redis-source-multiplexer-choice].

So, does Redis use `select`, probably not, but it could, if nothing else is available on the system it is being compiled on. The important part is that even if it doesn't, it uses alternatives that are conceptually similar to `select`.

## Conclusion

It took a while and explored a few different options, with threads and timeouts, only to discard them all and use `select` for everything. That may seem like a waste of time but it is not, I think it's extremely important to look at what the alternatives are to fully understand and appreciate the benefit of a given solution.

In the next chapter we'll add more commands to the server to make it a little bit closer to the real Redis server.

### Code

The code from this chapter is [available on GitHub](https://github.com/pjambet/redis-in-ruby/tree/master/code/chapter-3)



[redis-documentation-quit]:https://redis.io/commands/quit
[select-syscall]:https://man7.org/linux/man-pages/man2/select.2.html
[sidekiq-timeout-blog]:https://www.mikeperham.com/2015/05/08/timeout-rubys-most-dangerous-api/
[redis-source-multiplexer-choice]:https://github.com/redis-io/redis/blob/6.0/src/ae.c#L47-L61
[redis-source-multiplexer-constants]:https://github.com/redis-io/redis/blob/6.0/src/config.h#L76-L90
[ruby-doc-io-eof?]:http://ruby-doc.org/core-2.7.1/IO.html#eof-3F-method
[gets-with-timeout-gh]:https://github.com/pjambet/redis-in-ruby/blob/master/code/chapter-3/server_accept_thread_and_gets_timeout.rb
[ruby-doc-io-read-nonblock]:http://ruby-doc.org/core-2.7.1/IO.html#read_nonblock-method
[ruby-doc-io-select]:http://ruby-doc.org/core-2.7.1/IO.html#select-method
[select-problems]:http://www.moythreads.com/wordpress/2009/12/22/select-system-call-limitation/
[kqueue]:https://www.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2
[epoll]:https://linux.die.net/man/4/epoll
[redis-ae]:https://redis.io/topics/internals-rediseventlib
[redis-source-ae]:https://github.com/redis/redis/blob/6.0/src/ae.c
[ae-epoll]:https://github.com/redis-io/redis/blob/6.0/src/ae_epoll.c
[ae-select]:https://github.com/redis-io/redis/blob/6.0/src/ae_select.c
[ae-kqueue]:https://github.com/redis-io/redis/blob/6.0/src/ae_kqueue.c
[ae-evport]:https://github.com/redis-io/redis/blob/6.0/src/ae_evport.c
[ruby-doc-timeout]:http://ruby-doc.org/stdlib-2.7.1/libdoc/timeout/rdoc/Timeout.html
