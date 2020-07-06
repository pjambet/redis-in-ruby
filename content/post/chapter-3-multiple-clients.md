---
title: "Chapter 3 - Multiple Clients"
date: 2020-07-06
lastmod: 2020-07-06T11:55:02-04:00
draft: true
keywords: []
summary: "In this chapter we will improve the server to efficiently handle multiple clients connected at the same time. We will end up using the `select` syscall and explore different alternatives using threads on the way."
---

## What we'll cover

In this chapter we will add support for efficient handling of multiple clients connected simultaneously. We will first isolate the problematic elements of the current implementation and explore different solutions before getting to the final one using the [`select`][select-syscall] syscall.

## First problem, accepting multiple clients

Let's start with the new client problem. The goal is that, regardless of the state of the server, of what it may or may not currently doing, or whether other clients are already connected, new clients should be able to establish a new connection, and keep the connection open as long as they wish, until they either disconnect on purpose or a network issue occurs.

Before attempting to fix the problem, we need to think about what we're trying to achieve, what parts of the current implementation are problematic and only then can we really start thinking about what needs to change.

First things first, we want our server to keep client connections alive until clients disconnect. After all, this is what what Redis does, it keeps the connection alive until the client closes the connection, either explicitly with the [QUIT][redis-documentation-quit] or as a side effect of the process that had started the connection dying.

In order to achieve this we first have to remove the `client.close` line, we will add it back when we add a handler for the `QUIT` command, but let's set that aside for now.

This is what the main server loop looks like now:

``` ruby
loop do
  client = server.accept
  puts "New client connected: #{ client }"
  client_command_with_args = client.gets
  if client_command_with_args && client_command_with_args.length > 0
    response = handle_client_command(client_command_with_args)
    client.puts response
  else
    puts "Empty request received from #{ client }"
  end
end
```

The server starts, waits for a client to connect, and then handles the requests from the client, nothing changed. Once the server wrote the response back, it starts doing the same thing again, waiting for a new client to connect, not keeping track of the first client, that is still connected as far as we know.

Let's start there, we need the server to keep track of all the clients that are currently connected.

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

Every time a client connects, we add it to the `@clients` array.

The rest of the loop is the same, when the first iteration ends, we go back to the beginning and wait for a new client. But what if the first client sends a request in the meantime? The server is currently waiting, potentially forever, for a new client to connect.

It is really starting to look that waiting for clients to connect and trying to handle connected clients in the same loop is quite problematic, especially with all these blocking calls that potentially wait forever.

One approach could be to time block these blocking calls, to make sure they don't block the server while there might be other things to do, such as responding to another client. We could start a second thread, make it loop until either a new client connected or  an arbitrary duration has elapsed and raise an exception when it has:

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

In the previous example, we create a new thread that will loop as long as the `server.accept` has not returned or if 5 seconds have elapsed. This means that the call to accept will not run for more than 5 seconds. The `abort_on_exception` setting is necessary, otherwise an uncaught exception in a Thread does not propagate to the parent thread.

Any clients connecting to the server within five seconds will prevent the `"Timeout!"` exception from being thrown.

As it turns out, we don't have to write this, Ruby gives us the `Timeout` module, that does pretty much the same thing, and throws an exception if the block hasn't finished after the given timeout:

``` ruby
require 'timeout'
Timeout.timeout(5) do
  server.accept
end
```

The Timeout module has received [a fair amount of criticism][sidekiq-timeout-blog] of the past few years. There are a few other posts out there if you search for the following keywords: "ruby timeout module dangerous". We should absolutely follow their recommendation.

Looking back at our primitive timeout implementation above, if the second thread enters the `if Time.now.to_f > timeout` condition, it will then throw an exception, but it is entirely possible that a client would connect at the exact same time, and the exception being thrown by the second thread would effectively interrupt the connection process and prevent the server from completing the `accept` call.


{{% admonition info "Clients, Servers and failures" %}}

When dealing with clients & servers, that is, code running in different processes, and potentially not running on the same machine, it is important to remember that a piece of code running on one machine can never really be sure that the other ones are in the state that they expect. The main different with running code in a single process is that when two pieces of code run in difference processes, they do not share memory, you can't create a variable in one, and read its value from the other. On top of that, each process has its own life cycle, one process might be stopped, for various reasons, while the other might still be running.

In concrete terms, it means that when we write code that will run on the server part, which is what we're doing here, we always have to keep in mind that a client that has connected in the past, may have disconnected by the time the server tries to communicate with it. There might be various reasons, to name a few, the client may have explicitly closed the connection, a network issue may have happened, causing the connection to be accidentally closed, or maybe the client code had an internal error, such as an exception being thrown and the process died.

That means that after creating the `client` variable, we have absolutely no guarantees that the client process on the other side is still connected. It is reasonable to assume that the client is still connected two lines below when we call `client.gets`, and while unlikely, it's still important to keep in mind that the network communication might still fail.

But what about later on, imagine that we kept the

{{% /admonition %}}

The timeout based approach seems a bit complicated and already shows limitations, so let's try another approach to allow the server to accept new clients while still being able to handle incoming requests from connected clients.

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
      if client.closed?
        @clients.delete(client)
      elsif client.eof?
        client.close
        @clients.delete(client)
      else
        client_command_with_args = client.gets
        if client_command_with_args && client_command_with_args.length > 0
          response = handle_client_command(client_command_with_args)
          client.puts response
        else
          puts "Empty request received from #{ client }"
        end
      end
    end
  end
end
```


Let's go through the main changes:

### `Thread.new` in the constructor

As soon as the server starts, we create a new thread, which does only thing, accept new clients. This second thread starts an infinite loop, inside the loop we call `accept`, and block until it returns a new client. When we do receive a new client, we store in the `@client` instance variable, so that it can be used from the main thread, in the main loop.

By moving the blocking call to `accept` to a different thread, we're not blocking the main loop anymore. Not with `accept` at least, there are still issues with this implementation, and `gets` is also a blocking call. We're improving things one step at a time.

### `client.eof?`

The main loop is pretty different now. We start by iterating through the `@clients` array. The idea being that on each iteration of `loop`, we want to give each of the connected clients a change to be handled.

`eof?` is a method defined on [`IO`][ruby-doc-io-eof?], the documentation describes it as:

> Returns true if ios is at end of file that means there are no more data to read. The stream must be opened for reading or an IOError will be raised.

In our case, `eof?` will return true if the client either explicitly closed the connection with the `close` method on `IO` or if the process that started the connection was killed.

This condition is essentially a first check to make sure that the client referenced by the `client` variable is still connected.

One way to think about it is to imagine a phone call, if you started a phone call, left your phone on your desk to go pick up a pen and came back, you would probably start by asking something like: "Are you still there?" and only if the person on the other end says yes, you would proceed to continue the conversation.

If `eof?` returns true, there's no one on the other end anymore, the client hung up, we remove the entry for the list of connected clients.

### The rest

The `else` branch inside the main loop is identical to what we started this chapter with, we use the blocking method `gets` to read from the client, and we write back a response.

### Still problematic

We made a lot progress but there are still many issues with the last version we looked at. `gets` is a blocking call, and iterate over the connected. If two clients connect to the server, client1 and client2, but client1 never sends a command, client2 will never get a chance to communicate with the server.

We need to fix this.

## Accept in a thread, gets with a timeout

There are different ways to make sure that all the connected clients get a chance to communicate with the server and to send their commands. Let's start with an approach we looked at earlier, timeouts.

The pros and cons of using timeouts here are the same as they were when explored it as an option to prevent `accept` from blocking the server.

Additionally, it would be fairly inefficient to do so, even with a short timeout, we would wait for the timeout duration on each client, even when there's nothing to read. It might fine with a handful of clients, but with a hundred clients, even a short timeout would be problematic.

Even with a timeout of 10ms, if all the clients are quietly waiting, not sending any commands, and only the 100th connected client sent a command, it would have to 990ms (99 * 10) before its command being read by the server.

I don't think it is that interesting to spend that much time with this approach since we've already established that it wasn't a good one, but you can experiment with it if you're interested. It is [in the `code` folder on GitHub][gets-with-timeout-gh]

## Read without blocking

The title of this section says it all, we are going to use a non-blocking alternative, the explicitly named [`read_nonblock`][ruby-doc-io-read-nonblock]. A key difference is that it requires an int argument to set the maximum number of bytes that will be read from the socket. For reasons that I can't explain, it seems to be common practice to set it as a power of two. We could set it to a very low value, like 4, but then we wouldn't even be able to read a whole `SET` command in on call. `SET 1 2` is seven bytes long. We could also set it to a very high value, like 4,294,967,296 (2^32), but then we would expose ourselves to instantiating a String of up to that length if a client were to send it.

It seems to be common to choose an arbitrary length, that should "long enough". Let's pick 256 for now, because we never expect commands to be longer than seven bytes for now, 256 gives us a lot to play with for now.

``` ruby

```

## Accept in a thread, select clients, still read_nonblock

## select everything, new clients and reads

## Conclusion

TODO

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
