---
title: "Chapter 2 - Respond to Get and Set"
date: 2020-05-17T21:28:17-04:00
lastmod: 2020-06-25T14:27:12-04:00
comment: false
summary: >
  In this chapter, we'll build on the foundations we established in the previous chapter. We now know how to start a TCP
  server using the built-in `TCPServer` class. In this chapter we'll build a basic client using another built-in class,
  `TCPSocket`. We'll then make the server actually usable by making it respond to two commands, `GET` and `SET`."
---

## What we'll cover

In this chapter, we'll build on the foundations we established in the previous chapter. We now know how to start a TCP
server using the built-in `TCPServer` class. In this chapter we'll build a basic client using another built-in class,
`TCPSocket`.
We'll then make the server actually usable by making it respond to two commands, `GET` and `SET`.

## Let's write some code

We're going to start by wrapping the code to start a server in a class, because this will make it easier to add
functionality later on.

Here's the code we'll use for now.

``` ruby
require 'socket'

class BasicServer

  def initialize
    server = TCPServer.new 2000
    puts "Server started at: #{ Time.now }"
    loop do
      client = server.accept
      puts "New client connected: #{ client }"
      client.puts "Hello !"
      client.puts "Time is #{ Time.now }"
      client.close
    end
  end
end
```

We can test this by saving this code in a ruby file, say `server.rb`, and run it with:

```ruby
ruby -r "./server" -e "BasicServer.new"
```

We're using this one-liner as a temporary workaround while we don't have an easy way to start the server, with an
executable, which would allow to do something like: `./simple-ruby-redis-server`. The command means, run a ruby process,
first require the `server.rb` file located in the same folder, and then execute the following command, `BasicServer.new`.
We should see a line indicating that the server started, with the current time.

Let's confirm that the server is running as expected by running, in a different shell: `nc localhost 2000`, the output
should be similar to the following, with a different date:

```
Hello !
Time is 2020-04-18 10:54:10 -0400
```

You should also see a line in the shell where you started your server, indicating that a client successfully connected:

```
New client connected: #<TCPSocket:0x00007fd83108f9d8>
```

The string after `TCPSocket:0x` will very likely be different on your machine, ruby's default
[`to_s`](https://ruby-doc.org/core-2.7.1/Object.html#method-i-to_s) method uses the object id, which is pretty much
always gonna be different.

### Reading from the socket in the client

So, now that we confirmed that our `BasicServer` class runs correctly, let's connect to it in ruby instead of using
`nc`. The direct parent class of `TCPServer` is `TCPSocket`, and according to the documentation:

> TCPSocket represents a TCP/IP client socket.

So far we've been using the code examples provided in the documentation, we can still do that here, we can paste the
lines one by one:

``` ruby
irb(main):001:0> require 'socket'
=> true
```

We already know what this line does, and we can confirm that there is [a file,
tcpsocket.c](https://github.com/ruby/ruby/blob/v2_7_1/ext/socket/tcpsocket.c#L88), in the `ruby/ext/socket/` folder,
that defines `TCPSocket`. Moving on.

``` ruby
irb(main):002:0> s = TCPSocket.new 'localhost', 2000
```

This line creates a new socket for the given host and port. It requires that a socket is listening on the other side,
which you can confirm by either running this before starting your server, by killing your server and re-running this, or
by changing the port value to a port that is unused, like 2001. You should see a Connection refused error:
`Errno::ECONNREFUSED (Connection refused - connect(2) for "localhost" port 2000)`. `connect(2)` in the previous error
message refers to the connect system call. The number 2 refers to the section of the manual, which is an optional
argument to the `man` command. It turns out there is no other man page for connect, so you can run `man connect` to
learn more about it, or you can be explicit and ask for a specific section, with `man 2 connect`. This is useful for
other pages, such as `accept`, `man accept` returns the page for `accept(8)`, but there is also an `accept` system
call, which you can read the documentation for with `man 2 accept`.


{{% admonition info "System Calls" %}}

As we start adding more features, we'll see more "system calls", often called syscalls, [to quote
Wikipedia][wikipedia-syscall]:

> In computing, a system call (commonly abbreviated to syscall) is the programmatic way in which a computer program
> requests a service from the kernel of the operating system on which it is executed.

So far we've implicitly seen two syscalls

- `accept`: This is how you connect to existing socket
- `socket`: This is how we created the server in the previous chapter

There are many syscalls, [this is list on linux][linux-syscalls], there are similarities but the list is different [on
macOS][macos-syscalls].


{{% /admonition %}}


Let's now look at the `while` loop from the documentation of `TCPSocket`:

``` ruby
while (line = s.gets) != nil do puts line end # This is different from the doc, but adapted to a one-liner
```

After running it in `irb`, the output should be:

``` ruby
irb(main):003:0> while (line = s.gets) != nil do puts line end
Hello !
Time is 2020-04-18 20:34:34 -0400
=> nil
```

We used a new method, [`gets`](https://ruby-doc.org/core-2.7.1/IO.html#method-i-gets), its documentation states:

> Reads the next "line" from the I/O stream; lines are separated by sep. A separator of nil reads the entire contents,
> and a zero-length separator reads the input a paragraph at a time (two successive newlines in the input separate
> paragraphs). The stream must be opened for reading or an IOError will be raised. The line read in will be returned and
> also assigned to $_. Returns nil if called at end of file. If the first argument is an integer, or optional second
> argument is given, the returning string would not be longer than the given value in bytes.

As we can see, we were able to connect to the server, on `localhost`, over port 2000, and we were able to read what the
server wrote with the `gets` method, one line at a time.

In true Ruby fashion, there are a few different ways to read from the socket, the example we just looked at used `gets`,
which is defined on `IO`, but if you look at the `IO` documentation, you'll find a few other similar methods, `read`,
`read_nonblock`, `readline` & `readlines` to name a few.  Exploring the differences between these methods is left as an
exercise to the reader.

OK, I have to admit, I always wanted to write that. I hate when books/posts do that. But seriously, it's a little bit
off topic for now, so we'll get back to it later, `read` can be convenient because it does not require a max length
argument as some of the others methods do and defaults to reading the whole thing, aka until it reaches `EOF`, as
opposed to doing it line by line like `gets` and `readline` do. As we'll see later on, it's also quite convenient
sometimes to read a whole line received through a socket, whereas `read` will keep on reading until it sees `EOF`, which
basically means, until the stream is closed.

There are at least two other methods, which map closely to system calls, `recvfrom` on `IPSocket` and `recv` on
`BasicSocket`.  So `gets` it is for now.


Let's close this `irb` session and kill the server with `Ctrl-C` for now, and create a new file, `client.rb` with the
following content:

``` ruby
require 'socket'

socket = TCPSocket.new 'localhost', 2000
message = ""
message << new_line while new_line = socket.gets
puts message
```

And with this last step, we now have a way to exercise what we went through without going through `irb`, first we can
start the server with the following command: `ruby -r "./server" -e "BasicServer.new"`

The `-r` option from ruby, according to `ruby -h`, is used to "require the library before executing your script". So by
passing a relative path, we require the content of the `server.rb` file. The `-e` option is used to "one line of
script. Several -e's allowed. [...]". Omitting the `-r` option would fail with a `NameError` because ruby wouldn't be
able to find a definition for `BasicServer`.

In another shell, run `ruby client.rb` and you will see an output similar to what we saw earlier, `Hello !`, followed by
a string containing the time when the server received the connection.

Now that we can connect to a server, let's see how to send data. This is a necessary step since we want clients to send
`GET` and `SET` requests, and have the server respond accordingly.

### Sending data from the client

Let's assume that our server is still running, we can start by sending a string with `nc` with the following command:
`echo -n "Hello Server, this is Client" | nc localhost 2000`. The output is still the same, minus the time
difference. So let's make some changes to the server to print what we received from the client.

As a reminder, our server is an instance of TCPServer, which happens to be a subclass of `TCPSocket`. This is great
news, because that means that we can read what the client sent the same way we read what the server sent to the
client. This is also an indication that at the end of the day, the client and the server have a lot in common. A socket
is open on each side, one of the main differences is that the `TCPServer` class adds the `accept` and `listen` methods,
which we can't do with a `TCPSocket`.

Let's add a print statement to the server code:

``` ruby
loop do
  client = server.accept
  puts "New client connected: #{ client }"
  client_message = client.read
  if client_message
    puts "Message received from #{ client }: #{ client_message }"
  end
  client.puts "Hello !"
  client.puts "Time is #{ Time.now }"
  client.close
end
```

Let's close our server if it was still running, with Ctrl-C, and restart it. And re-run the previous `nc` command. The
output for the client is the same, we didn't change anything there, but our server logs now show a new line:

```
Server started at: 2020-04-26 20:52:13 -0400
New client connected: #<TCPSocket:0x00007fb879931f78>
Message received from #<TCPSocket:0x00007fb879931f78>: Hello Server, this is Client # This is new!
```

Perfect! So it looks like we have all the pieces we need for now:

- We can create a client that can connect to our server
- The client can send data to the server
- The server can read the data sent from the client and write data back
- The client can read the data it received from the server

### Wrapping up

We want our server to understand two commands, `GET` and `SET`, for the sake of simplicity, let's focus on their most
basic versions.

`GET` takes one argument, the key name, and return its value if it exists and `(nil)` otherwise. Note that this is
purposefully different from the [Redis Protocol](https://redis.io/topics/protocol#resp-bulk-strings). We'll focus on
actual redis compatibility later on, once we have a stronger foundation for our implementation.

`SET` takes two arguments, the key value and the key name. It sets the value accordingly, erasing the previous value if
there was one.

All other command will return the following string `` (error) ERR unknown command `foo`, with args beginning with:
<args> ``, where \<args\> is everything that follows the command, aka, everything after the first space.

We are not performing any other validations for now, again, to focus on having a basic implementation. We'll make it
more robust later on.

``` ruby
require 'socket'

class BasicServer

  COMMANDS = [
    "GET",
    "SET",
  ]

  def initialize
    @data_store = {}

    server = TCPServer.new 2000
    puts "Server started at: #{ Time.now }"
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
      client.close
    end
  end

  private

  def handle_client_command(client_command_with_args)
    command_parts = client_command_with_args.split
    command = command_parts[0]
    args = command_parts[1..-1]
    if COMMANDS.include?(command)
      if command == "GET"
        if args.length != 1
          "(error) ERR wrong number of arguments for '#{ command }' command"
        else
          @data_store.fetch(args[0], "(nil)")
        end
      elsif command == "SET"
        if args.length != 2
          "(error) ERR wrong number of arguments for '#{ command }' command"
        else
          @data_store[args[0]] = args[1]
          'OK'
        end
      end
    else
      formatted_args = args.map { |arg| "`#{ arg }`," }.join(" ")
      "(error) ERR unknown command `#{ command }`, with args beginning with: #{ formatted_args }"
    end
  end
end
```

The key element of this implementation is that we initialize an empty `Hash` when creating a new instance of
`BasicServer`, and this is what we use to store the data when responding to `SET` requests. Using a `Hash` here could
almost be considered "cheating". If you have already looked at the Redis source code, a big part of its implementation
is related to how data is stored, and since it's written in C, it can't "just" say: "throw this value, for this key, in
this existing data structure and call it a day". That being said, as already mentioned multiple times, we're taking an
incremental approach. For now we're focusing on how to integrate the networking parts of our client/server architecture,
how to exchange information, and while we're busy with this, Ruby's built-in `Hash` class is a amazing, it supports,
among many other, the two main features we need: [`[]`](https://ruby-doc.org/core-2.7.1/Hash.html#method-i-5B-5D) &
[`[]=`](https://ruby-doc.org/core-2.7.1/Hash.html#method-i-5B-5D-3D).

Redis uses the [SipHash algorithm][wikipedia-siphash], as we can see in the [`siphash.c` file][redis-siphash-file], and
as it turns out, this seems to be what [Ruby uses][ruby-source-siphash] as well! I say _seem_ here because by browsing
the code I couldn't really confirm that siphash was used in `hash.c`.

And here is the client code:

``` ruby
require 'socket'

class BasicClient

  COMMANDS = [
    "GET",
    "SET",
  ]

  def get(key)
    socket = TCPSocket.new 'localhost', 2000
    result = nil
    socket.puts "GET #{ key }"
    result = socket.gets
    socket.close
    result
  end

  def set(key, value)
    socket = TCPSocket.new 'localhost', 2000
    result = nil
    socket.puts "SET #{ key } #{ value }"
    result = socket.gets
    socket.close
    result
  end
end
```

Let's confirm that it works as expected, as usual, let's start the server in one shell and run commands from another
one with `irb -r "./client"`

```
irb(main):001:0> client = BasicClient.new
irb(main):002:0> client.get 1
=> "(nil)\n"
irb(main):003:0> client.get 2
=> "(nil)\n"
irb(main):004:0> client.set 1, 2
=> "2\n"
irb(main):005:0> client.set 2, 3
=> "3\n"
irb(main):006:0> client.get 1
=> "2\n"
irb(main):007:0> client.get 2
=> "3\n"
```

## A few tests

I apologize if you're a TDD enthusiast because what I'm about to do might make you cringe. Now that we have added these
features and manually tested that they worked, we're going to add a few tests.

I'm using minitest, because I like how easy it is to setup. Yes, I know, I am technically already breaking the rules I
laid out in the "from scratch" section of the first chapter, but we're talking about tests here. While it might be
interesting to write a testing library from scratch, and I might do that at some point, I think it's ok to leverage a
library and focus on the main task, building a Redis server.

And while minitest is a gem, it [has been included by default][ruby-minitest] in ruby for a while now (at least since
2.0), so I'm not _really_ breaking the rules!

``` ruby
require 'minitest/autorun'
require 'timeout'
require 'stringio'
require './server'

describe 'BasicServer' do

  def connect_to_server
    socket = nil
    # The server might not be ready to listen to accepting connections by the time we try to connect from the main
    # thread, in the parent process. Using timeout here guarantees that we won't wait more than 1s, which should
    # more than enough time for the server to start, and the retry loop inside, will retry to connect every 10ms
    # until it succeeds
    Timeout::timeout(1) do
      loop do
        begin
          socket = TCPSocket.new 'localhost', 2000
          break
        rescue
          sleep 0.01
        end
      end
    end
    socket
  end

  def with_server

    child = Process.fork do
      # We're effectively silencing the server with these two lines
      # stderr would have logged something when it receives SIGINT, with a complete stacktrace
      $stderr = StringIO.new
      # stdout would have logged the "Server started ..." & "New client connected ..." lines
      $stdout = StringIO.new
      BasicServer.new
    end

    yield

  ensure
    if child
      Process.kill('INT', child)
      Process.wait(child)
    end
  end

  def assert_command_results(command_result_pairs)
    with_server do
      command_result_pairs.each do |command, expected_result|
        begin
          socket = connect_to_server
          socket.puts command
          response = socket.gets
          assert_equal response, expected_result + "\n"
        ensure
          socket.close if socket
        end
      end
    end
  end

  describe 'when initialized' do
    it 'listens on port 2000' do
      with_server do
        # lsof stands for "list open files", see for more info https://stackoverflow.com/a/4421674
        lsof_result = `lsof -nP -i4TCP:2000 | grep LISTEN`
        assert_match "ruby", lsof_result
      end
    end
  end

  describe 'GET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'GET', '(error) ERR wrong number of arguments for \'GET\' command' ],
      ]
    end

    it 'returns (nil) for unknown keys' do
      assert_command_results [
        [ 'GET 1', '(nil)' ],
      ]
    end

    it 'returns the value previously set by SET' do
      assert_command_results [
        [ 'SET 1 2', 'OK' ],
        [ 'GET 1', '2']
      ]
    end
  end

  describe 'SET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SET', '(error) ERR wrong number of arguments for \'SET\' command' ],
      ]
    end

    it 'returns OK' do
      assert_command_results [
        [ 'SET 1 3', 'OK' ],
      ]
    end

  end

  describe 'Unknown commands' do
    it 'returns an error message' do
      assert_command_results [
        [ 'NOT A COMMAND', '(error) ERR unknown command `NOT`, with args beginning with: `A`, `COMMAND`,' ],
      ]
    end
  end
end
```

There are a few oddities in there, most of them are documented inline, but the main approach is that for each test, we
create a new process with `Process.fork`, and we start the server in the new process. We then connect to it from the
original process, and send commands over the TCP connection.

## Conclusion

Well, that was a lot, but we now have a nice base to build from.

There are many things we can do from now, but one that I think is important is to start thinking about how it would
handle multiple clients. There are a few major flaws with the current implementation, one of them being that once a
client connects, it will block until it receives a new line from the client that just connected, or until the client
disconnects.

We can reproduce this behavior by opening a third shell, requiring `socket` and creating a new socket connected to
`localhost` over port 2000. Now go back to the `irb` console we used previously to test our client and try to call `get`
or `set`. It will hang. This is because the server is running a single thread, and that thread is waiting for
`client.gets` to return something. We have two ways to make `gets` return, either send a new line with `puts` from the
newly created socket, or close it, with `close`.

The `BasicClient` works around this issue by never keeping a connection open, when you call `get` or `set`, it starts
from scratch, establishes a new connection, sends the command, reads the response, and closes the socket. Effectively
killing the connection. This works for now, but is fairly wasteful, TCP connections can stay open and be reused. Let's
illustrate this right now, with a non scientific quick benchmark:

We'll start by adding a logging statement to the `get` method in the client class, to observe how long it takes to
connect to the server, send a command, and get a response:

``` ruby
def get(key)
  t0 = Time.now
  # ...
  puts "Time elapsed: #{ (Time.now - t0)*1000 }ms"
  result
end
```

Results vary a lot from one run to another on my machine, but I'm getting in the 1ms to 1.8ms range, ish. Remember that
both the client and server are running on the same machine, we would see very different numbers if these were running on
different machines.

Note: This is something I am really interested in and might take a few minutes to spin up two EC2
instances and see what the numbers are on AWS.

Let's now see what it looks like to run two `GET` commands, sequentially:

``` ruby
def two_full_gets(key)
  t0 = Time.now
  get(key)
  get(key)
  puts "Time elapsed: #{ (Time.now - t0)*1000 }ms"
end
```

Interestingly I am consistently seeing the second `get` call to be 40% to 100% faster than the first one. After running
it a dozen of times, I am seeing results in the 1.6ms to 4ms range.

Let's end this quick test by running two gets, over the same connection!

``` ruby
def two_gets_a_single_connection(key)
  t0 = Time.now
  socket = TCPSocket.new 'localhost', 2000
  result = nil
  socket.puts "GET #{ key }"
  socket.puts "GET #{ key }"
  result = socket.gets
  socket.close
  puts "Time elapsed: #{ (Time.now - t0)*1000 }ms"
  result
end
```

Again, I ran this about a dozen times, and saw it as low as .982ms and never above 1.4ms. This makes sense, establishing
a connection is not free. When we create an instance of `TCPSocket`, ruby delegates to the OS through the `socket`
syscall and it attempts to establish a network connection. All of that work is done twice in the first example, and only
once in the second example. And once again, it is fair to assume that the difference would be even bigger with two
different hosts, because to establish a connection, TCP packets would have to actually travel from one host to the
other, over the network, instead of on the same physical machine.

The next chapter will look at what our options are to make sure that our server can keep client connections open and
still serve all its clients efficiently, without blocking like the current implementation does.

### Code

The code from this chapter is [available on GitHub](https://github.com/pjambet/redis-in-ruby/tree/master/code/chapter-2)


[wikipedia-syscall]:https://en.wikipedia.org/wiki/System_call
[linux-syscalls]:https://man7.org/linux/man-pages/man2/syscalls.2.html
[macos-syscalls]:https://opensource.apple.com/source/xnu/xnu-1504.3.12/bsd/kern/syscalls.master
[wikipedia-siphash]:https://en.wikipedia.org/wiki/SipHash
[redis-siphash-file]:https://github.com/antirez/redis/blob/bf3a67be437e6a3cd5189116d9ad628492db0c4d/src/siphash.c
[ruby-source-siphash]:https://github.com/ruby/ruby/blob/v2_7_1/siphash.h
[ruby-minitest]:https://github.com/ruby/ruby/tree/v2_7_1/tool/lib/minitest
