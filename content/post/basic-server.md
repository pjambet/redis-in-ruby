---
title: "A basic TCP server"
date: 2020-05-16T15:54:30-04:00
draft: false
---

## What we'll cover

This chapter will cover the creation of a TCP server in ruby, as well as how to interact with it with `netcat` (`nc`), a
utility bundled with macOS. We will briefly look at concurrency and parallelism and how threads can impact the behavior
of our server.

## Introduction

The goal of this series of posts is to re-implement a Redis server, "from scratch". At the time of this writing Redis
supports 9 different [data types](TODO), dozens of commands related to those data types as well as many features in the
[Administration](https://redis.io/documentation#administration) category, such Redis Sentinel for High Availability.
I will start with a heavily simplified version of Redis, and slowly add more features, trying to get as close as
possible to what Redis supports today.
I'm choosing Ruby for this language for no other reasons that I like it, and I find it fun to play with. Ruby is often
caracterized as a "slow language" but performance is not a big concern here. While I will make sure to make sound
implementation choices, the goal is to learn more about Redis, Ruby, networking, OS signals and so so and not to produce
a production ready software.

## A note about "from scratch"

"From scratch" can be a an ambiguous term, especially with a languate like Ruby that provides so many features out of
the box.
So, my goal with this series will be to rely exclusively on the Ruby standard library. At the time of this writing,
April 2020, the latest Ruby version is 2.7.1.

## Let's write some code

The main Redis component is `redis-server`, which is an executable that starts a TCP server. When experimenting with
Redis, it is common to use `redis-cli`, which is an executable that starts a REPL client that connects to a redis server.
By default Redis runs on port 6379 locally.

The official Ruby documentation shows the following example for the [TCPServer
class](http://ruby-doc.org/stdlib-2.7.1/libdoc/socket/rdoc/TCPServer.html):

{{< highlight ruby >}}
require 'socket'

server = TCPServer.new 2000 # Server bind to port 2000
loop do
  client = server.accept    # Wait for a client to connect
  client.puts "Hello !"
  client.puts "Time is #{Time.now}"
  client.close
end
{{< / highlight >}}

Followed by the following example and comment: "A more usable server (serving multiple clients)"

``` ruby
require 'socket'

server = TCPServer.new 2000
loop do
  Thread.start(server.accept) do |client|
    client.puts "Hello !"
    client.puts "Time is #{Time.now}"
    client.close
  end
end
```

I will ignore the second example for now because concurrency/parallelism is a complicated topic and I want to address it
in depth in a later post.
So, for now, let's focus on the first example, line by line:

First, we require `'socket'`. Ruby's require syntax has always been weird to me, especially compared to more explicit
ones like Python's for instance.
After a bit of browsing the Ruby source code, my guess is that this require will add all the constants defined in the c
files [in this folder](https://github.com/ruby/ruby/tree/v2_7_1/ext/socket) to `$LOAD_PATH`, making a bunch of classes
such as [`Addrinfo`](https://github.com/ruby/ruby/blob/v2_7_1/ext/socket/raddrinfo.c#L2689),
[`UnixServer`](https://github.com/ruby/ruby/blob/v2_7_1/ext/socket/unixserver.c#L118),
[`UNIXSocket`](https://github.com/ruby/ruby/blob/v2_7_1/ext/socket/unixsocket.c#L584),
[`UDPSocket`](https://github.com/ruby/ruby/blob/v2_7_1/ext/socket/udpsocket.c#L238),
[`TCPSocket`](https://github.com/ruby/ruby/blob/v2_7_1/ext/socket/tcpsocket.c#L88),
[`TCPServer`](https://github.com/ruby/ruby/blob/v2_7_1/ext/socket/tcpserver.c#L139) &
[`SOCKSocket`](https://github.com/ruby/ruby/blob/v2_7_1/ext/socket/sockssocket.c#L68).
You can try this on your own in an `irb` shell, requiring any of these constants would fail before calling `require 'socket'` and would work afterwards:

``` ruby
irb(main):001:0> TCPSocket.new
Traceback (most recent call last):
# [truncated]
NameError (uninitialized constant TCPSocket)
irb(main):002:0> UNIXServer.new
Traceback (most recent call last):
# [truncated]
NameError (uninitialized constant UNIXServer)
irb(main):003:0> require 'socket'
=> true
irb(main):004:0> TCPSocket.new
# Traceback (most recent call last):
[truncated]
ArgumentError (wrong number of arguments (given 0, expected 2..4))
irb(main):005:0> UNIXServer.new
# Traceback (most recent call last):
[truncated]
ArgumentError (wrong number of arguments (given 0, expected 1))
```

The next line creates a new `TCPServer` instance, listening on port 2000. The documentation for new is the following:

`new([hostname], port) => tcpserver`

> Creates a new server socket bound to port.
> If hostname is given, the socket is bound to it.
> Internally, ::new calls getaddrinfo() function to obtain addresses. If getaddrinfo() returns multiple addresses, ::new tries to create a server socket for each address and returns first one that is successful.

What the documentation implies is that the `hostname` argument is optional and that omitting it runs on `localhost` by default.
Great, so now we have a TCP server running on `localhost:2000`, but is it actually doing anything? Let's see.

We can run a server in an `irb` shell:

``` ruby
irb(main):001:0> require 'socket'
=> true
irb(main):002:0> TCPServer.new 2000
=> #<TCPServer:fd 10, AF_INET6, ::, 2000> # Note that fd value might be different on your system.
```

We'll spend more time digging into what these values mean, but for now let's just very briefly look at what that means:

`fd 10`: `fd` stands for File Descriptor, if you're interested you can see all the descriptors used by a process on macOS with the `lsof` tool: `lsof -p <process id>`, on my machine, this is what the last line look like:

``` bash
[...]
ruby    86096 pierre   10u   IPv6 0x80d76e9380eb855d      0t0     TCP *:callbook (LISTEN)
```


`2000`: This is the port value, which we passed as an argument to the constructor

I wasn't exactly sure what the two values in the middle were, `AF_INET6` & `::`, so I first tried to figure out how this
string was built by looking at where the `inspect` method came from on the `TCPServer` instance:

``` bash
irb(main):001:0> TCPServer.new(2003).method(:inspect)
=> #<Method: TCPServer(IPSocket)#inspect()>
```

This makes sense, `TCPServer` inherits from `TCPSocket`, which in turn inherits from `IPSocket`. This is a C function
defined in [`ipsocket.c`](https://github.com/ruby/ruby/blob/v2_7_1/ext/socket/ipsocket.c#L206).  It looks like the
values come from the `addr` function, which according the docs: "Returns the local address as an array which contains
address_family, port, hostname and numeric_address."  A bit of Gooling about `AF_INET6` [confirmed
this](https://stackoverflow.com/a/1594039/919641), `AF` stands for Address Family, and INET mean Internet Protocol v4,
and INET6 means Internet Protocol v6. `::` has a special meaning for IPv6 addresses, equivalent to 0.0.0.0 for IPv4.

Back to our server, if you still have a terminal open, where you ran `TCPServer.new 2000` in an `irb` shell, now open a
new terminal and run `nc localhost 2000`. `nc` or `netcat`, from its `man` page: "is used for just about anything under
the sun involving TCP or UDP". `telnet` is another similar tool, but it does come with macOS, that being said, it is
only a `brew install` away.

Running `nc localhost 2000` should "hang", nothing is happening. Feel free to exit with Ctrl-C. You can confirm that it
did indeed do something, because if you try it with an unused port, such as 2001, it should return right away, with an
exit code of 1, aka, an error. If it hangs, it might be because you have something running on port 2001 on your machine.

Let's dive a bit deeper by passing the verbose flag, `-v`: `nc -v localhost 2000`:

``` bash
found 0 associations
found 1 connections:
     1: flags=82<CONNECTED,PREFERRED>
        outif lo0
        src 127.0.0.1 port 53022
        dst 127.0.0.1 port 2000
        rank info not available
        TCP aux info available

Connection to localhost port 2000 [tcp/callbook] succeeded!
```

So it did connect, and it does nothing, because our server was just started, but it wasn't instructed to do anything
with the incoming connection.

`loop` in Ruby starts an infinite loop, nothing special here, it is common for a server to start and never end. Think
about Redis for a minute, once the server is running, we want to run pretty much forever, we don't expect it to stop
unless told to do so. An inifinite loop makes perfect sense for a use case like that.

The next line is really interesting, and probably one of the most interesting ones in this small snippet: `server.accept`.
Let's go back to `irb` for a moment, because it makes it easier to experiment with these methods, one at a time.

``` ruby
irb(main):001:0> require 'socket'
=> true
irb(main):002:0> s = TCPServer.new 2000
irb(main):003:0> s.accept
```

The `accept` method does not return. The documentation is sadly very succint:

> Accepts an incoming connection. It returns a new TCPSocket object.

What it doesn't tell us is that it effectively a "blocking" method. This can technically be inferred by the presence of
another method on `TCPServer`, `accept_nonblock`, which, according to the documentation:

> Accepts an incoming connection using accept(2) after O_NONBLOCK is set for the underlying file descriptor. It returns an accepted TCPSocket for the incoming connection.

We'll look closer at `accept_nonblock` in a later chapter. Let's go back to our second shell, note that the hanging `nc
localhost 2000` ended if you close your `irb` shell. What happened is that the server closed the connection, and the
client, `nc`, stopped as well. Let's re-run the same command, `nc localhost 2000`. We'll see a very similar output we
saw above, saying that the connection succeeded.
The main difference is that that hanging `accept` call in `irb` now
returned, with a `TCPSocket` instance. Let's get a reference to this socket with `socket = _` (`_` is a reference to the
last value returned in `irb`) and send something to our client: `socket.puts "Hey!"`. If you go back to the terminal
where you ran `nc`, you should see "Hey!". Let's now close the connection with `socket.close` and we can observer that
the `nc` calls returned, with an exit code of 0, aka, success.

And that's it, we went through the official example of `TCPServer`. The example starts a TCP server on port 2000, and
then enters an infinite loop, it first wait for an incoming connection, and does nothing else until a client connects,
once a client connects, it writes "Hello !" first and then the current time and finally closes the connection, and start
the same process again.

If you remember, at the beginning we briefly looked at the second example in the documentation, using
`Thread.start`. The justification for this example was that it was more usable by being able to serve multiple clients.
There is indeed a major issue with the initial example, it can only serve one client at a time. If you were to put the
example in a file, say, `server.rb` and run it with `ruby server.rb`, it would start one process, one thread and start
executing the loop.
The second example improves the situation by passing the result of the blocking operation, `server.accept`, to a new
thread and letting it handle the client.

There are a few important things to note here. First, ruby does not support lazy arguments, so when we write
`Thread.start(server.accept)`, we first need to evaluate the argument, and only then will `start` be called, with the
result of the evaluation.
What that means for our example is that the loop starts, then blocks until a client connects, and once the client is
connected, the resulting socket is passed to `Thread.start`.

Let's illustrate this with an example, we'll simulate a slow server by adding a `sleep` call. Still in `irb`, run the
following:

``` ruby
loop { socket = server.accept; socket.write "Hello"; sleep 5; socket.close }
```

This is almost identical to the first example, except that the main thread sleeps for five seconds before closing the
socket.  Back to the other terminal, run `nc localhost 2000` again, and you'll see "Hello" being printed almost
instantly, followed by the process hanging for five seconds and then exiting. While it is hanging, open a terminal and
run the same command, `nc localhost 2000`, if you see "Hello" right away, it might because the five seconds elapsed in
the previous terminal. Feel free to close the inifinite loop in `irb` with Ctrl-C and increase the value to 10 seconds
or more.
What this shows us is that while the server is busy dealing with a client, if it doesn't do anything and just sleeps,
all other incomint clients are effectively waiting to be served.
The second example improves on this as can be seen in `irb` if you run the following instead:

``` ruby
loop {
  Thread.start(server.accept) { |socket|
    socket.write "Hello"
    sleep 5
    socket.close
  }
}
```

Running the same manual test, we should see that both `nc localhost 2000` calls get the "Hello" response, and then they
proceed to both wait for five seconds until the server closes the socket.
This is because each client is being handled by a different thread. The first client will trigger the first
`server.accept` call to return, resulting in a second thread being started (second thread because the initial program is
itself running in a thread as well). While the second thread is sleeping, the main thread is not blocked anymore, it
passed the socket to the second thread and is back at being blocked on `server.accept`. When the second client connects,
the same thing happens, a new thread is started, and it's being given the newly created socket.
We have a server that can server multiple clients at one, which is great. That being said, there is a big problem with
this approach. Threads are not an unlimited, if we were to create more and more threads, we could be at risk of slowing
down the whole system. Using multiple threads is something that needs to be done very carefully and it can easily lead
to race conditions. As mentioned earlier, concurrency and parallelism are both complicated topics and we'll try to cover
them in future chapters.

## Conclusion

We now know how to run a basic TCP server, which doesn't do much, it can write strings to its clients and then close the
connection, that's about it. That being said, as we'll see in future chapters, we can do a lot with that.
We also looked at the limitiation of a single threaded approach, and while we could use threads to improve the
situation, we are purposefully not doing it for now, to keep our example simple and improve it one step at a time.

In the next chapter we'll create a server class and make it read input from clients, and respond to `GET` and `SET`, in
their most basic forms, we won't implement things like TTL or other options,


### Appendix - A C implementation of the server

I was interested to see what a lower level implementation looks like, so I tried to put together an example, of a server,
listening on port 2000, and writing data back to clients when they connect. Here is the code.

You can test it yourself, first you need to compile it with the following command:

Note: If you do not have `gcc` installed your machine, you can install in on macOS with the following command:
`xcode-select --install`, if you're on a different OS, I'll let you search, this should be a fairly common questions and
have lots of answers on stackoverflow and the likes.

The code I'm sharing here is a simplified version of the client/server code available on
[GeeksforGeeks](https://www.geeksforgeeks.org/tcp-server-client-implementation-in-c/):

Server:

``` c
#include <stdio.h> // For printf
#include <netdb.h> // For bind, listen, AF_INET, SOCK_STREAM, socklen_t, sockaddr_in, INADDR_ANY
#include <stdlib.h> // For exit
#include <string.h> // For bzero
#include <unistd.h> // For close & write
#include <errno.h> // For errno, duh!
#include <arpa/inet.h> // For inet_ntop

#define MAX 80
#define PORT 2000
#define SA struct sockaddr

int main()
{
    socklen_t client_address_length;
    int server_socket_file_descriptor, client_socket_file_descriptor;
    struct sockaddr_in server_address, client_address;

    // socket create and verification
    server_socket_file_descriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_file_descriptor == -1) {
        printf("socket creation failed...\n");
        exit(0);
    }
    else {
        printf("Socket successfully created..\n");
    }
    bzero(&server_address, sizeof(server_address));

    // assign IP, PORT
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = htons(PORT);

    // Binding newly created socket to given IP and verification
    if ((bind(server_socket_file_descriptor, (SA*)&server_address, sizeof(server_address))) != 0) {
        printf("socket bind failed... : %d, %d\n", server_socket_file_descriptor, errno);
        exit(0);
    }
    else {
        printf("Socket successfully binded..\n");
    }

    // Now server is ready to listen and verification
    if ((listen(server_socket_file_descriptor, 5)) != 0) {
        printf("Listen failed...\n");
        exit(0);
    }
    else {
        printf("Server listening..\n");
    }
    client_address_length = sizeof(client_address);

    // Accept the data packet from client and verification
    client_socket_file_descriptor = accept(server_socket_file_descriptor, (SA*)&client_address, &client_address_length);
    if (client_socket_file_descriptor < 0) {
        printf("server acccept failed: %d,%d...\n", client_socket_file_descriptor, errno);
        exit(0);
    }
    else {
        printf("server acccept the client...\n");
        char human_readable_address[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_address.sin_addr, human_readable_address, sizeof(human_readable_address));
        printf("Client address: %s\n", human_readable_address);
    }

    char message_buffer[MAX];
    read(client_socket_file_descriptor, message_buffer, sizeof(message_buffer));
    printf("From Client: %s\n", message_buffer);
    bzero(message_buffer, MAX);

    strcpy(message_buffer, "Hello, this is Server!");
    write(client_socket_file_descriptor, message_buffer, sizeof(message_buffer));

    // After chatting close the socket
    printf("Closing server_socket_file_descriptor\n");
    close(server_socket_file_descriptor);
}
```

Client:

``` c
#include <stdio.h> // For printf
#include <netdb.h> // For AF_INET, SOCK_STREAM, sockaddr_in
#include <stdlib.h> // For exit
#include <string.h> // For bzero
#include <sys/socket.h> // For connect
#include <arpa/inet.h> // For inet_addr
#include <unistd.h> // for close

#define MAX 80
#define PORT 2000
#define SA struct sockaddr

int main() {
    int server_socket_file_descriptor;
    struct sockaddr_in server_address;

    // socket create and varification
    server_socket_file_descriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_file_descriptor == -1) {
        printf("socket creation failed...\n");
        exit(0);
    }
    else {
        printf("Socket successfully created..\n");
    }

    bzero(&server_address, sizeof(server_address));

    // assign IP, PORT
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = inet_addr("127.0.0.1");
    server_address.sin_port = htons(PORT);

    // connect the client socket to server socket
    if (connect(server_socket_file_descriptor, (SA*)&server_address, sizeof(server_address)) != 0) {
        printf("connection with the server failed...\n");
        exit(0);
    }
    else {
        printf("connected to the server..\n");
    }

    char message_buffer[MAX] = "Hello, this is Client";
    write(server_socket_file_descriptor, message_buffer, sizeof(message_buffer));
    bzero(message_buffer, sizeof(message_buffer));
    read(server_socket_file_descriptor, message_buffer, sizeof(message_buffer));
    printf("From Server: %s", message_buffer);

    // close the socket
    close(server_socket_file_descriptor);
}
```

As usual, let's run a few manual tests, but since this is C, we first need to compile this:

``` bash
$ gcc server.c -o server
$ gcc client.c -o client
```

We're going to need to shells, start the server in the first one, `./server`. It should log the following:

``` bash
Socket successfully created..
Socket successfully binded..
Server listening..
```

Note that, as we saw previously when creating a server from Ruby, it is "hanging" and hasn't returned yet. In the other
shell, run the client: `./client`, you should see the following output:

``` bash
$ ./client
Socket successfully created..
connected to the server..
From Server: Hello, this is Server!
```

And if you return to the other shell, where the server was running, you can see that it now returned and log a few more
things before doing so:

``` bash
server acccept the client...
Client address: 127.0.0.1
From Client: Hello, this is Client
Closing server_socket_file_descriptor
```

It works! A server, that waits until a client connects, reads what the client sent and writes a message back, and once
all of that is done, exits.

Doing a step by step walkthrough of the client and server code is both a little bit out of scope and frankly something
that I am not really capable of doing at the moment. That being said, I thought it would be interesting to visualize a
similar-ish implementation to get a rough idea of what Ruby does for us under the hood.
