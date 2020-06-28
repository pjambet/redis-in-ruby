---
title: "Chapter 3 - Multiple Clients"
date: 2020-05-18T01:30:32-04:00
lastmod: 2020-05-18T01:30:32-04:00
draft: true
keywords: []
description: "In this chapter we will improve the Redis server to efficiently handle multiple clients connected at the same time"
---

## Introduction
👋

## First problem, accepting clients

Let's start with the new client problem. The goal is that, regardless of the state of the server, of what it may or may not currently doing, or whether other clients are already connected, new clients should be able to establish a new connection, and keep the connection open as long as they wish, until they either disconnect on purpose or a network issue occurs.

Before attempting to fix the problem, we need to think about what we're trying to achieve, what parts of the current implementation are problematic and only then can we really start thinking about what needs to change.

First things first, we want our server to keep client connections alive until clients disconnect. After all, this is what what Redis does, it keeps the connection alive until the client closes the connection, either explicitly with the [QUIT][redis-documentation-quit].

So that means removing this `client.close` line, we will add it back when we add a handler for the `QUIT` command, but let's set that aside for now.

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

The server starts, waits for a client to connect, and then handles the requests from the client, nothing changed there. But what happens once the server wrote the response back, it starts doing the same thing again, waiting for a new client to connect, not keeping track of the first client, that is, as far as we know, still connected.

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

Everytime a client connects, we add it to the `@clients` array, so now, since the rest of the loop is the same, the first iteration ends, we go back to the beginning and wait for a new client. But what if the first client sends a request in the meantime? The server is currently waiting, potentially forever, for a new client to connect.

It is really starting to look that waiting for clients to connect and trying to handle connected clients in the same loop is quite problematic, especially with all these blocking calls that potentially wait forever.

One approach here could be to timeblock these blocking calls, to make sure they don't block the server while there might be other things. We could write a custom loop around a blocking call, and making sure it doesn't wait longer than an arbitrary time:

``` ruby
timeout = Time.now.to_f + 1
while Time.now.to_f < timeout do
  server.accept
  sleep 0.001
end
```

This loop will only run for about a second, and will then stop. It turns out we don't have to write this, Ruby gives us the `Timeout` module, that does pretty much the same thing, and throws an exception if the block hasn't finished after the given timeout:

``` ruby
require 'timeout'
Timeout.timeout(1) do
  server.accept
end
```

Note: The Timeout module has received a fair amount of criticism of the past few years, we're only using it here to explore potential approaches, not as a long term solution.

{{% admonition info "Clients, Servers and failures" %}}

When dealing with clients & servers, that is, code running in different processes, and potentially not running on the same machine, it is important to remember that a piece of code running on one machine can never really be sure that the other ones are in the state that they expect.

In concrete terms, it means that when we write code that will run on the server part, which is what we're doing here, we always have to keep in mind that a client that has connected in past, may have disconnected by the time the server tries to communicate with it. There might be various causes, to name a few, the client may have explicitly closed the connection, a network issue may have happened, causing the connection to be accidentally closed, or maybe the client code had an internal error, such as an exception being thrown and the process died.

That means that after creating the `client` variable, we have absolutely no guarantees that the client process on the other side is still connected. It is reasonable to assume that the client is still connected two lines below when we call `client.gets`, and while unlikely, it's still important to keep in mind that the network communication might still fail.

But what about later on, imagine that we kept the

{{% /admonition %}}



TODO: Finish the above after we kept track of the clients.

``` ruby
def initialize
  @clients = []
  @data_store = {}

  server = TCPServer.new 2000
  puts "Server started at: #{ Time.now }"
  Thread.new do
    loop do
      sleep 1
      puts "Accepting clients"
      new_client = server.accept
      puts "New client connected: #{ new_client }"
      @clients << new_client
      sleep 1
    end
  end

  loop do
    @clients.each do |client|
      if client.closed?
        puts "Found a closed client, removing"
        @clients.delete(client)
      elsif client.eof?
        puts "Found a client at eof, closing and removing"
        client.close
        @clients.delete(client)
      else
        puts "Reading from client: #{ client }"
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

``` ruby
```


[redis-documentation-quit]:https://redis.io/commands/quit
