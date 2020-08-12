---
title: "Chapter 5 Redis Protocol Compatibility"
date: 2020-07-26T12:07:43-04:00
lastmod: 2020-07-26T12:07:43-04:00
draft: true
comment: false
keywords: []
summary: "In this chapter we will focus on making RedisServer speak the Redis Protocol, RESP. Doing so will allow us to use Redis' built-in client, redis-cli to communicate with our own server."
---

## What we'll cover

By the end of this chapter `RedisServer` will speak [`RESP v2`, the Redis Protocol][resp-spec]. Doing this will allow any clients that was written to communicate with the real Redis to also communicate with our own server, granted that the commands it uses are within the small subset of the ones we implemented.

One such client is the `redis-cli` utility, that ships with Redis, it'll look like this:

![redis-cli-gif](/redis.gif/)

In this demo the client crashes when we attempt to use the `DEL` command. This is because the command is not implemented by the server

[RESP v2][resp-spec] has been the protocol used by Redis since version 2.0, to quote the documentation:

> 1.2 already supported it, but Redis 2.0 was the first version to talk only this protocol)

## RESP3

RESP v2 is the default version, but not the latest one. RESP3 has been released in 2018, it improves many different aspects of RESP v2, such as adding new types for maps — often called dictionary — and a lot more. The spec is [on GitHub][resp3-spec] and explains in details [the background behind it][resp3-spec-background].
RESP3 is supported as of Redis 6.0, as indicated in [the release notes][release-notes-6-0]:

> Redis now supports a new protocol called RESP3, which returns more semantical replies: new clients using this protocol can understand just from the reply what type to return to the calling program.

The [`HELLO`][redis-doc-hello] command can be used to switch the connection to a different protocol. As we can see below, only two versions are currently supported, 2 & 3. We can also see the new map type in action, `hello 2` returned an array with 14 items, representing 7 key/value pairs, whereas `hello 3` leveraged the new map type to return a map with 7 key/value pairs.

``` bash
127.0.0.1:6379> hello 2
 1) "server"
 2) "redis"
 3) "version"
 4) "6.0.6"
 5) "proto"
 6) (integer) 2
 7) "id"
 8) (integer) 6
 9) "mode"
10) "standalone"
11) "role"
12) "master"
13) "modules"
14) (empty array)
```

``` bash
127.0.0.1:6379> hello 3
1# "server" => "redis"
2# "version" => "6.0.6"
3# "proto" => (integer) 3
4# "id" => (integer) 6
5# "mode" => "standalone"
6# "role" => "master"
7# "modules" => (empty array)
```

``` bash
127.0.0.1:6379> hello 1
(error) NOPROTO unsupported protocol version
```

``` bash
127.0.0.1:6379> hello 4
(error) NOPROTO unsupported protocol version
```

Support for the `HELLO` command and RESP3 might be added later on but it's not currently on the roadmap of this online book.

## Back to RESP v2

The [official specification][resp-spec] goes into details about the protocol and is still reasonably short and approachable, so feel free to read it, but here are the main elements that will drive the changes to our `RedisServer` class:


### Pipelining

RESP clients can send multiple requests at once and the RESP server will write multiple responses back, this is called [pipelining][redis-pipelining]. The only constraint is that commands must be processed in the same ordered they were received, so that clients can associate the responses back to each request.

### Pub/Sub

The semantics of RESP are different when a client subscribes to a topic using the `SUB` command. We have not yet implemented the `PUB` & `SUB` command and will therefore ignore their implications for now.

### The 5 data types

RESP v2 defines five data types:

- Simple Strings
- Errors
- Integers
- Bulk Strings
- Arrays

The type of a serialized RESP data is determined by the first byte:

- Simple strings start with `+`
- Errors start with `-`
- Integers start with `:`
- Bulk strings start with `$`
- Arrays start with `*`

The data that follows the type byte depends on each type, let's look at each of them one by one.

**Simple Strings**

A simple string cannot contain a new line. One its main use case is to return `OK` back to the client. The full format of a simple string is "A `+` character, followed directly by the content of the string, followed by a carriage return (often written as `CR` or `\r`) and a line feed (often written as `LF` or `\n`).

This is why Simple Strings cannot contain multiples lines, a newline would create confusion given that it is also use a delimiter.

The common `"OK"` string, returned by the `SET` command upon success is therefore serialized as `+OK\r\n`.

`redis-cli` does the work of detecting the type of the response and only shows us the actual string, `OK`, as we can see in the example below:

``` bash
127.0.0.1:6379> SET 1 2
OK
```

Using `nc`, we can see what the full response sent back from Redis is:

``` bash
> nc -v localhost 6379
SET 1 2
+OK

```

`nc` does not explicitly display invisible characters such as `CR` & `LF`, so it is hard to know for sure that they were returned, beside the newline printed after `+OK`. The `hexdump` command is useful here, it allows us to see all the bytes:

``` bash
echo "SET 1 2" | nc -v localhost 6379 | hexdump -C
# ...
00000000  2b 4f 4b 0d 0a                                    |+OK..|
00000005
```

The interesting part is the middle one, `2b 4f 4b 0d 0a`, these are the 5 bytes returned by Redis. The part to the right, between pipe characters (`|`) is their ASCII representation. We can see five characters there, `+` is the ASCII representation of `2b`, `O` is for `4f`, `K` is for `4d`, and the last two bytes do not have a visual representation so they're displayed as `.`.

`2b` is the hex notation of 43 (`'2b'.to_i(16)` in `irb`), and 43 maps to `+` in the [ASCII table][ascii-table]. `4f` is the equivalent of 79, and the capital letter `O`, `4b`, the number 75 and the capital letter `K`.

`0d` is the equivalent of the number 13, and the Carriage Return character (CR), and finally, `0a` is 10, the Line Feed character (LF).

Redis follows the Redis Protocol, that's a good start!

**Errors**

Errors are very similar to simple strings, they also cannot contain new line characters. The main difference is that clients should treat them as errors instead of successful results. In languages with exceptions, a client library might decide to throw an exception when receiving an error from Redis. This is what [the official ruby library][redis-ruby-client] does.

Similarly to simple strings, an errors ends with a carriage return and a line feed, let's see it in action:

``` bash
❯ echo "GET 1 2" | nc -c -v localhost 6379 | hexdump -C
# ...
00000000  2d 45 52 52 20 77 72 6f  6e 67 20 6e 75 6d 62 65  |-ERR wrong numbe|
00000010  72 20 6f 66 20 61 72 67  75 6d 65 6e 74 73 20 66  |r of arguments f|
00000020  6f 72 20 27 67 65 74 27  20 63 6f 6d 6d 61 6e 64  |or 'get' command|
00000030  0d 0a                                             |..|
00000032
```

There are more bytes, to represent the string: "Err wrong number of arguments for 'get' command", but we can see that the response starts with the `2d` byte. Looking at the [ASCII table][ascii-table], we can see that 45, the numeric equivalent of `2d` maps to `-`, so far so good.

And finally, the response ends with `0d0a`, respectively `CR` & `LF`.

**Integers**

Integers have a similar representation to simple strings and errors. The actual integer comes after the `:` character and is followed by the `CR` & `LF` characters.

An example of integer reply is with the `TTL` and `PTTL` commands

The key `key-with-ttl` was set with the command: `SET key-with-ttl value EX 1000`.

``` bash
> echo "TTL key-with-ttl" | nc -c -v localhost 6379 | hexdump -C
# ...
00000000  3a 39 38 38 0d 0a                                 |:988..|
00000006
```

The key `not-a-key` does not exist.

``` bash
> echo "TTL not-a-key" | nc -c -v localhost 6379 | hexdump -C
# ...
00000000  3a 2d 32 0d 0a                                    |:-2..|
00000005
```

The key `key-without-ttl` was set with the command: `SET key-without-ttl value`.

``` bash
> echo "TTL key-without-ttl" | nc -c -v localhost 6379 | hexdump -C
# ...
00000000  3a 2d 31 0d 0a                                    |:-1..|
00000005
```

All of these responses start with the `3a` byte, which is equivalent to 58, aka `:`. In the two cases where the response is a negative value, `-2` for a non existent key and `-1` for an existing key without a ttl the next byte is `2d`, equivalent to 45, aka `-`.

The rest of the data, before the `0d` & `0a` bytes, is the actual integer data, in ASCII format, `31` is the hex equivalent to 49, which is the character `1`, 32 is the hex equivalent to 50, which is the character `2`. `39` & `38` are respectively the hex equivalent to 57 & 56, the characters `9` & `8`.

A ruby client parsing this data would extract the string between `:` and `\r\n` and call `to_i` on it: `'988'.to_i == 988`.

**Bulk Strings**

In order to work for any strings, bulk strings need to first declare their length, and only then the actual data. This lets the receiver know how many bytes to expect, instead of reading anything until it finds `CRLF` the way it does for a simple string.

The length of the string is sent directly after the dollar sign, and is delimited by `CRLF`, the following is the actual string data, and another `CRLF` to end the string.

Interestingly, it seems like Redis does not care that much about the final `CRLF`, as long as it finds two characters there, it assumes it's the end of the bulk string and tries to process what comes after:

The following first sends the command `GET a` to redis as a multi bulk string, followed by the non existent command `NOT A COMMAND`. The response first contains the `-1` integer, followed by the error.

```ruby
irb(main):029:0> socket.write("*2\r\n$3\r\nGET\r\n$1\r\na\r\n*1\r\n$13\r\nNOT A COMMAND\r\n")
=> 35
irb(main):028:0> socket.read_nonblock(1024, exception: false)
=> "$-1\r\n-ERR unknown command `NOT`, with args beginning with: `A`, `COMMAND`, \r\n"
```

The following is handled identically by Redis, despite the fact the `a` bulk string is not terminated by `CRLF`. We can see that Redis ignored the `b` and `c` characters and proceeded with the following command, the non existent `NOT A COMMAND`. I am assuming that the code in charge of reading input first reads the length, then grabs that many bytes and jumps by two characters, regardless of what these characters are.

```ruby
irb(main):027:0> socket.write("*2\r\n$3\r\nGET\r\n$1\r\nabc*1\r\n$13\r\nNOT A COMMAND\r\n")
=> 35
irb(main):030:0> socket.read_nonblock(1024, exception: false)
=> "$-1\r\n-ERR unknown command `NOT`, with args beginning with: `A`, `COMMAND`, \r\n"
```

There's a special value for Bulk Strings, the null Bulk String. It is commonly returned when a Bulk String would otherwise be expected, but there was no value to return. This happens in many cases, such as when there are no values for the key passed to the `GET` command. RESP represents it as a string with a length of -1: `$-1\r\n`.

**Arrays**

Arrays can contain values of any types, including other nested arrays. Similarly to Bulk Strings, arrays must first declare their lengths, followed by `CRLF`, and all items come afterwards, in their regular serialized form. The following is a JSON representation of an arbitrary array:


``` json
[ 1, "a-string", [ "another-string-in-a-nested-array" ], "a-string-with\r\n-newlines" ]
```

The following is the RESP representation of the same array:

```
*4\r\n:1\r\n$8\r\na-string\r\n*1\r\n$32\r\nanother-string-in-a-nested-array\r\n$24\r\na-string-with\r\n-newlines\r\n
```

We can include newlines and indentation for the sake of readability

```
*4\r\n
  :1\r\n
  $8\r\na-string\r\n
  *1\r\n
    $32\r\nanother-string-in-a-nested-array\r\n
  $24\r\na-string-with\r\n-newlines\r\n
```

RESP has a special notation for the NULL array: `*-1\r\n`. The existence of two different NULL values, one for Bulk Strings and one for Bulk Arrays is confusing and is one of the many changes in RESP3. RESP3 has a single null value.

### Inline Protocol

RESP's main mode of operation is following a request/response model described below. It also supports a simpler alternative, called "Inline Commands", which is useful for manual tests or interactions with a server. This is similar to how we've used `nc` in this book so far.

Anything that does not start with a `*` character — which is the first character of an array, the format Redis expects for a command, more on that below — is treated as an inline command. Redis will read everything until a newline is detected and attempts to parse that as a command. This is essentially what we've been doing so far when implementing the `RedisServer` class.

Let's try this quickly with `nc`:

``` bash
> nc -c -v localhost 6379
# ...
SET 1 2
+OK
GET 1
$1
2

```

As we're about to see, RESP's main mode of operations is more complicated. This complexity is necessary because inline commands are severely limited. It is impossible to store a key or a value that contains the carriage return and line feed characters since they're use as delimiters even though Redis does support any things as keys and values as seen in the following example:

``` bash
> redis-cli
127.0.0.1:6379> SET a-key "foo\nbar"
OK
127.0.0.1:6379> GET a-key
"foo\nbar"
```

Let's double check with `nc` to see what Redis stored:

``` bash
> nc -c -v localhost 6379
# ...
GET a-key
$7
foo
bar

```

We could also use `hexdump` to triple check:

``` bash
> echo "GET a-key" | nc -c -v localhost 6379 | hexdump -C
# ...
00000000  24 37 0d 0a 66 6f 6f 0a  62 61 72 0d 0a           |$7..foo.bar..|
0000000d
```

We can see the `0a` byte between `o`/`6f` & `b`/`62`

### Requests & Responses

Requests are send as arrays of bulk strings. The command `GET a-key` should be sent as `*2\r\n$3\r\nGET\r\n$5\r\na-key\r\n`, or in plain English: "An array of length 2, where the first string is of length 3 and is GET and the second string is of length 5 and is a-key".

We can illustrate this by sending this string with the `TCPSocket` class in ruby:

```ruby
irb(main):001:0> require 'socket'
=> true
irb(main):002:0> socket = TCPSocket.new 'localhost', 6379
irb(main):003:0> socket.write "*2\r\n$3\r\nGET\r\n$5\r\na-key\r\n"
=> 24
irb(main):004:0> socket.read_nonblock 1024
=> "$-1\r\n"
```

### Pub/Sub

Redis supports a [Publish/Subscribe messaging paradigm][pub-sub-wikipedia], with the `SUBSCRIBE`, `UNSUBSCRIBE` &  `PUBLISH` commands, documented on [Pub/Sub page][redis-doc-pub-sub] of the official documentation.

These commands have a significant impact of how data flows between clients and servers, and given that we have not yet added support for pipelining and pub/sub, we will ignore their impact on our implementation of the Redis Protocol for now. Future chapters will add support for these two supports and will follow the RESP specification.

## Making Redis::Server speak RESP

As far as I know there is no official test suite that we could run our server against to validate that it correctly follows RESP. What we can do instead is rely on `redis-cli` as a way to test the RESP implementation of our `Redis::Server` class. Let's see what happens when we try it with the current server, first let's start the server from Chapter 4:

```
DEBUG=t ruby -r"./server" -e "RedisServer.new"
```

and in another shell, let's open `redis-cli` on port 2000:

```
> redis-cli -p 2000
```

You should see the following the server logs:

```
D, [2020-08-12T16:11:42.461645 #91271] DEBUG -- : Received command: *1
D, [2020-08-12T16:11:42.461688 #91271] DEBUG -- : Response: (error) ERR unknown command `*1`, with args beginning with:
D, [2020-08-12T16:11:42.461925 #91271] DEBUG -- : Received command: $7
D, [2020-08-12T16:11:42.461960 #91271] DEBUG -- : Response: (error) ERR unknown command `$7`, with args beginning with:
D, [2020-08-12T16:11:42.462005 #91271] DEBUG -- : Received command: COMMAND
D, [2020-08-12T16:11:42.462036 #91271] DEBUG -- : Response: (error) ERR unknown command `COMMAND`, with args beginning with:
```

The server received the string `"*1\r\n$7\r\nCOMMAND\r\n"`, which is the RESP representation of the string `"COMMAND"` in a single item array, `[ "COMMAND" ]` in JSON.

The [`COMMAND` command][redis-doc-command] is useful when running Redis [in a cluster][redis-doc-cluster]. Given that we have not yet implementer cluster capabilities, going into details about the `COMMAND` command is a little bit out of scope. In short the `COMMAND` command is useful to provide meta information about each command, such as information about the positions of they keys. This is useful because in cluster mode, clients have to route requests to the different nodes in the cluster. It is common for a command to have the key as the second element, the one coming directly after the command itself. This happens to be the case for all the commands we've implemented so far. But some commands have different semantics. For instance [`MSET`][redis-doc-mset] can contain multiple keys, so clients need to know where the keys are in the command. While rare, some commands have the first key at a different index, this is the case for the [`OBJECT` command][redis-doc-object].

If you then try to send a command, `GET 1` for instance, `redis-cli` will crash after printing the following error:

```
Error: Protocol error, got "(" as reply type byte
```

This is because our server writes the string `(nil)` when it does find an try for the given key. But as we've seen, the correct response should `$-1\r\n` to follow RESP. This is what `redis-cli` tells us before stopping, it expected a "type byte", one of `+`, `-`, `:`, `$` or `*`, but instead got `(`.

So we now what we have to do, implement the `COMMAND` command, since `redis-cli` seems to use it by default, and change how process client input, to support "real" RESP and the inline version, as well as serializing responses following RESP.

Let's get to it!

### Parsing Input

**Modules & Namespaces**

Most of the changes here will take pace in `server.rb`. As the code started to grow, I thought it would be easier to start using ruby modules, so I nested the `Server` class under the `Redis` namespace. This will allow us to create other classes & modules under the `Redis` namespace as well. All the other classes have been updated to be under the `Redis` namespace as well. `ExpireHelper` is now `Redis::ExpireHelper`:

``` ruby
module Redis
  module ExpireHelper

    def self.check_if_expired(data_store, expires, key)
      # ...
    end
  end
end
```
_listing 5.x: expire_helper.rb_

**Storing partial client buffer**

As of the previous chapter we never stored the client input. We would read from the socket when `IO.select` would tell us there is something to read. Read until the end of the line, and process the result as a command.

It turns out that this approach is a bit too aggressive. Clients should be able to send a single command in two parts, there's no reason to treat that as an error.

In order to do this, we are going to create a `Client` struct to hold the client socket as well a string containing all the pending input we have not process yet:

``` ruby
Client = Struct.new(:socket, :buffer) do
  def initialize(socket)
    self.socket = socket
    self.buffer = ''
  end
end
```
_listing 5.x: server.rb_

We need to adapt `process_poll_events` to use this new class instead of the raw socket coming as a result of `TCPServer#accept`:

``` ruby
def process_poll_events(sockets)
  sockets.each do |socket|
    begin
      if socket.is_a?(TCPServer)
        @clients << Client.new(@server.accept)
      elsif socket.is_a?(TCPSocket)
        client = @clients.find { |client| client.socket == socket }
        client_command_with_args = socket.read_nonblock(1024, exception: false)
        if client_command_with_args.nil?
          @clients.delete(client)
          socket.close
        elsif client_command_with_args == :wait_readable
          # ...
        else
          # We now need to parse the input as a RESP array
          # ...
        end
      else
        # ...
      end
    rescue Errno::ECONNRESET
      @clients.delete_if { |client| client.socket == socket }
    end
  end
end
```
_listing 5.x: server.rb_

**Parsing commands as RESP Arrays**

Going back to `process_poll_events`, let's delegate the processing of the client input to a different method:

``` ruby
def process_poll_events(sockets)
  sockets.each do |socket|
    begin
      elsif socket.is_a?(TCPSocket)
        # ...
        else
          client.buffer += client_command_with_args
          split_commands(client.buffer) do |command_parts, command_length|
            response = handle_client_command(command_parts)
            @logger.debug "Response: #{ response.class } / #{ response.inspect }"
            @logger.debug "Writing: '#{ response.serialize.inspect }'"
            socket.write response.serialize
          end
        end
      else
        # ...
      end
      # ...
    end
  end
end

def split_commands(client_buffer)
  @logger.debug "Full result from read: '#{ client_buffer.inspect }'"

  scanner = StringScanner.new(client_buffer)
  if client_buffer[0] == '*'
    until scanner.eos?
      command_parts = parse_value_from_string(scanner)
      raise ProtocolError, "ERR Protocol Error: not an array" unless command_parts.is_a?(Array)

      yield command_parts, 0
    end
  else
    until scanner.eos?
      command = scanner.scan_until(/\r\n/)
      raise IncompleteCommand, scanner.string if command.nil?

      yield command.split.map(&:strip), client_buffer.length
    end
  end

  # Remove the processed commands from the client buffer, and leave either nothing or
  # the beginning of an incomplete command
  client_buffer.slice!(0, scanner.charpos)
end
```

`split_commands` is in charge of splitting the client input into multiple commands, which is necessary to support pipelining.

It also handles the two different versions of commands, inline, or "regular", as RESP Arrays. We use the `StringScanner` class, which is really convenient to process data from a string, from left to right.

We first peek at the first character, if it is `*`, the following should be a RESP array, and we process it as such. Otherwise, we assume that we're dealing with an inline command


### Case insensitivity

It is not explicitly mentioned in the RESP v2 documentation, but Redis treats commands and options as case insensitive. The following are valid: `get 1`, `GeT 1`, `set key value EX 1 nx`.

In order to apply the same handling logic, we changed the keys in the `COMMANDS` constant to be lower case, and we always lower case the client input when attempting to find a handler for the command:

``` ruby
COMMANDS = {
  'command' => CommandCommand,
  'get' => GetCommand,
  'set' => SetCommand,
  'ttl' => TtlCommand,
  'pttl' => PttlCommand,
}
# ...

def handle_client_command(command_parts)
  @logger.debug "Received command: #{ command_parts }"
  command_str = command_parts[0]
  args = command_parts[1..-1]

  command_class = COMMANDS[command_str.downcase]

  # ...
end
```
_listing 5.x: server.rb_

We also need to update the `Redis::SetCommand` class to handle options regardless of the case chosen by clients:

``` ruby

```
_listing 5.x: set_command.rb_

### Something else?

## Conclusion

We can now use redis-cli, with `redis-cli -p 2000` to interact with our redis server. There are many commands

In the next chapter we'll write our own Hashing algorithm and ban the use of the `Hash` class in our code.

### Code

It's [on GH][github-code], as usual
d

[github-code]:https://github.com/pjambet/redis-in-ruby/tree/master/code/chapter-5
[resp-spec]:https://redis.io/topics/protocol
[resp3-spec]:https://github.com/antirez/RESP3/blob/master/spec.md
[resp3-spec-background]:https://github.com/antirez/RESP3/blob/master/spec.md#background
[release-notes-6-0]:https://github.com/redis/redis/blob/6.0/00-RELEASENOTES
[redis-doc-hello]:http://redis.io/commands/hello
[ascii-table]:http://www.asciitable.com/
[redis-ruby-client]:https://github.com/redis/redis-rb
[redis-pipelining]:https://redis.io/topics/pipelining
[redis-doc-object]:http://redis.io/commands/object
[redis-doc-mset]:http://redis.io/commands/mset
[redis-doc-command]:http://redis.io/commands/command
[pub-sub-wikipedia]:http://en.wikipedia.org/wiki/Publish/subscribe
[redis-doc-pub-sub]:https://redis.io/topics/pubsub
[ruby-doc-string-scanner]:http://ruby-doc.org/stdlib-2.7.1/libdoc/strscan/rdoc/StringScanner.html
