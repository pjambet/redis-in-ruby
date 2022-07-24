---
title: "Chapter 5 - Redis Protocol Compatibility"
date: 2020-08-14T10:53:06-04:00
lastmod: 2020-08-17T13:28:40-04:00
draft: false
comment: false
keywords: []
summary: "In this chapter we will focus on making RedisServer speak the Redis Protocol, RESP. Doing so will allow us to use Redis' built-in client, redis-cli to communicate with our own server."
---

## What we'll cover

By the end of this chapter `RedisServer` will speak [the Redis Protocol, `RESP v2`][resp-spec]. Doing this will allow any clients that was written to communicate with the real Redis to also communicate with our own server, granted that the commands it uses are within the small subset of the ones we implemented.

One such client is the `redis-cli` utility that ships with Redis, it'll look like this:

![redis-cli-gif](/img/redis.gif)

[RESP v2][resp-spec] has been the protocol used by Redis since version 2.0, to quote the documentation:

> 1.2 already supported it, but Redis 2.0 was the first version to talk only this protocol)

As of version 6.0, RESP v2 is still the default protocol and is what we'll implement in this chapter.

## RESP3

RESP v2 is the default version, but not the latest one. RESP3 has been released in 2018, it improves many different aspects of RESP v2, such as adding new types for maps — often called dictionary — and a lot more. The spec is [on GitHub][resp3-spec] and explains in details [the background behind it][resp3-spec-background].
RESP3 is supported as of Redis 6.0, as indicated in [the release notes][release-notes-6-0]:

> Redis now supports a new protocol called RESP3, which returns more semantical replies: new clients using this protocol can understand just from the reply what type to return to the calling program.

The [`HELLO`][redis-doc-hello] command can be used to switch the connection to a different protocol version. As we can see below, only two versions are currently supported, 2 & 3. We can also see the new map type in action, `hello 2` returned an array with 14 items, representing 7 key/value pairs, whereas `hello 3` leveraged the new map type to return a map with 7 key/value pairs.

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

Support for the `HELLO` command and RESP3 might be added in a later chapter but it's not currently on the roadmap of this online book.

## Back to RESP v2

The [official specification][resp-spec] goes into details about the protocol and is still reasonably short and approachable, so feel free to read it, but here are the main elements that will drive the changes to our server.


### The 5 data types

RESP v2 defines five data types:

- Simple Strings
- Errors
- Integers
- Bulk Strings
- Arrays

The type of a serialized RESP data is determined by the first byte:

- Simple Strings start with `+`
- Errors start with `-`
- Integers start with `:`
- Bulk Strings start with `$`
- Arrays start with `*`

The data that follows the type byte depends on each type, let's look at each of them one by one.

**Simple Strings**

A Simple String cannot contain a new line. One of its main use cases is to return `OK` back to the client. The full format of a Simple String is "A `+` character, followed directly by the content of the string, followed by a carriage return (often written as `CR` or `\r`) and a line feed (often written as `LF` or `\n`).

This is why Simple Strings cannot contain multiples lines, a newline would create confusion given that it is also use a delimiter.

The `"OK"` string, here shown in its JSON form, returned by the `SET` command upon success is therefore serialized as `+OK\r\n`.

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
echo "SET 1 2" | nc localhost 6379 | hexdump -C
# ...
00000000  2b 4f 4b 0d 0a                                    |+OK..|
00000005
```

The interesting part is the middle one, `2b 4f 4b 0d 0a`, these are the 5 bytes returned by Redis. The part to the right, between pipe characters (`|`) is their ASCII representation. We can see five characters there, `+` is the ASCII representation of `2b`, `O` is for `4f`, `K` is for `4d`, and the last two bytes do not have a visual representation so they're displayed as `.`.

`2b` is the hex notation of 43 (`'2b'.to_i(16)` in `irb`), and 43 maps to `+` in the [ASCII table][ascii-table]. `4f` is the equivalent of 79, and the capital letter `O`, `4b`, the number 75 and the capital letter `K`.

`0d` is the equivalent of the number 13, and the carriage return character (CR), and finally, `0a` is 10, the line feed character (LF).

Redis follows the Redis Protocol, that's a good start!

**Errors**

Errors are very similar to Simple Strings, they also cannot contain new line characters. The main difference is that clients should treat them as errors instead of successful results. In languages with exceptions, a client library might decide to throw an exception when receiving an error from Redis. This is what [the official ruby library][redis-ruby-client] does.

Similarly to Simple Strings, errors end with a carriage return and a line feed, let's see it in action:

``` bash
> echo "GET 1 2" | nc localhost 6379 | hexdump -C
00000000  2d 45 52 52 20 77 72 6f  6e 67 20 6e 75 6d 62 65  |-ERR wrong numbe|
00000010  72 20 6f 66 20 61 72 67  75 6d 65 6e 74 73 20 66  |r of arguments f|
00000020  6f 72 20 27 67 65 74 27  20 63 6f 6d 6d 61 6e 64  |or 'get' command|
00000030  0d 0a                                             |..|
00000032
```

There are more bytes here, they represent the string: `"Err wrong number of arguments for 'get' command"`, but we can see that the response starts with the `2d` byte. Looking at the [ASCII table][ascii-table], we can see that 45, the numeric equivalent of `2d`, maps to `-`, so far so good.

And finally, the response ends with `0d0a`, respectively `CR` & `LF`.

**Integers**

Integers have a similar representation to Simple Strings and errors. The actual integer comes after the `:` character and is followed by the `CR` & `LF` characters.

An example of integer reply is with the `TTL` and `PTTL` commands

The key `key-with-ttl` was set with the command: `SET key-with-ttl value EX 1000`.

``` bash
> echo "TTL key-with-ttl" | nc localhost 6379 | hexdump -C
# ...
00000000  3a 39 38 38 0d 0a                                 |:988..|
00000006
```

The key `not-a-key` does not exist.

``` bash
> echo "TTL not-a-key" | nc localhost 6379 | hexdump -C
# ...
00000000  3a 2d 32 0d 0a                                    |:-2..|
00000005
```

The key `key-without-ttl` was set with the command: `SET key-without-ttl value`.

``` bash
> echo "TTL key-without-ttl" | nc localhost 6379 | hexdump -C
# ...
00000000  3a 2d 31 0d 0a                                    |:-1..|
00000005
```

All of these responses start with the `3a` byte, which is equivalent to 58, aka `:`. In the two cases where the response is a negative value, `-2` for a non existent key and `-1` for an existing key without a ttl, the next byte is `2d`, equivalent to 45, aka `-`.

The rest of the data, before the `0d` & `0a` bytes, is the actual integer data, in ASCII format, `31` is the hex equivalent to 49, which is the character `1`, 32 is the hex equivalent to 50, which is the character `2`. `39` & `38` are respectively the hex equivalent to 57 & 56, the characters `9` & `8`.

A ruby client parsing this data would extract the string between `:` and `\r\n` and call `to_i` on it: `'988'.to_i == 988`.

**Bulk Strings**

In order to work for any strings, Bulk Strings need to first declare their length, and only then the actual data. This lets the receiver know how many bytes to expect, instead of reading anything until it finds `CRLF`, the way it does for a Simple String.

The length of the string is sent directly after the dollar sign, and is delimited by `CRLF`, the following is the actual string data, and another `CRLF` to end the string.

The RESP Bulk String representation of the JSON string `"GET"` is: `$3\r\nGET\r\n`.

Interestingly, it seems like Redis does not care that much about the final `CRLF`, as long as it finds two characters there, it assumes it's the end of the Bulk String and tries to process what comes after.

In the following example, we first send the command `GET a` to Redis over port 6379, as a an array of Bulk Strings, followed by the non existent command `NOT A COMMAND`. The response first contains the `-1` integer, followed by the error.

```ruby
irb(main):001:0> require 'socket'
=> true
irb(main):002:0> socket = TCPSocket.new 'localhost', 6379
irb(main):004:0> socket.write("*2\r\n$3\r\nGET\r\n$1\r\na\r\n*1\r\n$13\r\nNOT A COMMAND\r\n")
=> 35
irb(main):005:0> socket.read_nonblock(1024, exception: false)
=> "$-1\r\n-ERR unknown command `NOT`, with args beginning with: `A`, `COMMAND`, \r\n"
```

The following is handled identically by Redis, despite the fact the `a` Bulk String is not terminated by `CRLF`. We can see that Redis ignored the `b` and `c` characters and proceeded with the following command, the non existent `NOT A COMMAND`. I am assuming that the code in charge of reading client input first reads the length, then grabs that many bytes and jumps by two characters, regardless of what these characters are.

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

### Requests & Responses

As we saw in a previous example, requests are sent as arrays of Bulk Strings. The command `GET a-key` should be sent as `*2\r\n$3\r\nGET\r\n$5\r\na-key\r\n`, or in plain English: "An array of length 2, where the first string is of length 3 and is GET and the second string is of length 5 and is a-key".

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

### Inline Protocol

RESP's main mode of operation is following a request/response model described above. It also supports a simpler alternative, called "Inline Commands", which is useful for manual tests or interactions with a server. This is similar to how we've used `nc` in this book so far.

Anything that does not start with a `*` character — which is the first character of an array, the format Redis expects for a command — is treated as an inline command. Redis will read everything until a newline is detected and attempts to parse that as a command. This is essentially what we've been doing so far when implementing the `RedisServer` class.

Let's try this quickly with `nc`:

``` bash
> nc localhost 6379
# ...
SET 1 2
+OK
GET 1
$1
2

```

The reason RESP's main mode of operations is more complicated is because inline commands are severely limited. It is impossible to store a key or a value that contains the carriage return and line feed characters since they're use as delimiters even though Redis does support any strings as keys and values as seen in the following example:

``` bash
> redis-cli
127.0.0.1:6379> SET a-key "foo\nbar"
OK
127.0.0.1:6379> GET a-key
"foo\nbar"
```

Let's double check with `nc` to see what Redis stored:

``` bash
> nc localhost 6379
# ...
GET a-key
$7
foo
bar

```

We could also use `hexdump` to triple check:

``` bash
> echo "GET a-key" | nc localhost 6379 | hexdump -C
# ...
00000000  24 37 0d 0a 66 6f 6f 0a  62 61 72 0d 0a           |$7..foo.bar..|
0000000d
```

We can see the `0a` byte between `o`/`6f` & `b`/`62`.

Without inline commands sending test commands would be excruciating:

``` bash
> nc -c localhost 6379
*2
$3
GET
$1
a
$1
1
```

Note that we're using the `-c` flags, which tells `nc` to send `CRLF` characters when we type the return key, instead of the default of `LF`. As we've seen above, for RESP arrays, RESP expects `CRLF` delimiters.

### Pub/Sub

Redis supports a [Publish/Subscribe messaging paradigm][pub-sub-wikipedia], with the `SUBSCRIBE`, `UNSUBSCRIBE` &  `PUBLISH` commands, documented on [Pub/Sub page][redis-doc-pub-sub] of the official documentation.

These commands have a significant impact of how data flows between clients and servers, and given that we have not yet added support for pub/sub, we will ignore its impact on our implementation of the Redis Protocol for now. Future chapters will add support for pub/sub and will follow the RESP specification.

### Pipelining

RESP clients can send multiple requests at once and the RESP server will write multiple responses back, this is called [pipelining][redis-pipelining]. The only constraint is that commands must be processed in the same ordered they were received, so that clients can associate the responses back to each request.

The following is an example of sending two commands at once and then reading the two responses, in Ruby:

``` ruby
irb(main):001:0> require 'socket'
=> true
irb(main):002:0> socket = TCPSocket.new 'localhost', 6379
irb(main):003:0> socket.write "SET 1 2\r\nGET 1\r\n"
=> 16
irb(main):004:0> socket.read_nonblock 1024
=> "+OK\r\n$1\r\n2\r\n"
```

We first wrote the string `"SET 1 2\r\nGET 1\r\n"`, which represents the command `SET 1 2` and the command `GET ` in the inline format.

The response we get from the server is a string containing the two responses, fist the Simple String `+OK\r\n`, followed by the Bulk String `$1\r\n2\r\n`.

## Making our Server speak RESP

As far as I know there is no official test suite that we could run our server against to validate that it correctly follows RESP. What we can do instead is rely on `redis-cli` as a way to test the RESP implementation of our server. Let's see what happens when we try it with the current server. First let's start the server from Chapter 4:

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

The [`COMMAND` command][redis-doc-command] is useful when running Redis [in a cluster][redis-doc-cluster]. Given that we have not yet implementer cluster capabilities, going into details about the `COMMAND` command is a little bit out of scope. In short the `COMMAND` command is useful to provide meta information about each command, such as information about the positions of the keys. This is useful because in cluster mode, clients have to route requests to the different nodes in the cluster. It is common for a command to have the key as the second element, the one coming directly after the command itself. This happens to be the case for all the commands we've implemented so far. But some commands have different semantics. For instance [`MSET`][redis-doc-mset] can contain multiple keys, so clients need to know where the keys are in the command. While rare, some commands have the first key at a different index, this is the case for the [`OBJECT` command][redis-doc-object].

Back to `redis-cli` running against our Redis server, if you then try to send a command, `GET 1` for instance, `redis-cli` will crash after printing the following error:

```
Error: Protocol error, got "(" as reply type byte
```

This is because our server writes the string `(nil)` when it does find an try for the given key. `(nil)` is what `redis-cli` displays when it receives a null Bulk String, as we can see with the following example, we first send the `GET 1` command with `redis-cli` and then with `nc` and observe the response in each case:

``` bash
> nc -c localhost 6379
GET 1
$-1
# ...
> redis-cli
127.0.0.1:6379> GET 1
(nil)
```

Our server must send the null Bulk String, `$-1\r\n`, to follow RESP. This is what `redis-cli` tells us before stopping, it expected a "type byte", one of `+`, `-`, `:`, `$` or `*`, but instead got `(`.

In order to use `redis-cli` against our own server, we should implement the `COMMAND` command, since it sends it directly after starting. We also need to change how we process client input, to parse RESP arrays of Bulk Strings. We also need to support inline commands. Finally, we also need to update the responses we write back, and serialize responses following RESP.

Let's get to it!

### Parsing Client Input

**Modules & Namespaces**

Most of the changes will take place in `server.rb`. As the codebase started to grow, I thought it would be easier to start using ruby modules, so I nested the `Server` class under the `Redis` namespace. This will allow us to create other classes & modules under the `Redis` namespace as well. All the other classes have been updated to be under the `Redis` namespace as well, e.g. `ExpireHelper` is now `BYORedis::ExpireHelper`. `BYO` stands for **B**uild **Y**our **O**wn. I'm purposefully not using `Redis` as it is already used by the popular [`redis`][redis-gem] gem. We're not using both at the same time in the same project for now, so it wouldn't really have been a problem. But say that you would like to use the `redis` gem to communicate with the server we're building, we will prevent any kind of unexpected errors by using different names.

``` ruby
# expire_helper.rb
module BYORedis
  module ExpireHelper

    def self.check_if_expired(data_store, expires, key)
      # ...
    end
  end
end
```
_listing 5.1: Nesting ExpireHelper under the Redis module_

**Storing partial client buffer**

As of the previous chapter we never stored the client input. We would read from the socket when `IO.select` would tell us there is something to read, read until the end of the line, and process the result as a command.

It turns out that this approach is a bit too aggressive. Clients should be able to send a single command in two parts, there's no reason to treat that as an error.

In order to do this, we are going to create a `Client` struct to hold the client socket as well a string containing all the pending input we have not process yet:

``` ruby
# server.rb
Client = Struct.new(:socket, :buffer) do
  def initialize(socket)
    self.socket = socket
    self.buffer = ''
  end
end
```
_listing 5.2: The new Client class_

We need to adapt `process_poll_events` to use this new class instead of the raw socket coming as a result of `TCPServer#accept`:

``` ruby
# server.rb
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
_listing 5.3: Updated handling of socket in server.rb_

**Parsing commands as RESP Arrays**

More things need to change in `process_poll_events`. We first append the result from `read_nonblock` to `client.buffer`, which will allow us to continue appending until we accumulate enough to read a whole command. We then delegate the processing of `client.buffer` to a different method, `split_commands`:

``` ruby
# server.rb
def process_poll_events(sockets)
  sockets.each do |socket|
    begin
      # ...
      elsif socket.is_a?(TCPSocket)
        # ...
        else
          client.buffer += client_command_with_args
          split_commands(client.buffer) do |command_parts|
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

  scanner = StringScanner.new(client_buffer.dup)
  until scanner.eos?
    if scanner.peek(1) == '*'
      yield parse_as_resp_array(scanner)
    else
      yield parse_as_inline_command(scanner)
    end
    client_buffer.slice!(0, scanner.charpos)
  end
end
#...
```
_listing 5.4 Updated handling of client input in server.rb_

`split_commands` is in charge of splitting the client input into multiple commands, which is necessary to support pipelining. As a reminder, since we're adding support pipelining, we have to assume that the content of `client.buffer` might contain more than one command, and if so, we want to process them all in the order we received them, and write the responses back, in the same order.

It also handles the two different versions of commands, inline, or "regular", as RESP Arrays. We use the `StringScanner` class, which is really convenient to process data from a string, from left to right. We call `String#dup` on the argument to `StringScanner` to make sure that the `StringScanner` gets its own instance. As we iterate through `client.buffer`, every time we find a whole command, we want to remove it from the client input. We do this with `client_buffer.slice!(0, scanner.charpos)`. If `client_buffer` contains two commands, i.e. `GET a\r\nGET b\r\n`, once we processed `GET a`, we want to remove the first 7 characters from the string: `GET a\r\n`, so that we never attempt to process them again. Note that we only do this after yielding, meaning that we only ever treat a command as done after we successfully wrote to the socket.

We first peek at the first character, if it is `*`, the following should be a RESP array, and we process it as such. Otherwise, we assume that we're dealing with an inline command. Each branch delegates to a method handling the parsing of the string.

The `yield` approach allows us to process each parsed command one by one, once parsed, we `yield` it, and it is handled by the `handle_client_command` method, which has barely changed from the previous chapter.

Let's look at the `parse_as_resp_array` & `parse_as_inline_command` methods:

``` ruby
def parse_as_inline_command(client_buffer, scanner)
  command = scanner.scan_until(/(\r\n|\r|\n)+/)
  raise IncompleteCommand if command.nil?

  command.split.map(&:strip)
end

def parse_as_resp_array(scanner)
  unless scanner.getch == '*'
    raise 'Unexpectedly attempted to parse a non array as an array'
  end

  expected_length = scanner.scan_until(/\r\n/)
  raise IncompleteCommand if expected_length.nil?

  expected_length = parse_integer(expected_length, 'invalid multibulk length')
  command_parts = []

  expected_length.times do
    raise IncompleteCommand if scanner.eos?

    parsed_value = parse_as_resp_bulk_string(scanner)
    raise IncompleteCommand if parsed_value.nil?

    command_parts << parsed_value
  end

  command_parts
end

def parse_integer(integer_str, error_message)
  begin
    value = Integer(integer_str)
    if value < 0
      raise ProtocolError, "ERR Protocol error: #{ error_message }"
    else
      value
    end
  rescue ArgumentError
    raise ProtocolError, "ERR Protocol error: #{ error_message }"
  end
end
```
_listing 5.5 Parsing RESP Arrays in server.rb_

`parse_as_inline_command` starts by calling `StringScanner#scan_until`, with `/\r\n/`. `scan_until` keeps iterating through the string, until it encounters something that matches its argument. In our case it will keep going through `client_buffer` until it finds `CRLF`, if it doesn't find a match, it returns `nil`. We're not even trying to process the string in this case, it is incomplete, so we'll leave it in there and eventually reattempt later on, the next time we read from this client.

If the string returned is not `nil`, it contains the string, and in this case, we do what we used to, we split it on spaces, and return it as an array of string parts, e.g. `GET 1\r\n` would be returned as `[ 'GET', '1' ]`

`parse_as_resp_array` is more complicated. As a sanity check, we test again that the first character is indeed `*`, `getch` also moves the internal cursor of `StringScanner`, moving it to the first character of the expected length. Using `scan_until` we extract all the characters until the first `CRLF` characters in the client input.

If `nil` is returned, this means that we reached the end of the string without encountering `CR` & `LF`, and instead of treating this as a client error, we raise an `IncompleteCommand` error, to give the client a change to write the missing parts of the command later on.

`expected_length` will contain a string composed of the characters before `CRLF` & the `CRLF` characters. For instance, if the scanner was created with the string `$3\r\nabc\r\n` — The Bulk String representation of the string `"3"` — `expected_length` would be equal to `"3\r\n"`. The Ruby `String#to_i` is not strict enough here. It returns `0` in a lot of cases where we'd want an error instead, such as `"abc".to_i == 0`. We instead use the `Kernel.Integer` method, which raises an `ArgumentError` exception with invalid strings. We catch `ArgumentError` and raise a `ProtocolError` instead.

In the next step we iterate as many times as the value of `expected_length` with `expected_length.times`. We start each iteration by checking if we reached the end of the string with `eos?`. If we did, then instead of returning a protocol error, we raise an `IncompleteCommand` exception. This gives a chance to the client to send the remaining elements of the array later on.

As mentioned above, a request to Redis is always an array of Bulk Strings, so we attempt to parse all the elements as strings, by calling `parse_as_bulk_string` with the same `scanner` instance. Before looking at the method, let's see how the two new exceptions `IncompleteCommand` & `ProtocolError` are defined and handled:


`IncompleteCommand` & `ProtocolError` are custom exceptions defined at the top of the file:

``` ruby
# server.rb
IncompleteCommand = Class.new(StandardError)
ProtocolError = Class.new(StandardError) do
  def serialize
    RESPError.new(message).serialize
  end
end
```
_listing 5.6 The new exceptions in server.rb_


`RESPError` is defined in `resp_types.rb`:

``` ruby
# resp_types.rb
module BYORedis
  RESPError = Struct.new(:message) do
    def serialize
      "-#{ message }\r\n"
    end
  end
  # ...
end
```
_listing 5.7 The new RESPError class_

They are handled in the `begin/rescue` block in `process_poll_events`:

``` ruby
# server.rb
begin
  # ...
rescue Errno::ECONNRESET
  @clients.delete_if { |client| client.socket == socket }
rescue IncompleteCommand
  # Not clearing the buffer or anything
  next
rescue ProtocolError => e
  socket.write e.serialize
  socket.close
  @clients.delete(client)
end
```
_listing 5.8 Handling the new exceptions in server.rb_

We don't write anything back when encountering an `IncompleteCommand` exception, we assume that the client has not finished sending the command. On the other hand, for `ProtocolError`, we write an error back to the client, following the format of a RESP error and we disconnect the client. This is what Redis does too.

Back to `parse_as_resp_bulk_string`:

``` ruby
# server.rb
def parse_as_resp_bulk_string(scanner)
  type_char = scanner.getch
  unless type_char == '$'
    raise ProtocolError, "ERR Protocol error: expected '$', got '#{ type_char }'"
  end

  expected_length = scanner.scan_until(/\r\n/)
  raise IncompleteCommand if expected_length.nil?

  expected_length = parse_integer(expected_length, 'invalid bulk length')
  bulk_string = scanner.rest.slice(0, expected_length)

  raise IncompleteCommand if bulk_string.nil? || bulk_string.length != expected_length

  scanner.pos += bulk_string.bytesize + 2
  bulk_string
end
```
_listing 5.9 Parsing Bulk Strings_


The first step is calling `StringScanner#getch`, it moves the internal cursor of the scanner by one character and returns it. If the first character is `$`, we received a Bulk String as expected. Anything else is an error.

Redis accepts empty strings, and while it may be unusual, it is possible for a Redis key to be an empty string, and a value can also be an empty string. If the expected length is negative, then we stop and return a `ProtocolError`

The next step is extracting the actual string. `StringScanner` maintains an internal cursor of the progress through the string. At this point this cursor is right after `CRLF`, where the string content starts. `StringScanner#rest` returns the string from this cursor until the end, and using `slice`, we extract only the number of characters indicated by `expected_length`.

If the result of this operation is `nil` or shorter than the expected length, we don't want to treat it as an error yet, since it is possible for the clients to write the missing elements of the command, so we raise an `IncompleteCommand`, in the hope that the client will send the missing parts later on.

The final step is to advance the cursor position in the `StringScanner` instance. We do this with the `StringScanner#pos=` method. Notice how we use the `bytesize` methods and two to it. We use `bytesize` instead of `length` to handle characters that span over multiple bytes, such as [CJK characters][wikipedia-cjk], accentuated characters, emojis and many others. Let's look at the difference in `irb`:

``` ruby
irb(main):045:1* def print_length_and_bytesize(str)
irb(main):046:1*   puts str.length
irb(main):047:1*   puts str.bytesize
irb(main):048:0> end
=> :print_length_and_bytesize
irb(main):049:0> print_length_and_bytesize('a')
1
1
=> nil
irb(main):050:0> print_length_and_bytesize('é')
1
2
=> nil
irb(main):051:0> print_length_and_bytesize('你')
1
3
=> nil
irb(main):058:0> print_length_and_bytesize('😬')
1
4
=> nil
```

As we can see, all of these strings return `1` for `length`, but different values, respectively 2, 3 & 4 for `bytesize`. Going into details about UTF-8 encoding is out of scope, but the main takeaway from this is that what we consider to be a single character, might span over multiple bytes.

If a client had sent `你` as a Bulk String, we'd expect it to pass the length as 3, and therefore we need to advance the cursor by 3 in the `StringScanner` instance. We also add two to account for the trailing `CRLF` characters. Note that, like Redis, we do not actually check that these two characters are indeed `CR` & `LF`, we just skip over them.


### Updating the command responses

The commands we've implemented so far, `GET`, `SET`, `TTL` & `PTTL` do not return data that follows the format defined in RESP. `GET` needs to return Bulk Strings, `SET` returns the Simple String `OK` or the null Bulk String if it didn't set the value and the last two, `TTL` & `PTTL`, return integers. We will first create new classes to wrap the process of serializing strings and integers to their matching RESP format:

``` ruby
# resp_types.rb
module BYORedis
  # ...
  RESPInteger = Struct.new(:underlying_integer) do
    def serialize
      ":#{ underlying_integer }\r\n"
    end

    def to_i
      underlying_integer.to_i
    end
  end

  RESPSimpleString = Struct.new(:underlying_string) do
    def serialize
      "+#{ underlying_string }\r\n"
    end
  end

  OKSimpleStringInstance = Object.new.tap do |obj|
    OK_SIMPLE_STRING = "+OK\r\n".freeze
    def obj.serialize
      OK_SIMPLE_STRING
    end
  end

  RESPBulkString = Struct.new(:underlying_string) do
    def serialize
      "$#{ underlying_string.bytesize }\r\n#{ underlying_string }\r\n"
    end
  end

  NullBulkStringInstance = Object.new.tap do |obj|
    NULL_BULK_STRING = "$-1\r\n".freeze
    def obj.serialize
      NULL_BULK_STRING
    end
  end

  RESPArray = Struct.new(:underlying_array) do
    def serialize
      serialized_items = underlying_array.map do |item|
        case item
        when RESPSimpleString, RESPBulkString
          item.serialize
        when String
          RESPBulkString.new(item).serialize
        when Integer
          RESPInteger.new(item).serialize
        when Array
          RESPArray.new(item).serialize
        end
      end
      "*#{ underlying_array.length }\r\n#{ serialized_items.join }"
    end
  end
  NullArrayInstance = Object.new.tap do |obj|
    NULL_ARRAY = "*-1\r\n".freeze
    def obj.serialize
      NULL_ARRAY
    end
  end
end
```
_listing 5.10 The new RESP types_

`RESPArray` is not strictly required at the moment since none of the commands we've implemented so far return array responses, but the `COMMAND` command, which we'll implement below returns an array, so it'll be useful there.

We could have chosen a few different options to represent the null array and the null list, such as adding the logic in `serialize` methods of `RESPArray` & `RESPBulkString`. I instead decided to create two globally available instances that implement the same interface, the `serialize` method. This allows the code in `server.rb` to always call `serialize` on the result it gets from calling the `call` method. On the other hand, in the `*Command` classes, it forces us to explicitly handle these null cases, which I find preferable to passing `nil` values around.

We use the `String#freeze` method to prevent accidental modifications of the values at runtime. Ruby will throw an exception if you attempt to do so:

``` ruby
irb(main):001:0> require_relative './server'
=> true
irb(main):002:0> BYORedis::NULL_BULK_STRING
=> "$-1\r\n"
irb(main):003:0> BYORedis::NULL_BULK_STRING << "a"
Traceback (most recent call last):
        4: from /Users/pierre/.rbenv/versions/2.7.1/bin/irb:23:in `<main>'
        3: from /Users/pierre/.rbenv/versions/2.7.1/bin/irb:23:in `load'
        2: from /Users/pierre/.rbenv/versions/2.7.1/lib/ruby/gems/2.7.0/gems/irb-1.2.3/exe/irb:11:in `<top (required)>'
        1: from (irb):3
FrozenError (can't modify frozen String: "$-1\r\n")
```

That said, do note that "constants" in Ruby aren't really "constants", it is possible to reassign the value at runtime:

``` ruby
irb(main):004:0> BYORedis::NULL_BULK_STRING = "something else"
(irb):4: warning: already initialized constant BYORedis::NULL_BULK_STRING
/Users/pierre/dev/redis-in-ruby/code/chapter-5/resp_types.rb:32: warning: previous definition of NULL_BULK_STRING was here
irb(main):005:0> BYORedis::NULL_BULK_STRING
=> "something else"
```

While it doesn't prevent all kinds of weird runtime issues, I do like the use of `String#freeze` to at least be explicit about the nature of the value, signifying that it is not supposed to be modified.


The `OK` Simple String is so common that I created a constant for it, `OKSimpleStringInstance`, so that it can be reused instead of having to allocate a new instance every time we need it. Only the `SetCommand` class uses it for now, but more commands use it, such as `LSET`, `MSET` and many others.

Let's start with `GET`:

``` ruby
# get_command.rb
module BYORedis
  class GetCommand

    # ...

    def call
      if @args.length != 1
        RESPError.new("ERR wrong number of arguments for 'GET' command")
      else
        key = @args[0]
        ExpireHelper.check_if_expired(@data_store, @expires, key)
        value = @data_store[key]
        if value.nil?
          NullBulkStringInstance
        else
          RESPBulkString.new(value)
        end
      end
    end
  end
end
```
_listing 5.11 Updated response in GetCommand_

Now that `BYORedis::GetCommand` has been updated, let's tackle `SetCommand`:

``` ruby
# set_command.rb
def call
  key, value = @args.shift(2)
  if key.nil? || value.nil?
    return RESPError.new("ERR wrong number of arguments for 'SET' command")
  end

  parse_result = parse_options

  existing_key = @data_store[key]

  if @options['presence'] # ...
    NullBulkStringInstance
  elsif @options['presence'] # ...
    NullBulkStringInstance
  else

    # ...

    OKSimpleStringInstance
  end

rescue ValidationError => e
  RESPError.new(e.message)
rescue SyntaxError => e
  RESPError.new(e.message)
end

```
_listing 5.12 Updated response in SetCommand_

The `SET` command has two possible outputs, either the nil string if the outcome was that nothing was set, as a result of the `NX` or `XX` options, or the Simple String `OK` if the outcome was a successful set. This is where the special case instances `NullBulkStringInstance` & `OKSimpleStringInstance` come in handy. By returning them here, the code in `server.rb` can leverage duck typing and call the `serialize` method, but under the hood, the same strings will be used, `BYORedis::OK_SIMPLE_STRING` & `BYORedis::NULL_BULK_STRING`. This is a very small optimization, but given how common it is to call the `SET` command, it is interesting to think about things like that to prevent unnecessary work on the server.

And finally we need to update `TtlCommand` and `PttlCommand`

``` ruby
# pttl_command.rb
def call
  if @args.length != 1
    RESPError.new("ERR wrong number of arguments for 'PTTL' command")
  else
    key = @args[0]
    ExpireHelper.check_if_expired(@data_store, @expires, key)
    key_exists = @data_store.include? key
    value = if key_exists
              ttl = @expires[key]
              if ttl
                (ttl - (Time.now.to_f * 1000)).round
              else
                -1
              end
            else
              -2
            end
    RESPInteger.new(value)
  end
end

# ttl_command.rb
def call
  if @args.length != 1
    RESPError.new("ERR wrong number of arguments for 'TTL' command")
  else
    pttl_command = PttlCommand.new(@data_store, @expires, @args)
    result = pttl_command.call.to_i
    if result > 0
      RESPInteger.new((result / 1000.0).round)
    else
      RESPInteger.new(result)
    end
  end
end
```
_listing 5.13 Updated response in PttlCommand & TtlCommand_


### Case insensitivity

It is not explicitly mentioned in the RESP v2 documentation, but Redis treats commands and options as case insensitive. The following examples are all valid: `get 1`, `GeT 1`, `set key value EX 1 nx`.

In order to apply the same handling logic, we changed the keys in the `COMMANDS` constant to be lower case, and we always lower case the client input when attempting to find a handler for the command:

``` ruby
# server.rb
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
_listing 5.14 Updates for case insensitivity in BYORedis::Server_

We also need to update the `BYORedis::SetCommand` class to handle options regardless of the case chosen by clients:

``` ruby
# set_command.rb
# ...
OPTIONS = {
  'ex' => CommandOptionWithValue.new(
    'expire',
    ->(value) { validate_integer(value) * 1000 },
  ),
  'px' => CommandOptionWithValue.new(
    'expire',
    ->(value) { validate_integer(value) },
  ),
  'keepttl' => CommandOption.new('expire'),
  'nx' => CommandOption.new('presence'),
  'xx' => CommandOption.new('presence'),
}
#...
def parse_options
  while @args.any?
    option = @args.shift
    option_detail = OPTIONS[option.downcase]
    # ...
  end
end
#...
```
_listing 5.15 Updates for case insensitivity in SetCommand_

### The `COMMAND` command

In order to implement `COMMAND`, we added a describe method to each of the `*Command` classes, so that the `CommandCommand` class can iterate over all these classes and call `.describe` on them, and then serialize the result to a RESP array:

``` ruby
# command_command.rb
module BYORedis
  class CommandCommand

    def initialize(_data_store, _expires, _args)
    end

    def call
      RESPArray.new(Server::COMMANDS.map { |_, command_class| command_class.describe } )
    end

    def self.describe
      [
        'command',
        -1, # arity
        # command flags
        [ 'random', 'loading', 'stale' ].map { |s| RESPSimpleString.new(s) },
        0, # position of first key in argument list
        0, # position of last key in argument list
        0, # step count for locating repeating keys
        # acl categories: https://github.com/antirez/redis/blob/6.0/src/server.c#L161-L166
        [ '@slow', '@connection' ].map { |s| RESPSimpleString.new(s) },
      ]
    end
  end
end
```
_listing 5.16 The new CommandCommand class_

``` ruby
# get_command.rb

def self.describe
  [
    'get',
    2, # arity
    # command flags
    [ 'readonly', 'fast' ].map { |s| RESPSimpleString.new(s) },
    1, # position of first key in argument list
    1, # position of last key in argument list
    1, # step count for locating repeating keys
    # acl categories: https://github.com/antirez/redis/blob/6.0/src/server.c#L161-L166
    [ '@read', '@string', '@fast' ].map { |s| RESPSimpleString.new(s) },
  ]
end

# pttl_command.rb

def self.describe
  [
    'pttl',
    2, # arity
    # command flags
    [ 'readonly', 'random', 'fast' ].map { |s| RESPSimpleString.new(s) },
    1, # position of first key in argument list
    1, # position of last key in argument list
    1, # step count for locating repeating keys
    # acl categories: https://github.com/antirez/redis/blob/6.0/src/server.c#L161-L166
    [ '@keyspace', '@read', '@fast' ].map { |s| RESPSimpleString.new(s) },
  ]
end

# set_command.rb

def self.describe
  [
    'set',
    -3, # arity
    # command flags
    [ 'write', 'denyoom' ].map { |s| RESPSimpleString.new(s) },
    1, # position of first key in argument list
    1, # position of last key in argument list
    1, # step count for locating repeating keys
    # acl categories: https://github.com/antirez/redis/blob/6.0/src/server.c#L161-L166
    [ '@write', '@string', '@slow' ].map { |s| RESPSimpleString.new(s) },
  ]
end

# ttl_command.rb

def self.describe
  [
    'ttl',
    2, # arity
    # command flags
    [ 'readonly', 'random', 'fast' ].map { |s| RESPSimpleString.new(s) },
    1, # position of first key in argument list
    1, # position of last key in argument list
    1, # step count for locating repeating keys
    # acl categories: https://github.com/antirez/redis/blob/6.0/src/server.c#L161-L166
    [ '@keyspace', '@read', '@fast' ].map { |s| RESPSimpleString.new(s) },
  ]
end
```
_listing 5.17 Updates for the COMMAND command in SetCommand, GetCommand, TtlCommand & PttlCommand_


### test.rb & test_helper.rb

Testing the `BYORedis::Server` class is becoming more and more complicated, in order to keep things clean, I moved a lot of the helper method to the `test_helper.rb` file, so that `test.rb` only contains the actual tests.

The `assert_command_results` helper has been updated to handle the RESP format. For the sake of simplicity, it assumes that the data is not serialized and does that for you. This allows us to write simpler assertions such as:

``` ruby
assert_command_results [
  [ 'SET 1 3 NX EX 1', '+OK' ],
  [ 'GET 1', '3' ],
  [ 'SET 1 3 XX keepttl', '+OK' ],
]
```

and the `assert_command_results` will serialize the commands as RESP Arrays for us.

I also added a new assertion helper, `assert_multipart_command_results`. It allows a little bit more flexibility around expectations for commands sent through multiple `write` calls. Instead of being a single command like in `assert_command_results`, the first element of the pair is itself an array of strings, each of them representing a sequence of characters that will be sent to the server. This is handy to test pipelining as well as edge cases with regard to RESP.

``` ruby
# test_helper.rb
# The arguments in an array of array of the form
# [
#   [ [ "COMMAND-PART-I", "COMMAND-PART-II", ... ], "EXPECTED_RESULT" ],
#   ...
# ]
def assert_multipart_command_results(multipart_command_result_pairs)
  with_server do |server_socket|
    multipart_command_result_pairs.each do |command, expected_result|
      command.each do |command_part|
        server_socket.write command_part
        # Sleep for one milliseconds to give a chance to the server to read
        # the first partial command
        sleep 0.001
      end

      response = read_response(server_socket)

      if response.length < expected_result.length
        # If the response we got is shorter, maybe we need to give the server a bit more time
        # to finish processing everything we wrote, so give it another shot
        sleep 0.1
        response += read_response(server_socket)
      end

      assert_response(expected_result, response)
    end
  end
end

def assert_command_results(command_result_pairs)
  with_server do |server_socket|
    command_result_pairs.each do |command, expected_result|
      if command.is_a?(String) && command.start_with?('sleep')
        sleep command.split[1].to_f
        next
      end
      command_string = if command.start_with?('*')
                         command
                       else
                         BYORedis::RESPArray.new(command.split).serialize
                       end
      server_socket.write command_string

      response = read_response(server_socket)

      assert_response(expected_result, response)
    end
  end
end

def assert_response(expected_result, response)
  assertion_match = expected_result&.match(/(\d+)\+\/-(\d+)/)
  if assertion_match
    response_match = response.match(/\A:(\d+)\r\n\z/)
    assert response_match[0]
    assert_in_delta assertion_match[1].to_i, response_match[1].to_i, assertion_match[2].to_i
  else
    if expected_result && !%w(+ - : $ *).include?(expected_result[0])
      # Convert to a Bulk String unless it is a Simple String (starts with a +)
      # or an error (starts with -)
      expected_result = BYORedis::RESPBulkString.new(expected_result).serialize
    end

    if expected_result && !expected_result.end_with?("\r\n")
      expected_result += "\r\n"
    end

    if expected_result.nil?
      assert_nil response
    else
      assert_equal expected_result, response
    end
  end
end

def read_response(server_socket)
  response = ''
  loop do
    select_res = IO.select([ server_socket ], [], [], 0.1)
    last_response = server_socket.read_nonblock(1024, exception: false)
    if last_response == :wait_readable || last_response.nil? || select_res.nil?
      response = nil
      break
    else
      response += last_response
      break if response.length < 1024
    end
  end
  response&.force_encoding('utf-8')
rescue Errno::ECONNRESET
  response&.force_encoding('utf-8')
end

def to_query(*command_parts)
  [ BYORedis::RESPArray.new(command_parts).serialize ]
end
```
_listing 5.18 The new test helpers in test_helper.rb_


## Conclusion

We can now use redis-cli, with `redis-cli -p 2000` to interact with our redis server:

``` bash
> redis-cli -p 2000
127.0.0.1:2000> COMMAND
1) 1) "command"
   2) (integer) -1
   3) 1) random
      2) loading
      3) stale
   4) (integer) 0
   5) (integer) 0
   6) (integer) 0
   7) 1) @slow
      2) @connection
2) 1) "get"
   2) (integer) 2
   3) 1) readonly
      2) fast
   4) (integer) 1
   5) (integer) 1
   6) (integer) 1
   7) 1) @read
      2) @string
      3) @fast
3) 1) "set"
   2) (integer) -3
   3) 1) write
      2) denyoom
   4) (integer) 1
   5) (integer) 1
   6) (integer) 1
   7) 1) @write
      2) @string
      3) @slow
4) 1) "ttl"
   2) (integer) 2
   3) 1) readonly
      2) random
      3) fast
   4) (integer) 1
   5) (integer) 1
   6) (integer) 1
   7) 1) @keyspace
      2) @read
      3) @fast
5) 1) "pttl"
   2) (integer) 2
   3) 1) readonly
      2) random
      3) fast
   4) (integer) 1
   5) (integer) 1
   6) (integer) 1
   7) 1) @keyspace
      2) @read
      3) @fast
127.0.0.1:2000> GET a-key
(nil)
127.0.0.1:2000> SET name pierre
OK
127.0.0.1:2000> GET name
"pierre"
127.0.0.1:2000> SET last-name J EX 10
OK
127.0.0.1:2000> TTL last-name
(integer) 6
127.0.0.1:2000> PTTL last-name
(integer) 5016
127.0.0.1:2000> PTTL last-name
(integer) 2432
127.0.0.1:2000> DEL name
(error) ERR unknown command `DEL`, with args beginning with: `name`,
```

All the commands we already implemented work as expected and non implemented commands such as `DEL` return an unknown command error. So far so good!

In the next chapter we'll write our own Hashing algorithm and ban the use of the `Hash` class in our code.

### Code

As usual, the code [is available on GitHub](https://github.com/pjambet/redis-in-ruby/tree/master/code/chapter-5).

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
[wikipedia-cjk]:https://en.wikipedia.org/wiki/CJK_characters
[redis-doc-lrange]:https://redis.io/commands/lrange
[chapter-7]:/post/chapter-7
[redis-doc-cluster]:https://redis.io/topics/cluster-tutorial
[redis-gem]:https://github.com/redis/redis-rb
