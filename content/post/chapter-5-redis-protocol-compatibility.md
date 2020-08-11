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

## The Redis Protocol V2 (RESP)

[RESP v2][resp-spec] has been the protocol used by Redis since version 2.0, to quote the documentation:

> 1.2 already supported it, but Redis 2.0 was the first version to talk only this protocol)

### RESP3

This chapter will focus on RESP v2, but a new version, called RESP3 has been released in 2018. RESP3 improves many different aspects of RESP v2, such as adding new types for maps — often called dictionary, and a lot more. The spec is [on GitHub][resp3-spec] and explains in details [the background behind it][resp3-spec-background].
RESP3 is supported as of Redis 6.0, as indicated in [the release notes][release-notes-6-0]:

> Redis now supports a new protocol called RESP3, which returns more semantical replies: new clients using this protocol can understand just from the reply what type to return to the calling program.

The [`HELLO`][redis-doc-hello] command can be used to switch the connection to a different protocol. As we can see below, only two versions are currently supported, 2 & 3. We can also see the new map type in action, `hello 2` returned an array with 14 items, representing 7 key/value pairs, whereas `hello 3` leveraged the new map type to return a map with 7 key/value pairs.

```
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

```
127.0.0.1:6379> hello 3
1# "server" => "redis"
2# "version" => "6.0.6"
3# "proto" => (integer) 3
4# "id" => (integer) 6
5# "mode" => "standalone"
6# "role" => "master"
7# "modules" => (empty array)
```

```
127.0.0.1:6379> hello 1
(error) NOPROTO unsupported protocol version
```

```
127.0.0.1:6379> hello 4
(error) NOPROTO unsupported protocol version
```

Adding support for the `HELLO` command and both protocols, RESP v2 and RESP3 might be added later on but it's not currently on the initial roadmap of this online book.

### Back to RESP v2

The [official specification][redis-spec] goes into details about the protocol and is still reasonably short and approachable, so feel free to read it, but here are the main elements that will drive the changes to our `RedisServer` class:


#### Inline Protocol

Redis also supports a convenient inline mode


#### The 5 data types

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

##### Simple Strings

A simple string cannot contain a new line. One its main use case is to return `OK` back to the client. The full format of a simple string is "A `+` character, followed directly by the content of the string, followed by a carriage return (often written as `CR` or `\r`) and a line feed (often written as `LF` or `\n`).

This is why Simple Strings cannot contain multiples lines, a newline would create confusion given that it is also use a delimiter.

The common `"OK"` string, returned by the `SET` command upon success is therefore serialized as `+OK\r\n`.

`redis-cli` does the work of detecting the type of the response and only shows us the actual string, `OK`, as we can see in the example below:

```
127.0.0.1:6379> SET 1 2
OK
```

Using `nc`, we can see what the full response sent back from Redis is:

```
> nc -v localhost 6379
SET 1 2
+OK

```

`nc` does not no anything unusual with invisible characters such as `CR` & `LF`, so it is hard to see them, beside the newline printed after `+OK`. The `hexdump` command is useful here, it allows us to see all the bytes:

```
echo "SET 1 2" | nc -v localhost 6379 | hexdump -C
# ...
00000000  2b 4f 4b 0d 0a                                    |+OK..|
00000005
```

`2b` is the hex notation of 43 (`'2b'.to_i(16)` in `irb`), and 43 maps to `+` in the [ASCII table][ascii-table]. `4f` is the equivalent of 79, and the capital letter `O`, `4b`, the number 75 and the capital letter `K`.

`0d` is the equivalent of the number 13, and the Carriage Return character (CR), and finally, `0a` is 10, the Line Feed character (LF).

Redis follows the Redis Protocol, that's a good start!

##### Errors

Errors are very similar to simple strings, they also cannot contain new line characters. The main difference is that clients should treat them as errors instead of successful results. In languages with exceptions, a client library might decide to throw an exception when receiving an error from Redis. This is what [the official ruby library][redis-ruby-client] does.

Similarly to simple strings, an errors ends with a carriage return and a line feed, let's see it in action:

```
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

##### Integers

Integers have a similar representation to simple strings and errors. The actual integer comes after the `:` character and is followed by the `CR` & `LF` characters.

An example of integer reply is with the `TTL` and `PTTL` commands

The key `key-with-tll` was set with the command: `SET key-with-ttl value EX 1000`.

```
> echo "TTL key-with-ttl" | nc -c -v localhost 6379 | hexdump -C
# ...
00000000  3a 39 38 38 0d 0a                                 |:988..|
00000006
```

The key `not-a-key` does not exist.

```
> echo "TTL not-a-key" | nc -c -v localhost 6379 | hexdump -C
# ...
00000000  3a 2d 32 0d 0a                                    |:-2..|
00000005
```

The key `key-without-tll` was set with the command: `SET key-without-ttl value`.

```
> echo "TTL key-without-ttl" | nc -c -v localhost 6379 | hexdump -C
# ...
00000000  3a 2d 31 0d 0a                                    |:-1..|
00000005
```

All of these responses start with the `3a` byte, which is equivalent to 58, aka `:`. In the two cases where the response is a negative value, `-2` for a non existent key and `-1` for an existing key without a ttl the next byte is `2d`, equivalent to 45, aka `-`.

The rest of the data, before the `0d` & `0a` bytes, is the actual integer data, in ASCII format, `31` is the hex equivalent to 49, which is the character `1`, 32 is the hex equivalent to 50, which is the character `2`. `39` & `38` are respectively the hex equivalent to 57 & 56, the characters `9` & `8`.

A ruby client parsing this data would extract the string between `:` and `\r\n` and call `to_i` on it: `'988'.to_i == 988`.

##### Bulk Strings

In order to work for any strings, bulk strings need to first declare their length, and only then the actual data. This lets the receiver know how many bytes to expect, instead of reading anything until it finds `CRLF` the way it does for a simple string.

The length of the string is sent directly after the dollar sign, and is delimited by `CRLF`, the following is the actual string data, and another `CRLF` to end the string.

Interestingly, it seems like Redis does not care that much about the final `CRLF`, as long as it finds two characters there, it assumes it's the end of the bulk string and tries to process what comes after:

The following first sends the command `GET a` to redis as a multi bulk string, followed by the non existent `NOT A COMMAND` in the inline format. The response first contains the `-1` integer, followed by the error.

```
irb(main):027:0> socket.write("*2\r\n$3\r\nGET\r\n$1\r\nabcNOT A COMMAND\r\n")
=> 35
irb(main):028:0> socket.read_nonblock(1024, exception: false)
=> "$-1\r\n-ERR unknown command `NOT`, with args beginning with: `A`, `COMMAND`, \r\n"
```

The following is handled identically by Redis, despite the fact the `a` bulk string is not terminated by `CRLF`. We can see that Redis ignored the `b` and `c` characters and proceeded with the following command, the non existent `NOT A COMMAND`. I am assuming that the code in charge of reading input first reads the length, then grabs that many bytes and jumps by two characters, regardless of what these characters are.

```
irb(main):029:0> socket.write("*2\r\n$3\r\nGET\r\n$1\r\na\r\nNOT A COMMAND\r\n")
=> 35
irb(main):030:0> socket.read_nonblock(1024, exception: false)
=> "$-1\r\n-ERR unknown command `NOT`, with args beginning with: `A`, `COMMAND`, \r\n"

```

##### Arrays

a

#### Requests & Responses

We have not yet added support for pipelining and pub/sub, so we will ignore their impact on our implementation of the Redis Protocol for now. Future chapters will add support for these two supports and will follow the RESP specification.


## Making RedisServer speak RESP

### Closing clients after a read if they closed the connection

### Parsing Input

### Case insensitivity

both commands and options

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
