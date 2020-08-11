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

#### The 5 data types

RESP v2 defines five data types:

- Simple Strings
- Errors
- Integers
- Bulk Strings
- Arrays

##### Simple Strings

s

##### Errors

e

##### Integers

i

##### Bulk Strings

b

##### Arrays

a

#### Requests & Responses

We have not yet added support for pipelining and pub/sub, so we will ignore their impact on our implementation of the Redis Protocol for now. Future chapters will add support for these two supports and will follow the RESP specification.

## Making RedisServer speak RESP


### Parsing Input

### Case insensitivity

both commands and options

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
