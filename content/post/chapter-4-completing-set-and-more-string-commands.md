---
title: "Chapter 4 - Completing Set and more string commands"
date: 2020-07-23T10:27:27-04:00
lastmod: 2020-07-23T10:27:27-04:00
draft: true
keywords: []
description: ""
---

## What we'll cover

We implemented a simplified version of `SET` in [Chapter 2][chapter-2], in this chapter, we will complete the command, and implement all [its options][redis-doc-set]. Note that we're still not following the [Redis Protocol][redis-protocol], we will do that in a later chapter. Doing so will require some significant refactoring,

## Completing the SET command

The `SET` commands accepts the following options:


- EX seconds -- Set the specified expire time, in seconds.
- PX milliseconds -- Set the specified expire time, in milliseconds.
- NX -- Only set the key if it does not already exist.
- XX -- Only set the key if it already exist.
- KEEPTTL -- Retain the time to live associated with the key.

As noted in the documentation, there is some overlap with some of the options above and the following commands: [SETNX][redis-doc-setnx], [SETEX][redis-doc-setex], [PSETEX][redis-doc-psetex]. As of this writing these three commands are not officially deprecated, but the documentation mentions that they might soon. Given that we can access the same features through the `NX`, `EX` & `PX` options respectively, we will not implement these three commands.

`SET a-key a-value EX 10` is equivalent to `SETEX a-key 10 a-value`, which you can demonstrate in `redis-cli` session:

```
127.0.0.1:6379> SET a-key a-value EX 10
OK
127.0.0.1:6379> TTL "a-key"
(integer) 8
127.0.0.1:6379> SETEX a-key 10 a-value
OK
127.0.0.1:6379> TTL "a-key"
(integer) 8
```

`TTL` returned 8 in both cases, because it took me about 2s to type the `TTL` command, and by that time about 8s where left, of the initial 10.

- INCR / INCRBY
- DECR / DECRBY
- MGET
- MSET

Reminder:
[Redis Protocol](https://redis.io/topics/protocol#resp-bulk-strings)


[chapter-2]:/post/chapter-2-respond-to-get-and-set/
[redis-protocol]:https://redis.io/topics/protocol
[redis-doc-set]:https://redis.io/commands/set
[redis-doc-setnx]:https://redis.io/commands/setnx
[redis-doc-setex]:https://redis.io/commands/setex
[redis-doc-psetex]:https://redis.io/commands/psetex
