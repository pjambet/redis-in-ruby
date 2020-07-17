---
title: "Chapter 4 - Adding the missing options to SET"
date: 2020-07-23T10:27:27-04:00
lastmod: 2020-07-23T10:27:27-04:00
draft: true
keywords: []
description: "In this chapter we add the missing features to the SET command we implemented in chapter 2, EX, PX, NX, XX & KEEPTTL"
comment: false
---

## What we'll cover

We implemented a simplified version of `SET` in [Chapter 2][chapter-2], in this chapter, we will complete the command, and implement all [its options][redis-doc-set]. Note that we're still not following the [Redis Protocol][redis-protocol], we will do that in a later chapter. Doing so will require some significant refactoring,

## Completing the SET command

The [`SET`][redis-doc-set] commands accepts the following options:


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

[`TTL`][redis-doc-ttl] returned 8 in both cases, because it took me about 2s to type the `TTL` command, and by that time about 8s where left, of the initial 10.

### The EX & PX options

We will first add support for the EX & PX options, both are followed by one argument, an integer for the number of the seconds and milliseconds specifying how long the key will be readable for. Once the duration has elapsed, the key is deleted, and calling `GET` would return `(nil)` as if it was never set.

When a key is set with either of these two options, or through the `SETEX` & `PSETEX` commands, but we are ignoring these for the sake of simplicity, Redis adds the key to different dictionary, `db->expires`. This dictionary is dedicated to storing keys with a TTL, the key is the same key and the value is the timestamp of the expiration, in milliseconds.

Redis uses two approaches, in two different places, to delete keys with a TTL. The first one is the lazy approach, when reading a key, it checks if it has a ttl, if it does and it is in the past, it deletes the key and does not proceed with the read.

The other one is a more proactive approach, redis periodically scans the list of keys with a ttl value and deletes the expired one. This action, performed in `serverCron` is part of the [event loop][redis-doc-event-loop]. The event loop is defined in `ae.c` and starts in the `aeMain` function, it continuously executes the `aeProcessEvents` function in a while loop, until the `stop` flag is set to `1`, which essentially never happens when the server is running under normal circumstances. The `aeStop` function is the only function doing this and it is only used in `redis-benchmark.c` & `example-ae.c`.

`aeProcessEvents` is a fairly big function, it would be hard to summarize it all here, but it first uses the `aeApiPoll` function, which is what we covered in the previous chapter. It processes the events coming from the poll, if any and then calls `processTimeEvents`.

Redis maintains a list of time events, as described in the event loop documentation page, for periodic task. When the server is initialized, one time event is created, for the function `serverCron`. This function is responsible for a lot of things. This how it is described in the [source code][redis-source-server-cron]:

> - Active expired keys collection (it is also performed in a lazy way on lookup).
> - Software watchdog.
> - Update some statistic.
> - Incremental rehashing of the DBs hash tables.
> - Triggering BGSAVE / AOF rewrite, and handling of terminated children.
> - Clients timeout of different kinds.
> - Replication reconnection.
> - Many more...

We're only interested in the first one for now, the active expiration of keys. Redis runs on a single thread, which essentially continuously runs the event loop described above. This means that each operation performed in the event loop effectively blocks the ones waiting to be processed. Redis tries to optimize for this by making all the operations as fast as possible.

`serverCron` is first added to the time events with a time set to 1ms in the future. The return value of function executed as a time event dictates if it is removed from the time event queue or if it is rescheduled in the future. `serverCron` returns a value based on the frequency set as config. By default 100ms. That means that it won't run more than 10 times per second.

This is one of the reasons the documentation provides the time complexity for each commands. Most commands are O(1), and commands like [`KEYS`][redis-doc-keys], with an O(n) complexity are not recommended in a production environment.

If Redis were to scan all the keys in the expires dictionary on every iteration of the event loop it would be an O(n) operation. Put differently, as you add keys with a TTL, you would slow down the process of active expiration. To prevent this, Redis only scans the expires up to certain amount. The `activeExpireCycle` contains a lot of optimizations that we will not explore for now. [EXPLAIN WHY].

I think that it's worth stopping for a second to recognize the benefits of having two types of expiration. The lazy approach gets the job done as it guarantees that no expired keys will ever be returned, but if a key is set with a ttl and never read again it would still unnecessarily sit in memory. The incremental active approach solves this problem, while still being optimized for speed and does not pause the server to clean all the keys.

#### Let's write some code!

We are making the following changes to the server:

- Add our own, simplified event loop, including support for time events
- Accepts options for the SET command, and support PX & EX
- Delete expired keys on read


---

Note: See `db.c`, functions:

1. `server.c` defines all the commands in `redisCommand`:   https://github.com/antirez/redis/blob/6.0/src/server.c#L201-L203
2. `t_string.c` defines the handler in `setCommand`: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L97-L147
3  `t_string.c` defines a more specific handlers after options are parsed: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L71-L79 & https://github.com/antirez/redis/blob/6.0/src/t_string.c#L89
4. `db.c` defines the `setExpire` function: https://github.com/antirez/redis/blob/6.0/src/db.c#L1190-L1206

Keys are deleted on reads

1. `server.c` defines the handler for `GET`: https://github.com/antirez/redis/blob/6.0/src/server.c#L187-L189
2. `t_string.c` defines the handler for `getCommand`: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L179-L181 & the generic one: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L164-L177
3. `db.c` defines `lookupKeyReadOrReply`: https://github.com/antirez/redis/blob/6.0/src/db.c#L163-L167
4. `db.c` defines `lookupKeyRead`  https://github.com/antirez/redis/blob/6.0/src/db.c#L143-L147 as well as `lookupKeyReadWithFlags`: https://github.com/antirez/redis/blob/6.0/src/db.c#L149-L157
5. `db.c` defines `expireIfNeeded`: https://github.com/antirez/redis/blob/6.0/src/db.c#L1285-L1326

In `expire.c`: `activeExpireCycleTryExpire`: https://github.com/antirez/redis/blob/6.0/src/expire.c#L35-L74

In `serverCron` and `databasesCron`, called from `arCreateTimeEvent`

On init, `aeCreateTimeEvent` is called, 1ms in the future. On each ae loop, we look at time events, `processTimeEvents`.

It loops through, starting at `timeEventHead`
If timeEvent func returns `AE_NOMORE`, `-1`, event is removed on next iteration, otherwise, it's bumped by `restval` ms. `serverCron` returns `1000/server.hz`.

in `evict.c`, in `freeMemoryIfNeeded`, keys might get deleted.

---

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
[redis-doc-ttl]:http://redis.io/commands/ttl
[redis-doc-event-loop]:https://redis.io/topics/internals-rediseventlib
[redis-docs-keys]:https://redis.io/commands/keys
