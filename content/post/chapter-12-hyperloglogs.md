---
title: "Chapter 12 Adding Hyperloglogs Commands"
date: 2020-10-17T18:17:49-04:00
lastmod: 2020-10-17T18:17:49-04:00
draft: true
comment: false
keywords: []
summary: ""
---

<!--more-->


PFADD
PFCOUNT
PFMERGE

Example with 1,000,000 values, 1 to 1,000,000:

``` bash
127.0.0.1:6379> pfcount hll
(integer) 1009972
127.0.0.1:6379> scard s
(integer) 1000000
127.0.0.1:6379> debug object hll
Value at:0x7f88e9585080 refcount:1 encoding:raw serializedlength:10587 lru:11279204 lru_seconds_idle:18
127.0.0.1:6379> debug object s
Value at:0x7f88e5f249f0 refcount:1 encoding:hashtable serializedlength:4934341 lru:11279207 lru_seconds_idle:16
```

Off by 0.9972%

``` bash
127.0.0.1:6379> debug object s2
Value at:0x7f88ebb70330 refcount:1 encoding:hashtable serializedlength:2874 lru:11279316 lru_seconds_idle:9
127.0.0.1:6379> debug object hll2
Value at:0x7f88ebc5ad90 refcount:1 encoding:raw serializedlength:1838 lru:11279321 lru_seconds_idle:6
127.0.0.1:6379> scard s2
(integer) 1000
127.0.0.1:6379> pfcount hll2
(integer) 1001
```

Off by 0.1%
