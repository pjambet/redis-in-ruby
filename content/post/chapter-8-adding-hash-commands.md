---
title: "Chapter 8 Adding Hash Commands"
date: 2020-10-02T18:54:07-04:00
lastmod: 2020-10-02T18:54:07-04:00
draft: true
comment: false
keywords: []
summary:  "In this chapter we add support for a new data type, Lists. We implement all the commands related to lists, such as HSET, HGET & HGETALL"
---

## What we'll cover


15 commands

Skip HMSET - deprecated
Skip HScan - too complicated

## How does Redis do it

## Adding Hash Commands

### Creating a Hash with HSET & HSETNX

HMSET is deprecated

### Reading Hash values with HGET, HMGET & HGETALL

### Incrementing numeric values with HINCRBY & HINCRBYFLOAT

### Utility commands

**HDEL**

**HEXISTS**

**HKEYS**

**HVALS**

**HLEN**

**HSTRLEN**

**HSCAN**

Not implementing because the *SCAN commands are too complicated for this chapter.

## Conclusion


[redis-doc-hashes]:https://redis.io/commands#hash
