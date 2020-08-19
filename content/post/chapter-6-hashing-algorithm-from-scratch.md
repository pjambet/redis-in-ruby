---
title: "Chapter 6 Hashing Algorithm From Scratch"
date: 2020-08-18T09:26:53-04:00
lastmod: 2020-08-18T09:26:53-04:00
draft: true
comment: false
keywords: []
summary: "In this chapter we will write our own hashing algorithm. This will allow to remove uses of the Ruby Hash class and use our, built from scracth, Dict class."
---

## What we'll cover

...

### Notes

- dbDictType used as argument to dictCreate in server.c (initServer)
- Uses dictSdsHash, dictSdsKeyCompare, dictSdsKeyDestructor, dictObjectDestructor
- dict struct:
  - dictType
  - privdata
  - ht (2 of them)
  - rehashidx
  - iterators

- dictht:
  - table (dictentry)
  - size
  - sizemask
  - used

- dictentry:
  - key
  - v (val, u64, s64, d)
  - next

- dictAdd
  - calls dictAddRaw
  - calls dictSetVal (a macro, sets the val to the entry)

- dictAddRaw (dict.c)
  - get the index with _dictKeyIndex and dictHashKey(d, key)
  - prepends the entry to ht->table with the index from the previous step, itself is a list of entries
  - calls dictSetKey (a macro, sets the key to entry)

- setGenericCommand (takes the key as argument, and the val)
  - called from ...

- genericSetKey (db.c)
  - calls dbAdd

- dbAdd (db.c)
  - calls dictAdd (dict.c)

- dictAdd (above)

- tryResizeHashTables is called from databasesCron
  - itself called from serverCron

- dictHashKey is a macro (dict.h)
  - delegates to (d)->type->hashFunction (dictSdsHash here)

- dictSdsHash (in server.c)
  - calls dictGenHashFunction (in dict.c)

- dictGenHashFunction
  - calls siphash

- dictSetHashFunctionSeed
  - called in main (server.c) with hashseed, result of getRandomBytes

- _dictKeyIndex (in dict.c)
