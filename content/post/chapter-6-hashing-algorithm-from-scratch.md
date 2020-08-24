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

So far we've been using the Ruby [`Hash`][ruby-doc-hash] class as the main storage mechanism for the key/value pairs received through the `SET` command. We also use it for the parallel dictionary necessary to implement the TTL related options of the `SET` commands. We store the expiration timestamp of keys with TTLs, which allows us to if know when a key is expired, or not.

Redis is written in C, which does not provide a hash collection, often called dictionary. In C, the only collection you get out of the box is [arrays][c-doc-array].

Redis implements its own dictionary collection, in [`dict.c`][github-link-dict]. Because the dictionary data structure is so central to how redis functions, we will ban the use of the ruby `Hash` class and replace its use with a `Dict` class we'll create in this chapter.

We're not adding any new features in this chapter, we're rewriting a key part of the system with lower level elements. Given that Redis' dict data structure relies on arrays, we will still use the ruby [`Array` class][ruby-doc-array]. We could have reimplemented the `Array` class, and you'll find an example in [Appendix A](#appendix-a-array-from-scratch-in-ruby), but arrays in C are not specific to Redis. On the other hand, the structure defined in [`dict.c`][github-link-dict] is!

Let's get to it.

## Maps, Dictionaries, Associative Arrays

This chapter cover hash tables, which is one way of implementing data structure commonly called Maps, Dictionaries, Associate Arrays.

The basic definition of such data structure is one that holds zero or more key/value pairs, where a key cannot appear more than once.

A key operation (pun intended!) of a map is the ability to retrieve a value given a key. The return value is either empty if no element with such keys exist in the map, or the value mapped to the key if it exists.

Some definitions also include the ability to add, update or delete key/value pairs, which is not implemented in immutable versions. The immutable versions will implement similar operations returning a new structure instead of modifying it.

There are multiple ways of implementing a data structure exposing these operations. One could be to use an array where each element is a key value pair:

``` ruby
def add(map, key, value)
  map.each do |pair_key, pair_value|
    return nil if key == pair_key
  end
  pair = [key, value]
  map << pair
  pair
end

def lookup(map, key)
  map.each do |pair_key, pair_value|
    return pair_value if key == pair_key
  end
  return
end

map = []
add(map, "key-1", "value-1") # => ["key-1", "value-1"]
add(map, "key-2", "value-2") # => ["key-2", "value-2"]
add(map, "key-2", "value-2") # => nil

lookup(map, "key-1") # => "value-1"
lookup(map, "key-2") # => "value-2"
lookup(map, "key-3") # => nil
```

This approach works from an API standpoint, but it would show performance issues as we keep adding elements to the array. Because we must prevent duplicated keys, we need to iterate through the whole array every time we attempt to add a new pair. If we find the key in the array we return nil, to indicate that nothing was inserted.

A lookup might not always require a complete scan of the array, if we're lucky and find the key before the end, but it might, in the worst case scenario.

For Redis, which should be able to handle hundreds of thousand of keys, even millions, these performance issues are not acceptable.

One common implementation that addresses this performance issues is a hash table.

https://en.wikipedia.org/wiki/Associative_array

## Hash Tables

Hash tables are available in many programming languages as part of their standard libraries. Python has dict, Java has HashMap, scala has Map, Elixir has Maps, Rust has HashMap, Ruby's `Hash` class is a hash table implementation too. You get it, they're almost everywhere.

Hash tables can be implemented in different ways, [the wikipedia article][wikipedia-hash-table] shows a few different examples. The one we'll explore in this chapter uses a collision resolution called separate chaining. But why do we need collision resolution? To answer this we first need to look at the central element of a hash table, its hash function.

A [hash function][wikipedia-hash-function] must obey a few properties, one of the most important ones being determinism, in other words, identical inputs should result in identical outputs. To explain why, let's look at how the hashing function is used through a pseudo code implementation of a hash table:

```
function new_node(key, value, next_node)
    return Node(key, value, next_node)

function create_hash_table()
    table = allocate_array_of_arbitrary_initial_size()
    return table

function add_key_value_pair(table, key, value)
    hash = hash_function(key)
    index = hash % table.size
    if table[index] == null
        table[index] = new_node(key, value, null)
    else
        existing_node = table[index]
        table[index] = new_node(key, value, existing_node)

function lookup_key(table, key)
    hash = hash_function(key)
    index = hash % table.size
    if table[index] == null
        return null
    else
        node = table[index]
        while node != null
            if node.key == key
                return key
            else
                node = node.next_node

        return null
```

The previous pseudo code section shows four functions, the first one is `new_node`. This function acts as the entry point of a linked list. A node contains a key, a value, and a next node value. If the next node value is null, the element is the last one in the list.

Appending an element to such list is done by first creating a single node list and then a second one, with the `next_node` value set to the first one:

```
first_node = new_node(k1, v1, null)
two_node_list = new_node(k2, v2, first_node)
```

In this example `first_node` is a list with a single node, and `two_node_list` is a list with two nodes. The first node is the one with the key `k2` and the value `v2`, its `next_node` value is equal to `first_node`, which has the key `k1` and value `v1`, it does not have a `next_node` value and is the last element of the list.

`create_hash_table` does one thing, it allocates an array of arbitrary size. We purposefully do not define this function here. The size is not really important, as long as it creates a non empty array. The implementation of the allocation is also not really relevant to this example. Most operating systems provide such features, so it's therefore fair to assume that it would use the allocation operations provided by the operating system.

`add_key_value_pair` does more work and let's walk through it, one line at a time. It takes three parameters, the table we want to insert the pair into, the key and the value.
We first call `hash_function` with `key`. We'll dive deeper into what an implementation of `hash_function` looks like later, but for now, let's assume it returns an integer. Because the hash function is unaware of the size of the array, the returned value might be larger than the size.

We use the modulo operation to convert the hash value returned by `hash_function` into a number between 0 and `table.size - 1`. We can now use the result of the modulo operation as an index. That's why we have the `create_hash_table` function, to make sure that table is initialized with empty slots. These slots are often called buckets in hash table documentations.

There is one last case to consider, there might already be one or more items in this bucket. This is called a collision.

Let's illustrate with an example. Let's set the initial size of the array to 4, all the buckets are empty:

```
table = [nil, nil, nil, nil]
```

Let's define a hash function, that returns length of the string input:

```
function hash_function(string)
    return string.length
```

If we first call `add_key_value_pair` with the key `"a"` and the value `"b"`, `hash_function` will return 1, the length of the string `"a"`. `1 % 4` returns 1, so we add the pair at index 1:

```
table = [nil, Node("a", "b", nil), nil, nil]
```

Let's call `add_key_value_pair` with the pair `"cd"`/`"e"`, the length of `"cd"` is 2, `2 % 4` is 2, we insert the pair at index 2:

```
table = [nil, Node("a", "b", nil), Node("cd", "e", nil), nil]
```

Let's now call `add_key_value_pair` with `"fg"`/`"h"`. The length of `"fg`" is 2, but there's already a pair at index 2. Because we want to keep all pairs we need a solution to resolve this collision. There are different strategies available to us here, and the one we're going to use is called "separate chaining".

The essence of this strategy is that each bucket contains a linked list of values. So in the previous example, we insert the new element at the beginning of the list at index 2. Note that prepending an element to a linked list is an O(1) operation, it takes the same amount of time regardless of the size of the list. This is the list once the operation is completed:

```
table = [nil, Node("a", "b", nil), Node("fg", "h", "Node("cd", "e", nil)), nil]
```

`lookup_key` is very similar to `add_key_value_pair`. We use the same process to find which bucket the key should be in. If the bucket is empty, we return `null`, the key is not present in the dictionary. On the other hand, if the bucket is not empty, we need to look through the list until we find the node with the key argument.

If we don't find any, the key is not present in the dictionary.

---

The `hash_function` we used in the previous works well as an example because of its simplicity but it would not be practical in the real world. To keep hash tables efficient, we want to reduce the number of collisions as much as possible. This is because iterating through the linked list is inefficient, if there are a lot of collisions, it could take a long time to loop through all the items in the bucket.

This is there the [uniformity property][wikipedia-hash-function-uniformity] of a hash function is really important. Uniformity helps reduce the likelihood of collision. In the previous example, if an hypothetical hash function had returned the values 1, 2 & 3, respectively, instead of 1, 2 & 2, there wouldn't have been any conflicts.

Collisions are also related to the size of the underlying array. Regardless of the uniformity of the hash function, if the underlying array has a size n, storing n + 1 items cannot happen without at least one collision.

One approach would be so start by allocating a very large amount of memory, but this can be wasteful, because there could be a lot of memory allocated, but unused. Many hash table implementation have mechanisms to adjust the size as needed, and it turns out that Redis does this too, as we'll see in the next section.

Hash tables need a hashing function. There are multiple hashing functions, with varying trade-offs. Common functions include, md5, sha-1 & sha-256. A hash function takes some input and return a hash value, the important part is that it must always returns the same value if the input is identical.

This last part important, especially as we start thinking about it in terms of implementation details. Two different variables can hold the same value:

``` ruby
str1 = "hello"
str2 = "hello"
p str1.object_id
p str2.object_id
p str1 == str2
```

In turns out that Ruby implements the `hash` method for all objects:

``` ruby
str1 = "hello"
str2 = "hello"
p str1.hash
p str2.hash
```

One approach to use the hash function to implement a hash map is to first hash the given key, and get the hash value back, as a number.
We'll use an array to store the value, created with an arbitrary initial size, let's say 10 for now. If we get the modulo of the hash value, we can use that result as index in the array.

This method means that for every input returning the same value, the same index will be returned. Collisions are possible, so instead of assuming that each empty cell of the array will hold zero or one value, we can instead assume that it will contain a list of value. By default it'll be an empty list and we'll prepend values at they arrive.

Let's look at an example.

## How does Redis do it?

http://blog.wjin.org/posts/redis-internal-data-structure-dictionary.html

Before writing any code and looking at what needs to change once we stop using the `Hash` class, let's first look at how Redis does it. Let's start with the main `struct`, [`dict`][redis-source-dict]:

``` c
typedef struct dict {
    dictType *type;
    void *privdata;
    dictht ht[2];
    long rehashidx; /* rehashing not in progress if rehashidx == -1 */
    unsigned long iterators; /* number of iterators currently running */
} dict;
```

If you're not used to C, don't worry too much about it for now, we're not going to look at pointers and other C specific features, we're mainly interested in the fields that compose a `dict` instance.

``` c
// https://github.com/antirez/redis/blob/6.0/src/dict.h#L58-L65
typedef struct dictType {
    uint64_t (*hashFunction)(const void *key);
    void *(*keyDup)(void *privdata, const void *key);
    void *(*valDup)(void *privdata, const void *obj);
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);
    void (*keyDestructor)(void *privdata, void *key);
    void (*valDestructor)(void *privdata, void *obj);
} dictType;
```

``` c
// https://github.com/antirez/redis/blob/6.0/src/dict.h#L67-L74
/* This is our hash table structure. Every dictionary has two of this as we
 * implement incremental rehashing, for the old to the new table. */
typedef struct dictht {
    dictEntry **table;
    unsigned long size;
    unsigned long sizemask;
    unsigned long used;
} dictht;
```

``` c
// https://github.com/antirez/redis/blob/6.0/src/dict.h#L47-L56
typedef struct dictEntry {
    void *key;
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;
    struct dictEntry *next;
} dictEntry;
```

Interesting functions:

``` c
// https://github.com/antirez/redis/blob/6.0.0/src/dict.h#L151
dict *dictCreate(dictType *type, void *privDataPtr);
// https://github.com/antirez/redis/blob/6.0.0/src/dict.h#L152
int dictExpand(dict *d, unsigned long size);
// https://github.com/antirez/redis/blob/6.0.0/src/dict.h#L153
int dictAdd(dict *d, void *key, void *val);
// https://github.com/antirez/redis/blob/6.0.0/src/dict.h#L161
dictEntry * dictFind(dict *d, const void *key);
// https://github.com/antirez/redis/blob/6.0.0/src/dict.h#L163
int dictResize(dict *d);
// ...
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
```

See notes below ...

[redis-source-dict]:https://github.com/antirez/redis/blob/6.0.0/src/dict.h#L76-L82

## Our own `Dict` class

### a

a

### b

b

### c

c

[ruby-doc-hash]:http://ruby-doc.org/core-2.7.1/Hash.html
[ruby-doc-array]:http://ruby-doc.org/core-2.7.1/Array.html
[c-doc-array]:https://www.tutorialspoint.com/cprogramming/c_arrays.htm
[github-link-dict]:https://github.com/redis/redis/blob/6.0.0/src/dict.c

## Notes

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

- Two similar functions dictResize & dictExpand. Resize calls Expand
  - expand does the actual expand
  - resize figures out the size for you
  - Rehash Just sets up the second ht for rehashing (->rehashidx = 0)

- incrementallyRehash is called in databsesCron (server.c)
  - aaa

- rules for _dictExpandIfNeeded (called for every call to _dictKeyIndex)
  - Resize to 2x the size if used >= size && (can_resize || used/size > 5)
  - Resize will find the right size, so it might shrink it
  - Will resize if bigger than 4 but usage is below 10%

- dictHashKey is a macro (dict.h)
  - delegates to (d)->type->hashFunction (dictSdsHash here)

- dictSdsHash (in server.c)
  - calls dictGenHashFunction (in dict.c)

- dictGenHashFunction
  - calls siphash

- dictSetHashFunctionSeed
  - called in main (server.c) with hashseed, result of getRandomBytes

- _dictKeyIndex (in dict.c)


## Appendix A: Array from scratch in Ruby

See ...
