---
title: "Chapter 6 Building a Hash Table"
date: 2020-08-18T09:26:53-04:00
lastmod: 2020-08-18T09:26:53-04:00
draft: true
comment: false
keywords: []
summary: "In this chapter we will write our own hash table. This will allow to remove uses of the Ruby Hash class and use our, built from scracth, Dict class."
---

## What we'll cover

So far we've been using the Ruby [`Hash`][ruby-doc-hash] class as the main storage mechanism for the key/value pairs received through the `SET` command. We also use it for the secondary dictionary necessary to implement the TTL related options of the `SET` commands. We store the expiration timestamp of keys with TTLs, which allows us to if know when a key is expired or not.

Redis is written in C, which does not provide a collection similar to Ruby's `Hash`. In C, the only collection you get out of the box is [arrays][c-doc-array].

Redis implements its own dictionary collection, in [`dict.c`][github-link-dict]. Because the dictionary data structure is so central to how Redis functions, we will replace the use of the ruby `Hash` class with a `Dict` class we will build from scratch.

We are not adding any new features in this chapter, we're rewriting a key part of the system with lower level elements. Given that Redis' dict data structure relies on arrays, we will still use the ruby [`Array` class][ruby-doc-array]. We could have reimplemented the `Array` class, and you'll find an example in [Appendix A](#appendix-a-array-from-scratch-in-ruby), but arrays in C are not specific to Redis. On the other hand, the structure defined in [`dict.c`][github-link-dict] is.

Let's get to it.

## Maps, Dictionaries, Associative Arrays

This chapter covers hash tables, which is one way of implementing data structure commonly called Maps, Dictionaries or Associate Arrays. From now on I will use the term "Dictionary". Map can be confusing, especially when working with languages providing a `map` function/method, such as Ruby! From my experience the term associate array, while very explicitly, is not as common.

The basic definition of such data structure is one that holds zero or more key/value pairs, where a key cannot appear more than once.

A key operation (pun intended!) of a dictionary is the ability to retrieve a value given a key. The returned value is either empty if no element with such keys exists in the map, or the value mapped to the key if it exists.

Some definitions also include the ability to add, update or delete key/value pairs, which is not provided in immutable versions, where such operations would result in the creation of a new dictionary. The immutable versions will implement similar operations returning a new structure instead of modifying it.

There are multiple ways of implementing a data structure providing these operations. A naive and inefficient version could be to use an array where each element is a key value pair:

``` ruby
def add(map, key, value)
  map.each do |pair|
    pair_key = pair[0]
    # Override the value if the key is already present
    if key == pair_key
      pair[1] = value
      return pair
    end
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
add(map, "key-2", "value-3") # => ["key-2", "value-3"]

lookup(map, "key-1") # => "value-1"
lookup(map, "key-2") # => "value-2"
lookup(map, "key-3") # => nil
```

This approach works from an API standpoint, but it would show performance issues as we keep adding elements to the array. Because we must prevent duplicated keys, we need to iterate through the whole array every time we attempt to add a new pair if the key is not already present.

A lookup might not always require a complete scan of the array, if we're lucky and find the key before the end, but it might, in the worst case scenario.

For Redis, which should be able to handle hundreds of thousand of keys, even millions, these performance issues are not acceptable.

One common implementation that addresses this performance issues is a hash table. Another possible implementation is the tree map, which uses a tree structure to store elements. The Java [`TreeMap`][java-doc-tree-map] uses a Red-Black tree. One benefits of a tree map compared to a hash map is that it stores elements in order, whereas a hash map does not.

In the next section we will learn how a hash tables implements these operations in a more time efficient manner.

https://en.wikipedia.org/wiki/Associative_array

## Hash Tables

Hash tables are available in many programming languages as part of their standard libraries. Python has `dict`, Java has `HashMap`, scala has `Map`, Elixir has `Maps`, Rust has `HashMap`, Ruby's `Hash` class is a hash table implementation too. You get it, they're almost everywhere.

Hash tables can be implemented in different ways, [the wikipedia article][wikipedia-hash-table] shows a few different examples. The one we'll explore in this chapter uses a collision resolution called separate chaining. But why do we need collision resolution? To answer this we first need to look at the central element of a hash table, its hash function.

A [hash function][wikipedia-hash-function] must obey a few properties, one of the most important ones being determinism, in other words, identical inputs should result in identical outputs. To explain why, let's look at how the hashing function is used through a pseudo code implementation of a hash table:

```
function new_node(key, value, next_node)
    return Node(key, value, next_node)

function update_node(node, new_value)
    return Node(node.key, new_value)

function create_hash_table()
    table = allocate_array_of_arbitrary_initial_size()
    return table

function add_key_value_pair(table, key, value)
    hash = hash_function(key)
    index = hash % table.size
    node = table[index]
    if node == null
        table[index] = new_node(key, value, null)
    else
        while node != null && node.key != key
            node = node.next_node

        if node.nil?
            existing_node = table[index]
            table[index] = new_node(key, value, existing_node)
        else
            update_node(node, value)

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

The previous pseudo code section shows five functions, the first one is `new_node`. This function acts as the entry point of a linked list. A node contains a key, a value, and a next node value. If the next node value is null, the element is the last one in the list.

Appending an element to such list is done by first creating a single node list and then a second one, with the `next_node` value set to the first one:

```
first_node = new_node(k1, v1, null)
two_node_list = new_node(k2, v2, first_node)
```

In this example `first_node` is a list with a single node, and `two_node_list` is a list with two nodes. The first node is the one with the key `k2` and the value `v2`, its `next_node` value is equal to `first_node`, which has the key `k1` and value `v1`, it does not have a `next_node` value and is the last element of the list.

`update_node` works with an existing node and changes its value. It is a useful function when we find an existing pair with a matching key in `add_key_value_pair`. We explore this other function in more details below.

`create_hash_table` does one thing, it allocates an array of arbitrary size. We purposefully do not define this function here. The size is not really important, as long as it creates a non empty array. The implementation of the allocation is also not really relevant to this example. Most operating systems provide such features, so it's therefore fair to assume that it would use the allocation operations provided by the operating system.

`add_key_value_pair` does more work and let's walk through it, one line at a time. It takes three parameters, the table we want to insert the pair into, the key and the value.
We first call `hash_function` with `key`. We'll dive deeper into what an implementation of `hash_function` looks like later, but for now, let's assume it returns an integer. Because the hash function is unaware of the size of the array, the returned value might be larger than the size.

We use the modulo operation to convert the hash value returned by `hash_function` into a number between 0 and `table.size - 1`. We can now use the result of the modulo operation as an index. That's why we have the `create_hash_table` function, to make sure that table is initialized with empty slots. These slots are often called buckets in hash table documentations.

There are two distinct cases to consider if there is already an item at the location obtained through the hash function. The existing key/value pair might be one with the same value, in this case we want to override its value with the new one.

The other one is that the node or nodes already present might be different, in which case we want to keep all the existing nodes and the new one. **This is called a collision**.

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

**Trying to avoid collisions**

The `hash_function` we used in the previous works well as an example because of its simplicity but it would not be practical in the real world. To keep hash tables efficient, we want to reduce the number of collisions as much as possible. This is because iterating through the linked list is inefficient, if there are a lot of collisions, it could take a long time to loop through all the items in the bucket.

This is there the [uniformity property][wikipedia-hash-function-uniformity] of a hash function is really important. Uniformity helps reduce the likelihood of collision. In the previous example, if an hypothetical hash function had returned the values 1, 2 & 3, respectively, instead of 1, 2 & 2, there wouldn't have been any conflicts.

Collisions are also related to the size of the underlying array. Regardless of the uniformity of the hash function, if the underlying array has a size n, storing n + 1 items cannot happen without at least one collision.

One approach would be to start by allocating a very large amount of memory, but this can be wasteful, because there could be a lot of memory allocated, but unused. Many hash table implementation have mechanisms to adjust the size as needed, and it turns out that Redis does this too, as we'll see in the next section.

**Back to determinism**

Now that we know how the result of a hash function is used, that is, it determines the location of a key/value pair in the underlying array, let's go back to the determinism element of a hash function.

Let's demonstrate why we need determinism by showing what would happen with a hash function that is not deterministic.

In Ruby, each object is given an object id, in the following examples, the two variables `str1` & `str2` are different instances, each holding the same value, and are therefore considered equal:

``` ruby
str1 = "hello"
str2 = "hello"
str1.object_id # => 180
str2.object_id # => 200
# The object_id values might be different on your machine, they are assigned at runtime
# and will therefore differ if you've created more Ruby objects beforehand for instance
str1 == str2 # => true
```

Let's define a wrong hash function, which returns its input's `object_id`:

``` ruby
def hash_function(object)
  object.object_id
end
```

Let's manually walk through a small example, let's start by creating a hash table of size 3 and add the pair `a-key/a-value` to it. Let's re-use the same `object_id` from the same example, and assume that `a-key` would have returned 180. `180 % 3 = 0`, so we insert the new node at index 0:

``` ruby
table = [Node("a-key", "a-value", nil), nil, nil]
```

And let's now call the lookup function with a different string holding the same value, and, reusing the previous example data again, assume that its object id is 200, `200 % 3 = 2`. The lookup would look at the bucket at index 2, find a `nil` value and return nil, whereas the table does contain a pair with the key `a-key`.

A deterministic hash function prevents this.

**Common Hash Functions**

In order for a hash table implementation to be efficient, it needs a good hash functions. Hash functions come in different flavors, as shown [on wikipedia][wikipedia-list-of-hash-functions]:

- Cyclic redundancy checks
- Checksums
- Universal hash function families
- Non-cryptographic hash functions
- Keyed cryptographic hash functions
- Unkeyed cryptographic hash functions

Some of the functions in the "Unkeyed cryptographic hash functions" category are pretty common. MD5 used to be very common to verify the integrity of a file downloaded over the internet. You would download the file, compute the md5 of the file locally and compare it against the md5 published by the author of the file. It is common to see sha256 used instead nowadays. This is what the [Downloads page on ruby-lang.org][ruby-downloads] does!

For a long time sha1 was the default algorithm used by git to hash commits and other objects. It now supports multiple algorithms such as sha256. This change was required after researchers proved that it was possible to forge two different inputs resulting in the same sha1 hash.

Redis uses SipHash which is in the "Keyed cryptographic hash functions". We will look closer at the SipHash algorithm below.

It turns out that Ruby's objects all implement a `hash` function, which happens to use the Siphash algorithm, the same algorithm Redis uses!

``` ruby
str1 = "hello"
str2 = "hello"
# Note that the hash value is partially computed from a random value and will thefore be different
# on your machine
str1.hash # => 2242191710387986831
str2.hash # => 2242191710387986831
```

Now that we know what a hash function, how it used to implement a hash table, let's look at how Redis handles it.

## How does Redis do it?

http://blog.wjin.org/posts/redis-internal-data-structure-dictionary.html

Redis uses 3 main data structures to implement a dictionary.

It's important to note that dictionaries are used in multiple places in the Redis codebase, but there are two main ones for each database, the one holding all the top-level key/value pairs, such as the ones added with `SET` and other commands creating pairs, and the `expires` dictionary, used to store key TTLs.

If you're not used to C, don't worry too much about it for now, we're not going to look at pointers and other C specific features.

Our implementation supports a single database, but Redis can handle multiple databases. A database in Redis represents a set of key/value pairs. A database is defined as the following C struct:

``` c
// https://github.com/antirez/redis/blob/6.0.0/src/server.h#L644-L654
typedef struct redisDb {
    dict *dict;                 /* The keyspace for this DB */
    dict *expires;              /* Timeout of keys with a timeout set */
    dict *blocking_keys;        /* Keys with clients waiting for data (BLPOP)*/
    dict *ready_keys;           /* Blocked keys that received a PUSH */
    dict *watched_keys;         /* WATCHED keys for MULTI/EXEC CAS */
    int id;                     /* Database ID */
    long long avg_ttl;          /* Average TTL, just for stats */
    unsigned long expires_cursor; /* Cursor of the active expire cycle. */
    list *defrag_later;         /* List of key names to attempt to defrag one by one, gradually. */
} redisDb;
```

We will ignore all the fields but the first two for now. We can see that the two fields, `dict` & `expires` are both of the same type: `dict`.

`dict` is defined in [`dict.h`][redis-source-dict]:

``` c
typedef struct dict {
    dictType *type;
    void *privdata;
    dictht ht[2];
    long rehashidx; /* rehashing not in progress if rehashidx == -1 */
    unsigned long iterators; /* number of iterators currently running */
} dict;
```

The `dictType` is struct is used to configure the behavior of a `dict` instance, such as using a different hash function for instance. It is defined as:

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

This syntax is used to defined pointers to function. That's about as far as we'll go with C in this chapter. We don't need to change these values so we will not implement these features in our implementation.

The most interesting element of the `dict` struct for us is the `dictht` array. `ht` here stands for **H**ash **T**able. `ht[2]` means that the struct member is named `ht` and is an array of size two. Essentially, each `dict` instance has two hash tables, `ht[0]` & `ht[1]`.

`dictht` is defined as follows:

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

The comment tells us why a dict has two tables, for rehashing. To explain rehashing, we first need to explain the first member of `dictht`: `dictEntry **table`. The double star syntax, a pointer to pointer, is not that interesting to us at the moment. What we do need to do is look at the `dictEntry` struct:

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

`dictEntry` is a linked list, a common term for a structure like this one is "a node". It contains a key, `key`, a value, `v` and a link to the next element in the list, `next`.

`dictEntry **table` in `dict` defines an array of `dictEntry` items, with a dynamic size, determined at runtime. This is why `dictht` also includes a `size` member. `used` is a counter, that starts at `0` and that is incremented when items are added, and decremented when items are removed.

`sizemask` is an integer value, which is initialized at `0` if size is also `0`, but is otherwise always set to `size - 1`.

To understand the need for the `sizemask` member, let's look back at our pseudo code implementation from above. We can see that a very common operation is to use `hash_value % array_size`. This operation converts a value, potentially larger than the array size, to one that is between 0 and size - 1, allowing us to use it as an index.

The modulo operation, `%`, is not that costly, but it does require a few steps, am integer division, followed by a multiplication and a subtraction:

```
c - (c/m*m)
```

Given how crucial this operation is to the performance of a hash table, every operation relies on it, to find the location inside the backing array, it is valuable to attempt to optimize it.

It turns out that if, and only if, the modulus (the second part of the modulo operation, b in a % b, the size of the array in the hash table) is a power of two, then the modulo can be computed in a single operation with the bitwise `AND`/`&` operator:

```
a & (b-1)
```

Let's illustrate this with a few examples, if the backing array of the hash table is of size 4, then:

```
0 % 4 = 0, 0 & 3 = 0
1 % 4 = 1, 1 & 3 = 1
2 % 4 = 2, 2 & 3 = 2
3 % 4 = 3, 3 & 3 = 3
4 % 4 = 0, 4 & 3 = 0
5 % 4 = 1, 5 & 3 = 1
...
```

It might help to visualize the number as binary, let's use 4 bit integers for readability. The binary representation of 4 is `0100` and 3 is `0011`, so `0 & 3` can be represented as:

```
 0000
&0011
    =
 0000 (0)
```

The following shows the visualization of `1 & 3`, `2 & 3`, `3 & 3`, `4 & 3` & `5 & 3`.

```
 0001 (1)  |  0010 (2) |  0011 (3) |  0100 (4) |  0101 (5)
&0011      | &0011     | &0011     | &0011     | &0011
    =      |     =     |     =     |     =     |     =
 0001 (1)  |  0010 (2) |  0011 (3) |  0000 (0) |  0001 (1)
```

In order to take advantage of this property, Redis always picks a size that is a power of two for the backing array. By setting `sizemask` to `size - 1`. Redis can efficiently compute the index of any keys once it obtained its hash value. This is the code for `dictFind`

``` c
dictEntry *dictFind(dict *d, const void *key)
{
    // ...
    h = dictHashKey(d, key);
    for (table = 0; table <= 1; table++) {
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        // ...
    }
    return NULL;
}
```

`h` is the value returned by the hash function and `d->ht[table].sizemask` is how Redis accesses the `sizemask` value for its hash table. `idx` is the index indicating the location of the bucket. Redis then looks into the array to inspect the bucket with `he = d->ht[table].table[idx]`.

**Rehashing**

Now that we know the data structures that Redis uses to implement its dict type, we need to look at the rehashing process. A new dictionary in Redis is always empty, the backing table is set to `NULL` and the `size`, `sizemask` and `used` members are all set to 0:

``` c
// https://github.com/antirez/redis/blob/6.0/src/dict.c#L102-L108
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

// https://github.com/antirez/redis/blob/6.0/src/dict.c#L121-L131
int _dictInit(dict *d, dictType *type,
        void *privDataPtr)
{
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);
    d->type = type;
    d->privdata = privDataPtr;
    d->rehashidx = -1;
    d->iterators = 0;
    return DICT_OK;
}
```

Whenever Redis adds a new key/value pair to a dictionary, it first checks if the dictionary should be expanded. The main reason causing a dict to expand is if the number of items in it, the `used` member, is greater than or equal to the size of the dict, the `size` member. This will always be true for an empty dictionary since both are initialized to 0. This will also true every time the number of items reaches the size of the dict. When the dict is of size 4, once 4 items are added, the next addition will trigger a resize.

As mentioned earlier, in order to take advantage of the "fast modulo for a power of two value through bitwise AND" property, Redis will always choose a power of two for the size. The smallest non empty size is 4, and it will grow through power of twos from there on. 8, 16, 32, 64 and so on. All the way up to `LONG_MAX + 1`, `9,223,372,036,854,775,808`. That's 9.2 billion billions! Yes it's a huge number!

Back in [Chapter 4][chapter-4] we talked about Big ) notation and time complexity. The bottom line being that since Redis processes incoming commands sequentially, a slow operation would effectively back the queue. You can think of it as someone taking a long time to go through checkout at a grocery store. The longer they take, the more likely it is that the queue of customers waiting in will increase.

Resizing a hash table is essentially an `O(n)` operation, that it is, the time it takes to do it is proportional to n, the number of elements in the hash table. In other words, the more elements in the table, the longer it'll take to resize it. And as we just saw, Redis hash tables can get big, really big! Forcing all the clients in the queue to wait while we resize the table is far from desirable.

Enter rehashing!

Rehashing is the process Redis uses to incrementally, in small steps, resize the table, while still allowing other operations to be processed, and this is why it uses two hash tables per dictionary. Let's look at how rehashing through an example.

Note that resizing the array is technically not necessary to store the items given that each entry in the array, the buckets, are linked lists. This means that even an array of size 4 could store millions and billions of key/value pairs. The problem is that the performance would suffer drastically, iterating through that many items in a linked list would take a very long time. With millions of items, it could easily take multiple seconds.

- The Redis server starts, the main dict is initialized with two hash tables, both empty
- The server receives a SET command, it needs to expand the dictionary.
- It finds the next power of two that will be enough to fit all the elements, it needs to store one element, so it uses the minimum value 4.
- It allocates the main array, ht[0], with a size of 4, and adds the first key/value pair
- The second, third & fourth values are added without any issues. used is now set to 4
- A fifth SET command is received, Redis decides to resize the dict.
- The resize process allocates a new table, big enough to store all the items, for which the size is a power of two. It selects the next power of two, 8.
- The new table is assigned to the secondary table, the rehashing one, ht[1]
- The dict is now a rehashing state. In this state, all new keys are added to the rehashing table.
- The dict has now 2 tables, where 4 keys are in the first table and 1 is in the second one.
- While in this state, many operations, such as key lookups will look at both tables. For instance a GET command will first look at the first table, and if it doesn't find the item, will look at the rehashing table, and only if the items can't be find in either table, will return NULL.
- While in rehashing state, many commands, such as key lookups or key additions, will perform a single step of rehashing. The server_cron time event we looked in Chapter 3 also attempts to rehash dictionaries that needs to.
- The rehashing process starts at index 0, looks at the first table, and if it finds an item, moves it to the second table. It then moves to index 1, until it iterated through the entire table
- Once it's done, it makes the rehashing table the primary, resets the rehashing table to an empty table and exits the rehashing state

This process allows Redis to resize dictionaries in small steps, while not preventing clients to send commands in the meantime.

If you want to dig deeper in the Redis implementation, here are few interesting functions you can start looking at:

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

### The SipHash hash function

The last thing we need to look at before building our own hash table is the hash function. Redis has been using the SipHash algorithm since 5.0. Before that it has been using the MurmurHash2 algorithm as of 2.5. And before that, it used a simple version from Dan Bernstein called [djb2][hash-djb2].

One of the benefits of SipHash is that it offers strong protection against attacks such as [hash flooding][hash-flooding].

The implementation of the Siphash algorithm is quite complicated. The one used by Redis is in the [`siphash.c` file][redis-source-siphash] and a Ruby implementation is provided in [Appendix B][appendix-b]. What is important to note is that Siphash requires a key, usually coming from random bytes, to compute a hash value.

This means that unlike md5 or sha1, which always return the same value for the same input, siphash will return the same value, if, and only if, the key is the same.

This is a simplified explanation of how the hash flooding protection works. If redis were to use md5 as its hashing function, I would know what the hash value used to compute the index would be:

The md5 hash for the string `a` is the string `0cc175b9c0f1b6a831c399e269772661`:

```
Digest::MD5.hexdigest("a") # => 0cc175b9c0f1b6a831c399e269772661
```

The result is a 32 character string representing a 128-bit (16 bytes) result. Because most CPUs use 64-bit integers as their largest types, the result we just saw is actually the hex representation of two 64 bit integers. Let's illustrate this with the `pack` and `unpack` method:

Source: https://anthonylewis.com/2011/02/09/to-hex-and-back-with-ruby/

The string is a hex string, so we need to look at each pair of characters. We call `hex` on each pair, which returns the integer value. For instance `'00'.hex` return `0`, `'ff'.hex` returns `255`, the maximum value of an 8-bit integer. We then call `.pack('c16')` which returns a string representing all the bits concatenated together.

Finally `.unpack('QQ')` looks at the string and tries to convert to two 64 bit integers:

```
"0cc175b9c0f1b6a831c399e269772661".scan(/../).map(&:hex).pack('c16').unpack("QQ")
# => [12157170054180749580, 7000413967451013937]
```

A simpler example might help illustrate this:

``` ruby
"ffff0000ffff0000ffff0000ffff0000".scan(/../).map(&:hex)
=> [255, 255, 0, 0, 255, 255, 0, 0, 255, 255, 0, 0, 255, 255, 0, 0]

[255, 255, 0, 0, 255, 255, 0, 0, 255, 255, 0, 0, 255, 255, 0, 0].pack('c16')
=> "\xFF\xFF\x00\x00\xFF\xFF\x00\x00\xFF\xFF\x00\x00\xFF\xFF\x00\x00"

"\xFF\xFF\x00\x00\xFF\xFF\x00\x00\xFF\xFF\x00\x00\xFF\xFF\x00\x00".unpack('QQ')
=> [281470681808895, 281470681808895]
```

We can play with `pack` and `unpack` a little bit more to confirm that these two 64-bit integers we got are the two elements of the md5 result:

```
[12157170054180749580].pack("Q").unpack("H16")
=> ["0cc175b9c0f1b6a8"]
[7000413967451013937].pack("Q").unpack("H16")
=> ["31c399e269772661"]
```

`12157170054180749580` represents the first 64 bits of the md5 value, by calling `.pack('Q')` we convert it to a string representing all these bits back to back, and convert it back to a string of 16 hex characters with `.unpack('H16')`. We can confirm that `0cc175b9c0f1b6a` is the first half of `0cc175b9c0f1b6a831c399e269772661` and that `31c399e269772661` is the second half.

We can also look at the actual 64 bits with `unpack('B64')`

```
[12157170054180749580].pack('Q').unpack('B64')
=> ["0000110011000001011101011011100111000000111100011011011010101000"]
[7000413967451013937].pack('Q').unpack('B64')
=> ["0011000111000011100110011110001001101001011101110010011001100001"]
```

Back to our hypothetical use of md5 as a hash function in Redis. Given that we would only use a single integer to apply the modulo to, we could pick either way, so let's pick the second one, just because.

And now, if I sent the command `SET a-key a-value`, the hash value of `a-key` is the 64 bit integer `7000413967451013937`. This knowledge can be used to forge special requests and maximize the chances of collisions, potentially causing performance issues to the hash table.

With a keyed algorithm such as Siphash, it's impossible to infer what the hash value would be if the server uses random bytes as the key. We can demonstrate this with Ruby, which also uses Siphash by running `"a".hash` in an `irb` shell, closing and reopening `irb`, the `hash` value will be different. This is because Ruby initializes random bytes at startup that it then uses to compute siphash values.

Siphash was added to Redis in [this commit](https://github.com/redis/redis/commit/adeed29a99dcd0efdbfe4dbd5da74e7b01966c67)

## Our own `Dict` class

### a

a

### b

b

### c

c

### Adding the `DEL` command

del

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

[java-doc-tree-map]:https://docs.oracle.com/javase/8/docs/api/java/util/TreeMap.html
[wikipedia-hash-table]:https://en.wikipedia.org/wiki/Hash_table
[wikipedia-hash-function]:https://en.wikipedia.org/wiki/Hash_function
[wikipedia-list-of-hash-functions]:https://en.wikipedia.org/wiki/List_of_hash_functions
[wikipedia-hash-function-uniformity]:https://en.wikipedia.org/wiki/Hash_function#Uniformity
[wikipedia-md5]:https://en.wikipedia.org/wiki/MD5
[wikipedia-sha-1]:https://en.wikipedia.org/wiki/SHA-1
[wikipedia-sha-2]:https://en.wikipedia.org/wiki/SHA-256
[chapter-4]:/post/chapter-4-adding-missing-options-to-set/
[redis-source-dict]:https://github.com/antirez/redis/blob/6.0.0/src/dict.h#L76-L82
[hash-djb2]:http://www.cse.yorku.ca/~oz/hash.html
[hash-flooding]:https://131002.net/siphash/siphashdos_appsec12_slides.pdf
[redis-source-siphash]:https://github.com/redis/redis/blob/6.0.0/src/siphash.c
[appendix-b]:#appendix-b-a-ruby-implementation-of-siphash
[ruby-downloads]:https://www.ruby-lang.org/en/downloads/

## Appendix B: A Ruby implementation of SipHash

``` ruby
# Credit to https://github.com/emboss/siphash-ruby/blob/master/lib/siphash.rb
module SipHash

  def self.digest(key, msg)
    s = State.new(key)
    len = msg.size
    iter = len / 8

    iter.times do |i|
      m = msg.slice(i * 8, 8).unpack("Q<")[0]
      s.apply_block(m)
    end

    m = last_block(msg, len, iter)

    s.apply_block(m)
    s.finalize
    s.digest
  end

  private

  def self.last_block(msg, len, iter)
    last = (len << 56) & State::MASK_64;

    r = len % 8
    off = iter * 8

    last |= msg[off + 6].ord << 48 if r >= 7
    last |= msg[off + 5].ord << 40 if r >= 6
    last |= msg[off + 4].ord << 32 if r >= 5
    last |= msg[off + 3].ord << 24 if r >= 4
    last |= msg[off + 2].ord << 16 if r >= 3
    last |= msg[off + 1].ord << 8 if r >= 2
    last |= msg[off].ord if r >= 1
    last
  end

  class State

    MASK_64 = 0xffffffffffffffff

    def initialize(key)
      @v0 = 0x736f6d6570736575
      @v1 = 0x646f72616e646f6d
      @v2 = 0x6c7967656e657261
      @v3 = 0x7465646279746573

      k0 = key.slice(0, 8).unpack("Q<")[0]
      k1 = key.slice(8, 8).unpack("Q<")[0]

      @v0 ^= k0
      @v1 ^= k1
      @v2 ^= k0
      @v3 ^= k1
    end

    def apply_block(m)
      @v3 ^= m
      1.times { compress }
      @v0 ^= m
    end

    def rotl64(num, shift)
      ((num << shift) & MASK_64) | (num >> (64 - shift))
    end

    def compress
      @v0 = (@v0 + @v1) & MASK_64
      @v2 = (@v2 + @v3) & MASK_64
      @v1 = rotl64(@v1, 13)
      @v3 = rotl64(@v3, 16)
      @v1 ^= @v0
      @v3 ^= @v2
      @v0 = rotl64(@v0, 32)
      @v2 = (@v2 + @v1) & MASK_64
      @v0 = (@v0 + @v3) & MASK_64
      @v1 = rotl64(@v1, 17)
      @v3 = rotl64(@v3, 21)
      @v1 ^= @v2
      @v3 ^= @v0
      @v2 = rotl64(@v2, 32)
    end

    def finalize
      @v2 ^= 0xff
      3.times { compress }
    end

    def digest
      @v0 ^ @v1 ^ @v2 ^ @v3
    end

  end
end

```
