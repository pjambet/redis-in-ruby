---
title: "Chapter 8 Adding Hash Commands"
date: 2020-11-03T14:36:32-05:00
lastmod: 2020-11-03T14:36:38-05:00
draft: false
comment: false
keywords: []
summary:  "In this chapter we add support for a new data type, Lists. We implement all the commands related to hashes, such as HSET, HGET & HGETALL"
---

## What we'll cover

Now that our server supports Lists, the next data type we will add support for is Hashes. We've covered the concept of a Hash, also called Map or Dictionary in [Chapter 6][chapter-6] where we built our own `Dict` class, implemented as a hash table, to store all the database data in memory. It turns out that within this `Dict`, the `@data_store` instance variables in the `DB` class, values can also be hashes.

This allows clients to store multiple key/value pairs for a top-level key. Say that for instance you wanted to store product data in Redis, where a product has an id, and a set of attributes, such as a name, a price, and an image URL. You could do this with good old `GET`/`SET`, but that would require you to use as many keys in the top level dictionary as you need attributes. It is simpler to use a hash in this case:

``` bash
127.0.0.1:6379> HSET product:1 name "Product One" price 25 image_url https://...
(integer) 3
127.0.0.1:6379> HSET product:123 name "Product 123" price 100 image_url https://...
(integer) 3
127.0.0.1:6379> HGETALL product:1
1) "name"
2) "Product One"
3) "price"
4) "25"
5) "image_url"
6) "https://..."
127.0.0.1:6379> HGETALL product:123
1) "name"
2) "Product 123"
3) "price"
4) "100"
5) "image_url"
6) "https://..."
127.0.0.1:6379> HGET product:1 name
"Product One"
127.0.0.1:6379> HGET product:123 price
"100"
```

With the `HSET` command we can set as many key/value pairs as we want for the given key, `product:1` and `product:123` in the example above. Note that since RESP does not support a dictionary type, the returned value of `HGETALL`, which returns all the pairs, is a flat array of field names and values, it is up to the client to read this array and wrap it in a more appropriate data type, such as `Hash` in Ruby, `Object` or `Map` in JavaScript, or `dict` in Python.

There are [15 commands][redis-doc-hash-commands] for the Hash data type:

- **HDEL**: Delete one or more fields from a hash
- **HEXISTS**: Check for the existence of a field in a hash
- **HGET**: Return the value for the given field
- **HGETALL**: Return all the key/value pairs
- **HINCRBY**: Increment the value for the given field, by the given integer, positive or negative
- **HINCRBYFLOAT**: Increment the value for the given field, by the given float, positive or negative
- **HKEYS**: Return all the keys
- **HLEN**: Return the number of pairs
- **HMGET**: Return all the values for the given keys
- **HMSET**: This command is deprecated, it was necessary before HSET gained the capability to set multiple key/value pairs at once
- **HSCAN**: Return a subset of key/value pairs as well as a scan cursor. This is similar to the [SCAN command][redis-doc-scan-command]
- **HSET**: Set one or more key/value pairs, creating the hash if it does not already exist
- **HSETNX**: Set the value for the given field in the hash, only if the field does not already exist
- **HSTRLEN**: Return the length of the string stored for the given field
- **HVALS**: Return all the values

We will only implement thirteen of these fifteen commands, we will not implement `HMSET`, because as noted above, it was made obsolete when `HSET` was updated in 4.0.0 to become variadic. That's just a fancy word to say that it accepts one or more key/value pairs. Prior to that it would only accept a single pair.

We will also ignore `HSCAN`, it behaves very similarly to `SCAN`, which operates on the top-level dictionary, `SSCAN`, which works on sets and `ZSCAN` which works on sorted sets. The idea behind each of these commands is that retrieving all the values is an O(n) operation, where n is the number of elements in the database for `SCAN`, the number of fields for `HSCAN` and the number of members for `SSCAN` & `ZSCAN`. In practical terms, it means that calling `HSCAN` on large hashes, which have no limit on the number of pairs, the memory available is the only limit, will be very slow if that number is really high.

The `*SCAN` commands "solve" this problem by breaking the iteration in multiple steps, calling `HSCAN` only returns a subset of the key/value pairs, and includes a cursor that can be used to keep iterating until the cursor is 0, indicating that the iteration is over.

`SCAN` is the alternative to `KEYS`, `HSCAN` is the alternative to `HKEYS`, `SSCAN` is the alternative to `SMEMBERS` and `ZSCAN` is the alternative to `ZRANGE zset 0 -1 WITHSCORES`.

All four `*SCAN` commands work very similarly and are fairly complex, the C implementation spans over a few hundred lines and code, and for reference the documentation of `dictScan`, one of the main functions, is [over 80 lines long][redis-src-dict-scan-doc]. A big part of the complexity comes from the fact that the `*SCAN` commands are stateless, the server does not store any data with the status of the iteration, and it also needs to be smart to know how to iterate over the underlying dict if it is being rehashed.

_The `*SCAN` commands might be implemented in a later chapter but this is not currently planned_

It is important to note that despite the existence of the `HINCRBY` & `HINCRBYFLOAT` commands, all keys and values in a Hash are strings. We will see in details how these two commands are implemented and how the data is converted from a string to an integer or a float later in this chapter.

## How does Redis do it

We already know, to some extent, how Redis handles dictionaries, we explored how it implemented a hash table in the `dict.c` file in [Chapter 6][chapter-6], but what we built so far is missing a few elements, which we'll look into later.

Additionally, Redis uses a really interesting approach where it uses a different underlying structure to store the hash data depending on the size of the hash and the length of the keys and values. These values can be configured through the configuration file, the default values are [512][redis-conf-max-items] for the maximum number of items stored as a ziplist & [64][redis-conf-max-value] for the maximum length of a key or value stored as a ziplist. As long as the number of keys is lower or equal to `512` and that the strings stored in the hash are shorter than `64` characters, Redis will use a ziplist to store the Hash. Once any of these two conditions break, it will convert the ziplist to a dict.

We will implement a similar approach because it illustrates a crucial point with regard to time complexity and O-notation. Most of the operations important to a hash, such as `HGET`, have an O(n) time complexity when using a list. This is because in the case where the element we're looking for is the last element in the list or is not present, we'd have to browse the whole list to find it. On the other hand, as we've seen in [Chapter 6][chapter-6], a hash table, such as the one we implemented in the `Dict` class, can perform this operation with a O(1) complexity.

That being said, O(n) does not mean "slow", and O(1) does not mean "fast". What they mean is that a dict lookup using a hash table will always require the same number of steps and therefore take roughly the same amount of time, that's what O(1) means, the time it takes is constant. On the other hand a hash lookup using a list will require one more step, in the worst case scenario, for each new items in the list, this is a linear growth.

Now, back to our hash table, what we want, or what the users of our database want, is for it to be as fast as possible. If a hash only contains a single key/value pair, it seems very likely that a list implementation will actually be faster than a hash table. There's no hashing, no internal table allocation, a single check of the element at the head of the list and that's it.

If a list is more efficient than a hash table for one element, it seems reasonable to assume it will also be faster for two, three, four and all "small" hashes. But how do we define "small" in concrete terms. Well, this is when you have to measure things. The process here would be to run benchmarks, to measure the performance of different operations, against each implementation and see at what point the list implementation starts to slow down to a point where a hash table would be faster.

The developers of Redis did this work, and we the default value is 512 entries, as we can see in the [`redis.conf` file][redis-conf-file-hash-max-ziplist-entries]. This value means that Redis will use a ziplist for the first 512 pairs added to the hash, and adding a 513th one will start using a hash table. Let's look at it in practice:

``` bash
127.0.0.1:6379> HSET h 1 1
(integer) 1
127.0.0.1:6379> DEBUG OBJECT h
Value at:0x7ffe00804650 refcount:1 encoding:ziplist serializedlength:16 lru:10150943 lru_seconds_idle:5
```

The `DEBUG OBJECT` command returns information about the given key, including its encoding, and we can see that the hash at key `h` is encoded as a ziplist. Let's add 511 items to it and see what happens, we could do this with redis-cli, but it would take a while, so let's use `irb`, with the `redis` gem:

``` ruby
irb(main):001:0> require 'redis'
=> true
irb(main):002:0> redis = Redis.new
irb(main):003:0> 511.times { |i| redis.hset('h', i, i) }
=> 511
```

Back in `redis-cli`:

``` bash
127.0.0.1:6379> debug object h
Value at:0x7ffdfec194a0 refcount:1 encoding:ziplist serializedlength:2836 lru:10150882 lru_seconds_idle:5
```

The hash is still a ziplist, and contains 512 pairs, let's add another one:

``` bash
127.0.0.1:6379> HSET h f 1
(integer) 1
127.0.0.1:6379> debug object h
Value at:0x7ffdfec194a0 refcount:1 encoding:hashtable serializedlength:2825 lru:10150894 lru_seconds_idle:2
```

And voila! Redis updated the encoding of the hash from a ziplist to a hashtable, the changes are in practice invisible to the user, the commands are the same, but internally Redis uses what it believes is the most efficient implementation.

The hash length is not the only factor Redis uses to decide which underlying implementation to use, it also uses the length of the values, with a default value of 64 in the [`redis.conf` file][redis-conf-file-hash-max-ziplist-value]. If either a key or a value in the hash has a length longer than 64, Redis will start using a hash table instead of a ziplist. This is a consequence of the ziplist data structure, which we explored in [Appendix A][chapter-7-appendix-a] in the previous chapter. Ziplists are represented as a chunk of contiguous memory, making them more and more expensive to manipulate at they grow, as the whole list needs to be reallocated when a new element is inserted for instance. When using small key and value strings, the whole chunk of memory allocated will stay relatively small, but if we were to store any strings until the size of the hash reaches 512 entries, we might still end up with a very big and slow ziplist if a client started using long strings as keys or values.

## Adding Hash Commands

It's interesting to consider the fact that in essence the hash type in Redis does not require any new concrete data structures, it is a layer of abstraction on top of ziplists and dicts. We have not reimplemented ziplists so we will instead use our `List` class for small hashes, and use the `Dict` class for large hashes.

We first need to add the ability to create hashes, that's what `HSET` & `HSETNX` do.

### Creating a Hash with HSET & HSETNX

`HSET`'s behavior is fairly similar to `LPUSH` & `RPUSH` from the previous chapter. If no objects exist for the given key, a new hash is created, and all the given key/value pairs are added to it. If an object already exists and is not a hash, an error is returned, and if a hash already exists, the new pairs are added to it. The command returns the number of fields added to the hash. Updating elements does not count as adding new elements since we're not adding a new pair to the hash. Let's look at some examples:

``` bash
127.0.0.1:6379> HSET h field-1 value-1 one 1 2 two
(integer) 3
127.0.0.1:6379> HGETALL h
1) "field-1"
2) "value-1"
3) "one"
4) "1"
5) "2"
6) "two"
127.0.0.1:6379> HSET h field-1 new-value one something-else a-new-key a-new-pair
(integer) 1
127.0.0.1:6379> HGETALL h
1) "field-1"
2) "new-value"
3) "one"
4) "something-else"
5) "2"
6) "two"
7) "a-new-key"
8) "a-new-pair"
```

The second `HSET` command returned one because only one of the three given keys did not already exists, the other two, `field-1` and `one` were updated. As mentioned above, note that in a hash, everything is a string.

**Config values**

We want our hash implementation to behave similarly to Redis and choose the best underlying structure, between `List` and `Dict`, depending on the size of the hash. To achieve this we are creating a `Config` module, which will keep the value of all the supported configuration options. For now we only need two configs, `hash_max_ziplist_entries` & `hash_max_ziplist_value`:

``` ruby
module BYORedis
  module Config

    UnsupportedConfigParameter = Class.new(StandardError)
    UnknownConfigType = Class.new(StandardError)

    DEFAULT = {
      hash_max_ziplist_entries: 512,
      hash_max_ziplist_value: 64,
    }

    @config = DEFAULT.clone

    def self.set_config(key, value)
      key = key.to_sym
      existing_config = @config[key]
      raise UnsupportedConfigParameter, key unless existing_config

      case existing_config
      when Integer
        @config[key] = Utils.string_to_integer(value)
      else
        raise UnknownConfigType, "#{ key }/#{ value }"
      end
    end

    def self.get_config(key)
      @config[key.to_sym]
    end
  end
end
```
_listing 8.1 The `Config` class_

Similarly to what we did in the previous chapter, we're going to create a new file, `hash_commands.rb`, where we'll add all the command classes related to the Hash data type, let's start with `HSetCommand`:

``` ruby
module BYORedis
  class HSetCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      key = @args.shift
      raise InvalidArgsLength unless @args.length.even?

      hash = @db.lookup_hash_for_write(key)
      count = 0

      @args.each_slice(2).each do |pair|
        key = pair[0]
        value = pair[1]

        count += 1 if hash.set(key, value)
      end

      RESPInteger.new(count)
    end

    def self.describe
      Describe.new('hset', -4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.2 The `HSetCommand` class_

In order for the `Server` class to respond to the `HSET` command we need to add a `require` statement in `server.rb` for the new `hash_commands.rb` file, as well as adding an entry in the `COMMANDS` dictionary. This is a repetitive process, so we will stop showing it from now on, but remember that for each class that we add, we actually need to "enable" it in the `Server` class.

We have to use a new type of validation for the number of arguments with the `HSET` command, all arguments after the hash's key come in pairs, so we need to validate that we have an even number of arguments after the key.

We now need a class to represent the hash, in the Redis source code, the file that implements the hash logic is called `t_hash.c`. The main data types are all implemented in files starting with `t_`, which I assume stands for **T**ype. String commands are implemented in `t_string.c`, List commands in `t_list.c`, Set commands in `t_set.c`, Sorted Sets commands in `t_zset.c`, Hash commands in `t_hash.c` and Stream commands in `t_stream.c`. We could follow this pattern and name our class `THash`, but this is not a very explicit name, so instead we'll go with `RedisHash`, to be more explicit. We are not calling it `Hash` because Ruby already ships a `Hash` class, and even though nothing technically prevents up from "reopening" the class and adding our own methods, overriding existing ones, this would likely become problematic. For instance, we might accidentally use methods defined in the Ruby `Hash` class, it is easier to start fresh, with our own class.

``` ruby
module BYORedis
  class RedisHash

    ListEntry = Struct.new(:key, :value)

    def initialize
      @underlying = List.new
    end

    def set(key, value)
      max_string_length = Config.get_config(:hash_max_ziplist_value)
      convert_list_to_dict if @underlying.is_a?(List) &&
                              (key.length > max_string_length || value.length > max_string_length)

      case @underlying
      when List then
        added = set_list(key, value)
        if @underlying.size + length > Config.get_config(:hash_max_ziplist_entries)
          convert_list_to_dict
        end
        added
      when Dict then @underlying.set(key, value)
      else raise "Unknown structure type: #{ @underlying }"
      end
    end
    alias []= set

    private

    def set_list(key, value)
      iterator = List.left_to_right_iterator(@underlying)
      while iterator.cursor && iterator.cursor.value.key != key
        iterator.next
      end

      if iterator.cursor.nil?
        @underlying.right_push(ListEntry.new(key, value))

        true
      else
        iterator.cursor.value.value = value

        false
      end
    end

    def convert_list_to_dict
      dict = Dict.new
      iterator = List.left_to_right_iterator(@underlying)

      while iterator.cursor
        dict[iterator.cursor.value.key] = iterator.cursor.value.value
        iterator.next
      end

      @underlying = dict
    end
  end
end
```
_listing 8.3 The `RedisHash` class_

The `RedisHash` class starts with a `List` as the data structure backing the hash, it will be converted to a `Dict` as needed, when the hash grows.

Our implementation differs slightly from Redis with regards to how data is stored in the list. Redis stores the keys and values, as flat elements, one after the other. This means that adding one pair to the hash results in two elements being added to the list. Our approach is bit different, we create a struct, `ListEntry`, to store the pairs in the list. This allows us to use our `List` class in a slightly more idiomatic way. One pair represents conceptually one element. This allows us to directly use the `size` attribute of the list, instead of having to divide it by two to obtain the number of elements in the hash.

The `set` method, which we alias to `[]=`, to provide a similar API to the `Dict` & `Hash` classes, is the method used by the `HSET` command. We first lookup the current value of the `hash_max_ziplist_value` config, and convert the list to a dict if either the key or the value we're adding are longer than the config.

Once this check is performed, we use a pattern we'll see a lot in this chapter, a `case/when` statement to check the type of `@underlying`. There are three branches, it is either a `List`, a `Dict`, or anything else, in which case we want to crash the server as this is never supposed to happen.

The code for the `List` branch requires a few more lines of code, so we extract it to the `set_list` private method, below in the class. In the `Dict` case, it only requires three lines of code. We start by checking how many items are present in the `Dict`, we then use the `Dict#set` method, which we slightly modify to return a boolean:

``` ruby
module BYORedis
  class Dict
    # ...
    def set(key, value)
      entry = get_entry(key)
      if entry
        entry.value = value

        false
      else
        add(key, value)

        true
      end
    end
    alias []= set
    # ...
    end
  end
end
```
_listing 8.4 Updates to the `set` method in the `Dict` class_

The `Dict#set` method we introduced in [Chapter 6][chapter-6] used to return the new value, which was not really helpful since the caller knows what the value is, it is the second argument to the method. We're now returning a boolean instead, indicating whether the pair was added or not.
The method either updates the value if the key is already in the hash, or add it altogether. We're not using the `Dict#[]=` alias here because doing so will not return anything, and we do care about the boolean value returned, to increment the count in the `HSetCommand` class.

The `set_list` method creates a list iterator and starts iterating from the head, as long as the node's key is different from the given key, we keep iterating. If we encounter a node with the same key, the iteration stops, and we update the value for that node. Otherwise, we add a new node at the end of the list. As noted above, we are using a new class to store node values, `ListEntry`. We could have used a "tuple approach", by storing a two-element array, such as `[ 'key', 'value' ]`, but the `ListEntry` struct makes things a little bit more explicit, the items stored in the list can be read with clear methods, `key` & `value`, instead of using `[0]` & `[1]` respectively with the tuple approach.

Now that we have enough of `RedisHash` implemented, we need to add the new `lookup_hash_for_write` method to the `DB` class:

``` ruby
module BYORedis
  class DB

    # ...

    def lookup_hash(key)
      hash = @data_store[key]
      raise WrongTypeError if hash && !hash.is_a?(RedisHash)

      hash
    end

    def lookup_hash_for_write(key)
      hash = lookup_hash(key)
      if hash.nil?
        hash = RedisHash.new
        @data_store[key] = hash
      end

      hash
    end
  end
end
```
_listing 8.5 The `lookup_hash_for_write` method in the `DB` class_

This method is very similar to the one we create for lists in the [previous chapter][chapter-7], except that it expects a `Hash` instance and creates one if necessary.

And with all this, the server can now response to `HSET` commands, let's now add the `HSetNXCommand`:

``` ruby
module BYORedis
  # ...

  class HSetNXCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      key = @args[0]
      field = @args[1]
      value = @args[2]
      hash = @db.lookup_hash_for_write(key)

      if hash[field]
        RESPInteger.new(0)
      else
        hash[field] = value
        RESPInteger.new(1)
      end
    end

    def self.describe
      Describe.new('hsetnx', 4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.6 The `HSetNX` class_

This new command uses existing methods, if the given field already exists in the hash, we directly return `0` and leave the hash untouched. On the other hand, if the field is not already present, we add it, using the `RedisHash#[]=` this time, since we know it will add the element, and return `1`.

Now that we can create hashes, we need to update the `TypeCommand` class to respond with `set` for set keys:

``` ruby
module BYORedis
  class TypeCommand < BaseCommand

    def call
      Utils.assert_args_length(1, @args)

      key = @args[0]
      ExpireHelper.check_if_expired(@db, key)
      value = @db.data_store[key]

      case value
      when nil       then RESPSimpleString.new('none')
      when String    then RESPSimpleString.new('string')
      when List      then RESPSimpleString.new('list')
      when RedisHash then RESPSimpleString.new('hash')
      else raise "Unknown type for #{ value }"
      end
    end

    # ...

  end
end
```
_listing 8.7 Updates to the `TypeCommand` class to handle `RedisHash` instances_

### Reading Hash values with HGET, HMGET & HGETALL

Now that we can create Hash instances in our database, we need to add the ability to read data from these hashes for them to be _actually_ useful. Redis hash three commands to do so, `HGET`, to retrieve a single value, `HMGET`, to retrieve multiple values at once, and `HGETALL` to retrieve all the key/value pairs.

Let's start with adding the `HGetCommand`:

``` ruby
module BYORedis
  # ...
  class HGetCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)

      hash = @db.lookup_hash(@args[0])

      if hash.nil?
        NullBulkStringInstance
      else
        key = @args[1]
        value = hash[key]
        if value.nil?
          NullBulkStringInstance
        else
          RESPBulkString.new(value)
        end
      end
    end

    def self.describe
      Describe.new('hget', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.8 The `HGetCommand` class_

If the hash does not exist, or if the hash exists but does not contain the field, we return a null string, otherwise, we return the string stored for that field. We need to add the ability to find a key/value pair to the `RedisHash` class:

``` ruby
module BYORedis
  class RedisHash

    # ...

    def get(field)
      case @underlying
      when List then get_list(field)
      when Dict then @underlying[field]
      else raise "Unknown structure type: #{ @underlying }"
      end
    end
    alias [] get

    private

    # ...

    def get_list(field)
      iterator = List.left_to_right_iterator(@underlying)

      while iterator.cursor
        return iterator.cursor.value.value if iterator.cursor.value.key == field

        iterator.next
      end
    end
  end
end
```
_listing 8.9 The `RedisHash#get` method_

Once again, the `Dict` branch is simpler, so we perform it inline, we call the `Dict#[]` method, and return its result, a string or nil. In the `List` case, we go to the `get_list` private method. The approach here is very similar to the `set_list` method we wrote earlier, we iterate through the list, starting at the head, and stop if we find a `ListEntry` for which the `key` attribute matches the `field` parameter. If no entry matches, the method returns `nil`. Note that this is a perfect example of the worst case scenario time complexity we previously discussed. If the `field` is not present in the hash, we still have to iterate through the entire list to check every `ListEntry` instances.

Let's continue with the `HMGetCommand`:

``` ruby
module BYORedis
  # ...
  class HMGetCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)

      key = @args.shift
      hash = @db.lookup_hash(key)

      if hash.nil?
        responses = Array.new(@args.length)
      else
        responses = @args.map do |field|
          hash[field]
        end
      end

      RESPArray.new(responses)
    end

    def self.describe
      Describe.new('hmget', -3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.10 The `HMGetCommand` class_

The `HMGET` command is very similar to `HGET`, the only difference is that it accepts multiple fields as its input, and returns an array. The implementation uses the same method from `RedisHash`, `get`, which we use through its alias, `[]`, and call it in the block passed to `Array#map`. Using `map` here allows us to maintain the order of the results, we create an array where the n-th item will be the value for the n-th field passed as command argument after the hash key itself.

If the hash does not exist, we use the `Array.new` method to create an array of `nil` values with the same length as the number of fields passed to the command.

The last read command we need to add is `HGetAllCommand`. Because RESP2 does not have support for a map type, the result is an even-numbered array, containing an alternating sequence of keys and values.

``` ruby
module BYORedis
  # ...
  class HGetAllCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)

      hash = @db.lookup_hash(@args[0])

      if hash.nil?
        pairs = []
      else
        pairs = hash.get_all
      end

      RESPArray.new(pairs)
    end

    def self.describe
      Describe.new('hgetall', 2, [ 'readonly', 'random' ], 1, 1, 1,
                   [ '@read', '@hash', '@slow' ])
    end
  end
end
```
_listing 8.11 The `HGetAllCommand` class_

This time we need a new method in `RedisHash`, `get_all`:

``` ruby
module BYORedis
  class RedisHash

    # ...

    def get_all
      case @underlying
      when List then get_all_list
      when Dict then get_all_dict
      else raise "Unknown structure type: #{ @underlying }"
      end
    end

    private

    # ...

    def get_all_list
      iterator = List.left_to_right_iterator(@underlying)
      pairs = []
      while iterator.cursor
        pairs.push(iterator.cursor.value.key, iterator.cursor.value.value)
        iterator.next
      end

      pairs
    end

    def get_all_dict
      pairs = []

      @underlying.each do |key, value|
        pairs.push(key, value)
      end

      pairs
    end
  end
end
```

The implementation for both data structures requires a few lines of code, so we move it to two private methods. In `get_all_list`, we follow the tried and true pattern we've used so far. We start iterating from the head, and accumulate the keys and values in an array.

In the `get_all_dict` method, we rely on the `Dict#each` method, which iterates through all the pairs in the dictionary, and for each pair we push both the key and the value to an array, and return it.

We now have a solid foundation for the Hash commands, we can add elements to the hash and read them back. Next on the list is the ability to increment values, if and only if the strings represent numeric values.

### Incrementing numeric values with HINCRBY & HINCRBYFLOAT

Redis supports two commands to increment or decrement numeric values, `HINCRBY` & `HINCRBYFLOAT`. Decrement operations are performed using these commands with a negative argument. To decrement a value in a hash by 1, you would call `HINCRBY h key -1`.

These commands are very similar to [`INCRBY`][redis-doc-incrby] and [`INCRBYFLOAT`][redis-doc-incrbyfloat] which operates on Strings at the top-level. The `*INCRBY` commands only accept integer increments, and will reject floats, the `*INCRBYFLOAT` commands accept both integers and floats.

Even though you could use integer values with `HINCRBYFLOAT`, `HINCRBY` is still useful for two reasons, well really, only the first one _actually_ matters:

**Exactness**: because the float based commands use floating point arithmetic, you're not guaranteed to get the result you'd expect, unexpected results can (and will) happen. Let's look at an example, imagine that you're building a bidding platforms where you store prices. It would be a fair requirement to increment the price of a product after a bid:

``` bash
127.0.0.1:6379> HSET product price 166.92
(integer) 1
127.0.0.1:6379> HINCRBYFLOAT product price 402.22
"569.14000000000000001"
```

Yikes, yeah, that's close, but that's not really what we'd expect, which is `569.14`. Floating point errors happen very often, for instance, many languages fail to return `3.3` for `1.1 + 2.2`, you can try it with Ruby, Python, Elixir, Scala, Haskell and Javascript, they all pretty much return the same thing: `3.3000000000000003`. The website [0.30000000000000004.com][floating-point-errors] shows even more examples across most programming languages and explains in more details the cause of this unexpected result.

The bottom line is that floating point arithmetic suffers from precision issues, whereas integer operations do not. The only caveat to be aware of regarding integer arithmetic is around overflows, which we'll cover later when we implement the `HINCRBY` command.

**A little bit less memory used**: Redis uses `long double` variables for the floating point numbers, which use 16 bytes of memory, whereas it uses `long long` for integers, which use 8 bytes of memory. That being said, note these types are only used while the command is processed, the data in the hash, whether it is a list or a dict is a string, which uses one byte per digit. The string `'1'` representing the integer `1` uses 1 byte, the string `'1.1'` representing the float `1.1` uses 3 bytes, and so on.

**Important note about prices**

If you're working on any systems that handle prices, avoid at all cost using floating point numbers. A common approach is to always manipulate prices in cents, or whatever is the smallest currency unit, and use integers. In the example above, we would have done the following:

``` bash
127.0.0.1:6379> HSET product price 16692
(integer) 1
127.0.0.1:6379> HINCRBY product price 40222
(integer) 56914
```

With this approach, you only transform the price from cents to the "regular" unit, dollar, yuan, pound, euro by doing the appropriate division only when displaying it to the user in the expected unit. By doing so, you guarantee that addition and subtraction operations will never result in loss of precision, as long as they don't overflow.

**HINCRBY**

Let's start with `HINCRBY`, and before writing any code, let's play with it quickly in the repl:

``` bash
127.0.0.1:6379> HINCRBY h an-int 1
(integer) 1
127.0.0.1:6379> HINCRBY h an-int a
(error) ERR value is not an integer or out of range
127.0.0.1:6379> HSET h not-an-int a
(integer) 1
127.0.0.1:6379> HINCRBY h not-an-int 1
(error) ERR hash value is not an integer
127.0.0.1:6379> HINCRBY h an-int 9223372036854775806
(integer) 9223372036854775807
127.0.0.1:6379> HINCRBY h an-int 1
(error) ERR increment or decrement would overflow
127.0.0.1:6379> HINCRBY h an-int -9223372036854775807
(integer) 0
127.0.0.1:6379> HINCRBY h an-int -9223372036854775807
(integer) -9223372036854775807
127.0.0.1:6379> HINCRBY h an-int -1
(integer) -9223372036854775808
127.0.0.1:6379> HINCRBY h an-int -1
(error) ERR increment or decrement would overflow
```

As we can see in the previous example, calling `HINCRBY` on a non existing hash creates one, initializes the field's value to 0 and apply the increment afterwards. An error is return is the increment value is not an integer, and a different error is return if the value we're trying to increment cannot be represented an integer.

The other examples show the behavior around integer overflows. Redis attempts to convert the stored string values as `long long`, which are represented as signed 64 bit integers. The maximum value of a `long long` is `2^63 - 1`, `9,223,372,036,854,775,807` and the minimum value is `-(2^63)`, `-9,223,372,036,854,775,808`. One might expect that the minimum should equal the inverse of the maximum, that is `min = -max`, but as we just saw that's not the case, we have `min = -(max + 1)`.

This is a result of the representation of signed integers as [two's complement][wikipedia-twos-complement]. With this representation the first bit is used to represent the sign, 1 means negative, 0 means positive. The other 63 bits are used to encode the actual integer value, which is why the max and min values are around `2^63`. The biggest value that can be encoded is a zero, for the positive sign, followed by sixty-three `1`s, which is `2^63 - 1`. Let's look at an example with less bits, for the sake of simplicity. Imagine a three bit integer, the max value would be `2^2 - 1`, `3` and the min value would be `-(2^2)`, `-4`. As previously mentioned the max value is a zero followed by ones, `011`. To obtain `3` from this, we start from the right, with the first digit, a `1`, and use the index, starting at zero, as the power value, we get `2^0`, which is `1`, we then continue, another `1`, at index 1, which gives us `2^1`, `2`. `2 + 1 = 3` so far so good. Another way to get to this number is with `2^2 - 1`, also `3`. In plain English, "Two to the power of the number of bits minus 1, minus 1". The same exact approach can be applied to 63 bits instead of 2, `2^0 + 2^1 + 2^2 + ... + 2^62 = 2^63 - 1`.

In order to confirm the min value, we need to look at how negative numbers are represented in two's complement representation.

Two's complement is defined as:

> The two's complement of an N-bit number is defined as its complement with respect to 2^N; the sum of a number and its two's complement is 2^N

Let's take 2 as an example, represented in binary as `010`, using three bits. So, with N set to three, `2^N` is `8`, so to get to 8 from 2, we need 6, since `8 - 2 = 6`, `6` is `110` as a three bit integer, `2^2 + 2^1`. This tells us that the complement of `2`, is `6`, so `-2` is represented the same way we'd represent `6`, as `110`.

Another, and probably easier, way to obtain the two's complement of a number is by inverting the digits and adding one.
Using this definition, let's see how we would represent `-1`. `1` is `001`, because `2^0 = 1`, so to represent `-1`, we first flip all the bits, `110`, and add one, `111`. Let's do the same thing for `-2`, `2` is `010`, because `2^1 = 2`, so flipping the bits gives `101`, and adding one is `110`, using the same process, we get to `101` for `-3`. We have the representation of 7 numbers so far, `-3` (`101`), `-2` (`110`), `-1` (`111`), `0` (`000`), `1` (`001`), `2` (`010`) & `3` (`011`), but there are eight possible values with three bits, indeed, none of these numbers use `100`.

For the previous numbers, we started from their decimal representation, but to show that `100` represents `-4`, we can use the opposite approach, convert a number from its two's representation, to its decimal version. So let's start with `100` and show that we end up with `-4`. To do so, we can start from the left, and the leftmost digit is treated differently from others, if `1`, it is negative, if zero, well, it's zero, there's nothing to do, we then proceed to add all the following power of twos, so for `100`, which is the third digit from the right, so index 2 in a 0-based system, `2^2 = 4`, but since it's a `1`, we start start at `-4`, we then add `0^1`, we use `1` as the power here because the second digit, has an index of 1, and we finally add `0^0`, for the rightmost digit, 0, at index 0, `-4 + 0 + 0 = -4`!

We can illustrate this approach with the numbers we previously arrived at, let's look at `-3`/`101` for instance `-(2^2) + 0^1 + 1^1`, which can be expanded to `-4 + 0 + 1`, `-3`!

It's important to note that with two's complement, it is impossible to represent negative zero, which does not exist in ordinary arithmetic, zero does not have a sign.

It's time to create the `HIncrByCommand` class:

``` ruby
module BYORedis

  # ...

  class HIncrByCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      incr = Utils.validate_integer(@args[2])

      key = @args[0]
      field = @args[1]
      hash = @db.lookup_hash_for_write(key)

      value = hash[field]
      if value.nil?
        value = 0
      else
        value = Utils.string_to_integer(value)
      end

      if (incr < 0 && value < 0 && incr < (LLONG_MIN - value)) ||
         (incr > 0 && value > 0 && incr > (LLONG_MAX - value))
        raise IntegerOverflow
      else
        new_value = value + incr
      end

      hash[field] = Utils.integer_to_string(new_value)

      RESPInteger.new(new_value)
    rescue InvalidIntegerString
      RESPError.new('ERR hash value is not an integer')
    rescue IntegerOverflow
      RESPError.new('ERR increment or decrement would overflow')
    end

    def self.describe
      Describe.new('hincrby', 4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.12 The `HIncrByCommand` class_

Once the validations are done, we look at the value for the given field and initialize it to `0` if the field does not exist. If the field does exist, we want to convert the string to an integer, returning an error if it cannot be converted. We use a new method in the `Utils` module to do so, `string_to_integer`.

The next step in an integer overflow check. This check is artificial in a language like Ruby that supports overflowing numbers, but, in order to both keep compatibility with Redis as well as understand how integer arithmetic works, we're imposing these arbitrary constraints on ourselves here.

We want to check that the operation will not result in an overflow. An overflow would happen if the sum of the old value and the new value were to be greater than the max value that can be represented by a signed 64-bit integer, `2^63-1` or lower than the minimum value, `-2^63`. We created two constants to hold these values, `LLONG_MIN` & `LLONG_MAX`, which happen to be defined in the `climits.h` header file in C.

We _could_ have written these lines in a way that might be considered easier to read, with:

``` ruby
new_value = value + incr
if new_value > LLONG_MAX || new_value < LLONG_MIN
```

While this would work, this is a little bit of a chicken and egg problem, we'd be relying on the fact that the operation did overflow to raise an exception, but we wouldn't be able to know that the operation overflowed in a system where such situations can happen, like in C, because it would have overflowed. In other words, the condition could never have been true because no signed integer can be greater than `LLONG_MAX` and no signed integer can be lower than `LLONG_MIN`.

So far we were relying on the `Kernel#Integer` method to parse strings to integers. While this worked well until now, doing this is a little bit like "cheating". As a matter of fact, Redis uses its own function to transform a string to a `long long`: [`string2ll`][redis-src-string2ll].

Let's now add the `string_to_integer` method to the `Utils` module:

``` ruby
module BYORedis

  ULLONG_MAX = 2**64 - 1 # 18,446,744,073,709,551,615
  ULLONG_MIN = 0
  LLONG_MAX = 2**63 - 1 # 9,223,372,036,854,775,807
  LLONG_MIN = 2**63 * - 1 # -9,223,372,036,854,775,808

  IntegerOverflow = Class.new(StandardError)
  InvalidIntegerString = Class.new(StandardError)

  module Utils

    # ...

    def self.string_to_integer(string)
      raise InvalidIntegerString, 'Empty string' if string.empty?

      bytes = string.bytes
      zero_ord = '0'.ord # 48, 'a'.ord == 97, so

      return 0 if bytes.length == 1 && bytes[0] == zero_ord

      if bytes[0] == '-'.ord
        negative = true
        bytes.shift
        raise InvalidIntegerString, 'Nothing after -' if bytes.empty?
      else
        negative = false
      end

      unless bytes[0] >= '1'.ord && bytes[0] <= '9'.ord
        raise InvalidIntegerString
      end

      num = bytes[0] - zero_ord

      1.upto(bytes.length - 1) do |i|
        unless bytes[i] >= zero_ord && bytes[i] <= '9'.ord
          raise InvalidIntegerString, "Not a number: '#{ bytes[i] }' / '#{ [ bytes[i] ].pack('C') }'"
        end

        raise IntegerOverflow, 'Overflow before *' if num > ULLONG_MAX / 10

        num *= 10
        raise IntegerOverflow, 'Overflow before +' if num > ULLONG_MAX - (bytes[i] - zero_ord)

        num += bytes[i] - zero_ord
      end

      if negative && num > -LLONG_MIN
        # In Redis, the condition is:
        #
        # if (v > ( (unsigned long long) (-(LLONG_MIN+1)) +1) )
        #
        # But used to be (-(unsigned long long)LLONG_MIN) until this commit:
        # https://github.com/redis/redis/commit/5d08193126df54405dae3073c62b7c19ae03d1a4
        #
        # Both seem to be similar but the current version might be safer on different machines.
        # Essentially it adds one to LLONG_MIN, so that multiplying it by -1 with the - operator
        # falls within the boundaries of a long long, given that min can be -9...808 while max
        # is always 9...807, we then cast the positive value to an unsigned long long, so that
        # we can add 1 to it, turning it into 9...808
        # The C standard does not seem to be very specific around the exact value of LLONG_MIN
        # it seems to either be -9..807 or, as it is on my machine, a mac, -9...808, which is
        # because it uses Two's Complement.
        raise IntegerOverflow, 'Too small for a long long'
      elsif negative
        -num
      elsif num > LLONG_MAX
        raise IntegerOverflow, 'Too big for a long long'
      else
        num
      end
    end
  end
end
```
_listing 8.13 The `string_to_integer` method_

There's a lot going on in this method, so let's take it one step at a time. The overall approach is to look at all the characters in the string, starting from the left, and converting them to a number, and accumulate it to the final result. The accumulated number is an unsigned number, and the last step is to make sure that the parsed number can fit within a signed number. Let's dive right in:

If the string is empty, there's no need to continue, we raise an `InvalidIntegerString`, which is rescued in the command class to return the `value is not an integer or out of range` error. The next step is to get all the bytes in the string, which is what we'll be iterating over. Note that an array of bytes is how strings are represented in C, with the type `char buf[]`, a `char` is an 8-bit type, a byte. While it could have been tempting to use the `String#[]` method, as we've shown in [Chapter 5][chapter-5], the Ruby String class performs a few tricks under the hood to deal with characters spanning over more than one byte. The following is an example using the wave emoji:

``` ruby
irb(main):102:0> s = '👋'
irb(main):103:0> s[0]
=> "👋"
irb(main):104:0> s.bytes
=> [240, 159, 145, 139]
```

As we can see the `String#[]` method makes it look like there's only one character, when there are actually four bytes in that string. The bytes are returned as numbers, between 0 and 255, the range of an 8-bit integer:

``` ruby
irb(main):110:0> 'abc'.bytes
=> [97, 98, 99]
irb(main):111:0> '123'.bytes
=> [49, 50, 51]
```

The next step is a small helper variable we'll need throughout the method. In Redis, they use `'0'` which can be used for integer arithmetic, and is replaced by its ASCII representation, 48. Because we will need this value a lot, we store it in a variable to avoid having to use `'0'.ord` throughout the method. The [`String#ord`][ruby-doc-string-ord] method returns the 'ordinal' value, that is its value in the ASCII encoding, `'a'` is `97`, `'b'` is `98`, `'1'` is `49`, `'2'` is `50` and so on. We can see that these values correspond to what the `String#bytes` method returns.

If the string only contains one byte and that byte is equal to `48`, the value of the zero character, then we return `0` right away, the work is done.

In the case where the first character is equal to `45`, which is the value returned by `'-'.ord`, then we set the boolean variable `negative` to true, so that we know to return a negative number at the end of the method. We also call `bytes.shift` to remove the negative sign from the byte array. If the array is empty after that, that is, we only received a string containing a negative sign, we raise an `InvalidIntegerString` error, the input is invalid.

The first digit of a number cannot be `0`, it can only be between `1` and `9`, so we raise an error if the first byte is not in that range, between `49` (`'1'.ord`) and `57` (`'9'.ord`).

We then initialize the `num` variable, which we'll be our accumulator throughout the method, to `num = bytes[0] - zero_ord`. The operation `bytes[0] - zero_ord` returns the integer value of a character representing a digit between `0` and `9`. This is because the value of the character `'0'` in ASCII is `48`, and only goes up from there, so the character `'5'`, which is `53` in ASCII, will return `5` when doing `'5'.ord - zero_ord`.

Now that the first digit was converted, we need to convert the rest of the string, where this time `0` is now an acceptable value, since numbers cannot start with a zero but can contain a zero afterwards. In the loop, we start by checking that the current byte is within `48` & `57` and raise an error if it is isn't. We then need to perform a few overflow checks. For the sake of simplicity, let's imagine that we were dealing with 8-bit numbers, where the maximum value would be `255` for an unsigned number, `2^8 - 1`.

Let's manually go through the steps for the number `254`, in this case, by the time we enter the loop, `num` would have been set to `'2'.ord - zero_ord`, `2`. In the loop, we'd start with `i` set to `1`, giving us `bytes[1] == '5'`. The range check would pass, `'5'.ord` is `53`, it is between `48` and `57`. The next check is `num > ULLONG_MAX / 10`, `ULLONG_MAX` is actually `2^64 - 1`, but in our simplified 8-bit example, it is `2^8 - 1`, `255`. `255 / 10` would return `25`, because this is an integer division, and `num`, `2`, is not greater than that, so the check would pass, we then multiply `num` by `10`, now that we know that multiplying it by `10` will not overflow.

We then check that `num > ULLONG_MAX - (bytes[i] - zero_ord)`, which is essentially a way to check that adding the next digit from the string to `num` will not overflow. `bytes[i]` is `'5'`, subtracting `zero_ord` returns `5`, so we check that `20 > 255 - 5`, `20` is not greater than `250`, so we can perform the addition, `num` is now `25`.

Repeating these steps in the next iteration, we look at the next character, `'4'`, `25` is not greater than `25`, so we multiply `num` by `10`, giving us `250`, and `250` is not greater than `255 - 4`, so we add `4` to `num` and get the final result, `254`.

Let's quickly look at two examples that would have raised overflow errors. If we had attempted to parse `256`, we would have entered the loop with `num` set to `2`, similarly as above, left the iteration at `25`, multiplied by `10`, and then failed the check `250 > 255 - 6`. `255` is greater than `249`, so we cannot add `6` to `255`, it does not fit.

The other failure happens with numbers greater than `299`, let's try with `300`. We would enter the loop with `num` set to `3`, exited the first iteration with `num` set to `30`, and in the second iteration, we would fail the check `30 > 25`. This tells us that we cannot multiply `30` by `10` and stay within the bounds of the integers we can represent.

The next steps take care of handling the sign of the number.

If `negative` is `true`, then we need to multiple `num` by `-1`, but before doing so, we need to check it is within the limits of a signed integer. So far we've parsed the numbers as an unsigned integer, which can go up to `2^64-1`, but the minimum value of a signed integer is `-2^63`, so if num is greater than `-LLONG_MIN`, `9,223,372,036,854,775,808`, then we raise an overflow error, otherwise, we can safely multiple `num` by `-1`. In other words, if `num` is `9,223,372,036,854,775,808` or lower, we can multiply it by `-1`, if it is `9,223,372,036,854,775,809` or more, then it would not fit.

We perform a similar check if `negative` is `false` and the final number should be positive, in that case, we check that `num` is not greater than `LLONG_MAX`, `2^63 - 1`, `9,223,372,036,854,775,807`, and if it is, we raise an overflow error.

Finally, if all the checks passed, we return `num`!

We can now rewrite `validate_integer` to use our own `string_to_integer` method:

Up until now we were using the `OptionUtils.validate_integer` method, which used the ruby `Integer` class to transform a `String` instance to an `Integer` instance, we can now use `string_to_integer` instead.

So, let's delete the `option_utils` file altogether and only use the `Utils` module:

``` ruby
module BYORedis
  module Utils

    # ...

    def self.validate_integer(str)
      string_to_integer(str)
    rescue IntegerOverflow, InvalidIntegerString
      raise ValidationError, 'ERR value is not an integer or out of range'
    end
  end
end
```
_listing 8.14 The `validate_integer` method_

We catch both exceptions here `IntegerOverflow` and `InvalidIntegerString`, and raise a `ValidationError` instead. This allows us to keep using the code in `BaseCommand` we introduced in the previous chapter.

The last method we need to add to the `Utils` module is `integer_to_string`, which we need to convert the new value back to a string before updating the value in the hash.

``` ruby
module BYORedis
  module Utils

    # ...

    def self.integer_to_string(integer)
      return '0' if integer == 0

      v = integer >= 0 ? integer : -integer
      zero_ord = '0'.ord
      bytes = []

      until v == 0
        bytes.prepend(zero_ord + v % 10)
        v /= 10
      end

      bytes.prepend('-'.ord) if integer < 0
      bytes.pack('C*')
    end
  end
end
```
_listing 8.15 The `validate_integer` method_

We start this method by converting the input to a positive integer in case it was negative, this allows us to process it regardless of its sign, and we prepend the `'-'` character at the end if the input was indeed negative.

We create an empty array, which we'll use to accumulate all the bytes representing all the characters of the string. We then loop until `v` reaches `0`. In each iteration of the loop we get the character value by getting the modulo 10 of the input. Getting the modulo 10 essentially returns the right most digit. `255 % 10 = 5`, `36 % 10 = 6`. Once we have the decimal value of the rightmost digit, we add it to `zero_ord`, `48`, to get the ASCII value of that number. The last step of the loop is to divide `v` by 10, to shift the whole number to the right, with the previous two examples, `255` would become `25`, and `36` would become `3`.

Once the loop exits, we have an array of number, representing the byte values of the string. We can now use the `pack` method to transform it a Ruby String. The `C` format tells Ruby to treat each number in the array as an 8-bit value representing a character, so it knows that `48` will be `'0'`, `49`, `'1'` and so on.

And with that, we have a working implementation of the `HINCRBY` command.

**HINCRBYFLOAT**

Ruby has a `Float` class, which, quoting the [official documentation][ruby-doc-float]:

> represent inexact real numbers using the native architecture's double-precision floating point representation.

While we could use the `Float` class to implement the `HINCRBYFLOAT` command, its precision is inferior to the implementation in Redis. Redis always use 17 digits precision, and with Ruby's `Float` class, we'd be stuck with at most a double-precision floating point number, which is a `double` in C. Redis uses [`long double`][wikipedia-long-double], which offer greater precision. Ruby does not provide an easy way to use `long double` numbers, but it provides another class to handle number with decimal digits, [`BigDecimal`][ruby-doc-bigdecimal]:

> provides arbitrary-precision floating point decimal arithmetic

The `BigDecimal` class provides so many features that it could be considered "cheating" according to the rules I set for this book, but dealing with floating point is a really complicated topic, and, to some extent, is not _that_ central to how Redis works. That being said, even the Redis codebase does not perform all the float operations "from scratch". The conversion from a string to a `long double` is performed with the [`strtold`][c-std-strtold] function provided by the C standard library. This function takes care of a lot of the heavy lifting, such as parsing numbers using the [E notation][wikipedia-e-notation], like `1.1234e5` being parsed to `112,340.0`. The conversion back from a `long double` to a string is then performed with `snprintf("%.17Lf")`, which is another function provided by the C standard library.

Let's look at how `BigDecimal` works:

``` ruby
irb(main):001:0> require 'bigdecimal'
=> true
irb(main):002:0> 0.1 + 0.2
=> 0.30000000000000004
irb(main):003:0> BigDecimal(0.1, 1) + BigDecimal(0.2, 1)
=> 0.3e0
```

The `BigDecimal` constructor, which weirdly enough is the [method `BigDecimal` on the `Kernel` class][ruby-doc-big-decimal-method], requires two arguments if the first argument is a float, to determine how many significant digits should be considered. In this example, one is enough, but let's look at an example with other numbers to see its impact:

``` ruby
irb(main):008:0> BigDecimal(0.123, 2)
=> 0.12e0
```

We passed the float `0.123`, but because we told `BigDecimal` to only consider two significant digits, the result is a `BigDecimal` object representing the number `0.12`.

Let's just check what Redis returns for the same operation:

``` bash
127.0.0.1:6379> HINCRBYFLOAT h 1 0.1
"0.1"
127.0.0.1:6379> HINCRBYFLOAT h 1 0.2
"0.3"
```

The example above illustrates that _some_ rounding errors we observe in Ruby, and other languages, with double-precision floating numbers are avoided with the `long double` type.

So now that we settled on the `BigDecimal` class, it's time to create the command class:

``` ruby
module BYORedis

  # ...

  class HIncrByFloatCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      incr = Utils.validate_float(@args[2], 'ERR value is not a valid float')

      key = @args[0]
      field = @args[1]
      hash = @db.lookup_hash_for_write(key)

      value = hash[field]
      if value.nil?
        value = BigDecimal(0)
      else
        value = Utils.validate_float(value, 'ERR hash value is not a float')
      end

      new_value = value + incr

      raise FloatOverflow if new_value.nan? || new_value.infinite?

      new_value_as_string = Utils.float_to_string(new_value)
      hash[field] = new_value_as_string

      RESPBulkString.new(new_value_as_string)
    rescue InvalidFloatString
      RESPError.new('ERR hash value is not a float')
    rescue FloatOverflow
      RESPError.new('ERR increment would produce NaN or Infinity')
    end

    def self.describe
      Describe.new('hincrbyfloat', 4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.16 The `HIncrByFloatCommand` class_

In the previous chapter we introduced the `OptionUtils.validate_float` method, but as we just did with `validate_integer`, we are going to use a new method in the `Utils` package, and use `BigDecimal` instead of the `Float` class as we used to:

``` ruby
module BYORedis
  module Utils

    # ...

    def self.validate_float(str, error_message)
      case str
      when '+inf', 'inf', 'infinity', '+infinity' then BigDecimal::INFINITY
      when '-inf', '-infinity' then -BigDecimal::INFINITY
      else
        parsed = BigDecimal(str)
        if parsed.nan?
          raise ArgumentError
        else
          parsed
        end
      end
    rescue ArgumentError, TypeError
      raise ValidationError, error_message
    end
  end
end
```
_listing 8.17 The `validate_float` method_

The method mainly relies on `Kernel#BigDecimal` to do the heavy lifting, but we have to add a few custom pieces of logic. The first one is to translate the Redis representation of infinity to the `BigDecimal` one.

Redis recognizes the strings `'inf'`, `'+inf'`, `'infinity'`, `'+infinity'`, `'-inf'` & `'-infinity'` as special values representing the positive and negative infinity values, which are valid floats.

The constraint of the `HINCRBYFLOAT` regarding infinity are interesting given that `HSET` can be used to set the value of a field to either `infinity` or `-infinity` but the result of `HINCRBYFLOAT` cannot be either of these values:

``` bash
127.0.0.1:6379> HSET h valid-inf inf
(integer) 1
127.0.0.1:6379> HSET h invalid-inf infi
(integer) 1
127.0.0.1:6379> HINCRBYFLOAT h valid-inf inf
(error) ERR increment would produce NaN or Infinity
127.0.0.1:6379> HINCRBYFLOAT h invalid-inf inf
(error) ERR hash value is not a float
```

This example shows that setting the value to `inf` means that Redis considers it to be a valid float, but rejects the operation because the result of `inf + inf` would result in infinity, and it refuses to do so. On the other hand, we can see that if the value in the hash was set to `infi`, which is _just_ a regular string, then it fails with a different error, telling us that it can't perform the operation because the value in the hash is not a valid float.

The first error message also mentions `NaN`, which stands for **N**ot **A** **N**umber. `NaN` can happen for operations that cannot result in a valid result, such as the following:

``` ruby
irb(main):122:0> BigDecimal::INFINITY - BigDecimal::INFINITY
=> NaN
```

In order to replicate this logic, we use the `BigDecimal#infinite?` and `BigDecimal#nan?` methods to raise an exception if the result of the operation is not valid for Redis. The last step is similar to the one in `HINCRBY`, we convert the value back to a string, store it in the hash and return it. Let's look at `float_to_string`:

``` ruby
module BYORedis
  module Utils

    # ...

    def self.float_to_string(big_decimal)
      if big_decimal == BigDecimal::INFINITY
        'inf'
      elsif big_decimal == -BigDecimal::INFINITY
        '-inf'
      elsif (truncated = big_decimal.truncate) == big_decimal
        # Remove the .0 part of the number
        integer_to_string(truncated)
      else
        big_decimal.to_s('F')
      end
    end
  end
end
```
_listing 8.18 The `float_to_string` method_

If the value is either `INFINITY` or `-INFINITY`, we transform it to the valid Redis representation, `inf` & `-inf`. This is not necessary for now, but will become useful with other commands.

In the case where the value could be represented as an integer, that is, there are only zeroes on the right side, we want to only return the left side. That is, if the value is `2.0`, we want to return `2`. Checking if the truncated number is the same as the number is a way to test whether or not the number is an integer, in which case we use the `integer_to_string` method. We have to do this because the `to_s` method would otherwise return the number `2.0`.

Finally, we use the `to_s` method on `BigDecimal` with the `F` argument, which returns the number using "conventional floating point notation", it would otherwise use the E notation:

``` ruby
irb(main):124:0> BigDecimal('1.2345').to_s('F')
=> "1.2345"
irb(main):125:0> BigDecimal('1.2345').to_s
=> "0.12345e1"
```

We can now use the `validate_float` method to create the `validate_timeout` method, which we can use for the blocking methods we created in the previous chapter:

``` ruby
module BYORedis
  module Utils

    # ...

    def self.validate_timeout(str)
      timeout = validate_float(str, 'ERR timeout is not a float or out of range')
      raise ValidationError, 'ERR timeout is negative' if timeout < 0 || timeout.infinite?

      timeout
    end
  end
end
```
_listing 8.19 The `validate_timeout` method_

And we can now update the blocking list commands:

``` ruby
module BYORedis
  # ...

  module ListUtils
    # ...

    def self.common_bpop(db, args, operation)
      Utils.assert_args_length_greater_than(1, args)

      timeout = Utils.validate_timeout(args.pop)
      list_names = args
      # ...
    end

    # ...
  end

  # ...

  class BRPopLPushCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      source_key = @args[0]
      source = @db.lookup_list(source_key)
      timeout = Utils.validate_timeout(@args[2])
      destination_key = @args[1]
      # ...
    end
    # ...
  end
end
```
_listing 8.20 Updates to the blocking list commands_

### Utility commands

We have six more commands to add, which happen to be simpler than the ones we added earlier. Let's start with a really useful one, `HDEL`.

**HDEL**

`HDEL` allows clients to delete one or more fields in a hash:

``` ruby
module BYORedis

  # ...

  class HDelCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      key = @args.shift
      hash = @db.lookup_hash(key)

      delete_count = 0
      if hash
        delete_count += @db.delete_from_hash(key, hash, @args)
      end

      RESPInteger.new(delete_count)
    end

    def self.describe
      Describe.new('hdel', -3, [ 'write', 'fast' ], 1, 1, 1,
                   [ '@write', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.21 The `HDelCommend` class_

We call the `DB#delete_from_hash` method, so let's create this method now:

``` ruby
module BYORedis
  class DB

    # ...

    def delete_from_hash(key, hash, fields)
      delete_count = 0
      fields.each do |field|
        delete_count += (hash.delete(field) == true ? 1 : 0)
      end
      @data_store.delete(key) if hash.empty?

      delete_count
    end
  end
end
```
_listing 8.22 The `delete_from_hash` method_

The method iterates over all the given fields and calls `RedisHash#delete`, incrementing a counter for all successful deletions, returning this count at the end of the process. The method also takes care of deleting the hash from the database if the hash is empty after deleting all fields. Let's look at the `delete` method:

``` ruby
module BYORedis
  class RedisHash
    # ...

    def delete(field)
      case @underlying
      when List then was_deleted = delete_from_list(field)
      when Dict then
        was_deleted = !@underlying.delete(field).nil?
        if was_deleted && length - 1 == Config.get_config(:hash_max_ziplist_entries)
          convert_dict_to_list
        elsif @underlying.needs_resize?
          @underlying.resize
        end
      else raise "Unknown structure type: #{ @underlying }"
      end

      was_deleted
    end

    private

    # ...

    def convert_dict_to_list
      list = List.new
      @underlying.each do |key, value|
        list.right_push(ListEntry.new(key, value))
      end

      @underlying = list
    end

    def delete_from_list(field)
      was_deleted = false
      iterator = List.left_to_right_iterator(@underlying)

      while iterator.cursor
        if iterator.cursor.value.key == field
          @underlying.remove_node(iterator.cursor)

          return true
        end

        iterator.next
      end

      was_deleted
    end
  end
end
```
_listing 8.23 The `RedisHash#delete` method_

The deletion process for a list is delegated to the private method `delete_from_list`, while it is inlined for a `Dict`. For the latter, we call the `Dict#delete` method, which returns `nil` if nothing was deleted, or the value for the key it is was found and deleted. We perform two additional checks, first, if the size of the hash is now below the threshold, we convert the dict back to a list, through the private method `convert_dict_to_list`. Finally, we check whether or not the `Dict` instance needs resizing, `Dict` instances automatically grow but do not automatically shrink, so this check will make sure that a `Hash` can reduce its memory footprint and avoid waste.

The `delete_from_list` method should look pretty familiar at this point, we iterate starting from the head, and keep going until we find the element we're trying to delete. When we do find the list entry we need to remove, we call a new method on the `List` class: `List#remove_node`:

``` ruby
module BYORedis
  class List

    ListNode = Struct.new(:value, :prev_node, :next_node) do
      def remove
        if prev_node
          prev_node.next_node = next_node
        end

        if next_node
          next_node.prev_node = prev_node
        end

        self.next_node = nil
        self.prev_node = nil
      end
    end

    # ...

    def remove_node(node)
      if @head == node
        @head = node.next_node
      end

      if @tail == node
        @tail = node.prev_node
      end

      node.remove
      @size -= 1
    end

    # ...
  end
end
```
_listing 8.24 The `List#remove_node` & `ListNode#remove` methods_

The `remove_node` method removes the given node from the list, while updating the `@head` and `@tail` variables if needed. It uses the `ListNode#remove` method, which delegates all the `next_node`/`prev_node` handling to the struct itself. The whole process is very mechanical and reminiscent of the previous chapter, all the node pointers have to be updated, while being careful to check for nil values at each step of the way.

**HEXISTS**

The `HEXISTS` commands is used to check for the existence of a key inside a hash. Note that because RESP does have a boolean type, it returns a boolean, `1` if the key exists, `0` otherwise.

``` ruby
module BYORedis

  # ...

  class HExistsCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)

      hash = @db.lookup_hash(@args[0])

      if hash.nil?
        RESPInteger.new(0)
      else
        value = hash[@args[1]]
        if value.nil?
          RESPInteger.new(0)
        else
          RESPInteger.new(1)
        end
      end
    end

    def self.describe
      Describe.new('hexists', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.25 The `HDelCommend` class_

The command uses the `RedisHash#get`, through its `[]` alias, to check for the existence of the field, and return the appropriate number, acting as a boolean.

**HKEYS**

The `HKEYS` command is used to list all the keys inside a hash:

``` ruby
module BYORedis

  # ...

  class HKeysCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)

      hash = @db.lookup_hash(@args[0])

      if hash.nil?
        EmptyArrayInstance
      else
        RESPArray.new(hash.keys)
      end
    end

    def self.describe
      Describe.new('hkeys', 2, [ 'readonly', 'sort_for_script' ], 1, 1, 1,
                   [ '@read', '@hash', '@slow' ])
    end
  end
end
```
_listing 8.26 The `HKeysCommand` class_

The command uses the new `RedisHash#keys` method:

``` ruby
module BYORedis
  class RedisHash
    # ...

    def keys
      case @underlying
      when List then keys_list
      when Dict then @underlying.keys
      else raise "Unknown structure type: #{ @underlying }"
      end
    end

    # ...

    def keys_list
      iterator = List.left_to_right_iterator(@underlying)
      keys = []

      while iterator.cursor
        keys << iterator.cursor.value.key

        iterator.next
      end

      keys
    end

    # ...
  end
end
```
_listing 8.27 The `RedisHash#keys` method_

When `@underlying` is a `Dict`, we can delegate directly to the `Dict#keys` method, on the other hand, if it is a `List`, we need to manually iterate through all the pairs in the list and accumulate the keys in an array.

**HVALS**

`HVALS` is very similar to `HKEYS`, except that it returns all the values:

``` ruby
module BYORedis

  # ...

  class HValsCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)
      hash = @db.lookup_hash(@args[0])

      if hash.nil?
        EmptyArrayInstance
      else
        RESPArray.new(hash.values)
      end
    end

    def self.describe
      Describe.new('hvals', 2, [ 'readonly', 'sort_for_script' ], 1, 1, 1,
                   [ '@read', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.28 The `HValsCommand` class_

This implementation is also very similar to `HKeysCommand`, except that we call `RedisHash#values`:

``` ruby
module BYORedis
  class RedisHash
    # ...

    def values
      case @underlying
      when List then values_list
      when Dict then @underlying.values
      else raise "Unknown structure type: #{ @underlying }"
      end
    end

    private

    # ...

    def values_list
      iterator = List.left_to_right_iterator(@underlying)
      values = []

      while iterator.cursor
        values << iterator.cursor.value.value

        iterator.next
      end

      values
    end
  end
end
```
_listing 8.29 The `RedisHash#values` method_

Similarly to `RedisHash#keys`, in the `Dict` case we call `Dict#values`, and in the `List` case we iterate through the list and accumulate all the values in an array.

**HLEN**

`HLEN` returns the number of key/value pairs in the hash:

``` ruby
module BYORedis

  # ...

  class HLenCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)

      hash = @db.lookup_hash(@args[0])
      hash_length = 0

      unless hash.nil?
        hash_length = hash.length
      end

      RESPInteger.new(hash_length)
    end

    def self.describe
      Describe.new('hlen', 2, [ 'readonly', 'sort_for_script' ], 1, 1, 1,
                   [ '@read', '@hash', '@slow' ])
    end
  end
end
```
_listing 8.30 The `HLenCommand` class_

We use the `RedisHash#length` method to return the length of the hash:

``` ruby
module BYORedis
  class RedisHash
    # ...

    def length
      case @underlying
      when List then @underlying.size
      when Dict then @underlying.used
      else raise "Unknown structure type: #{ @underlying }"
      end
    end

    # ...
  end
end
```
_listing 8.31 The `RedisHash#length` method_

The `length` method is pretty succinct, it either calls `List#size` or `Dict#used`, which both return the number of elements they contain.

**HSTRLEN**

Finally, `HSTRLEN` returns the length of the value for the given key inside a hash:

``` ruby
module BYORedis

  # ...

  class HStrLenCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      key = @args[0]
      field = @args[1]

      hash = @db.lookup_hash(key)
      value_length = 0

      unless hash.nil?
        value = hash[field]
        value_length = value.length unless value.nil?
      end

      RESPInteger.new(value_length)
    end

    def self.describe
      Describe.new('hstrlen', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@hash', '@fast' ])
    end
  end
end
```
_listing 8.32 The `HStrLenCommand` class_

This command does not need any new methods from the `RedisHash` class, it obtains the string stored at `field` with the `RedisHash#get` method and uses the Ruby `String#length` method to return its length.

## Refactoring the test utilities

We introduced the `Config` class in this chapter, but there's no way to change the default values. We are going to add the `CONFIG GET` & `CONFIG SET` commands, in order to update config values at runtime and test the `RedisHash` class behavior with both a `List` and a `Dict` as the underlying data structure.

Let's first add the `ConfigCommand` class. The `CONFIG` command is different from all the other commands we've implemented so far in that it supports sub-commands. We are only adding support for the `GET` & `SET` sub-commands here, but the real Redis also supports `CONFIG RESETSTAT` & `CONFIG REWRITE`.

``` ruby
module BYORedis
  class ConfigCommand < BaseCommand

    def call
      if @args[0] != 'SET' && @args[0] != 'GET'
        message =
          "ERR Unknown subcommand or wrong number of arguments for '#{ @args[0] }'. Try CONFIG HELP."
        RESPError.new(message)
      elsif @args[0] == 'GET'
        Utils.assert_args_length(2, @args)
        value = Config.get_config(@args[1].to_sym)
        return RESPBulkString.new(Utils.integer_to_string(value))
      elsif @args[0] == 'SET'
        Utils.assert_args_length_greater_than(2, @args)
        @args.shift # SET
        @args.each_slice(2) do |key, _|
          raise RESPSyntaxError if key.nil? || value.nil?

          Config.set_config(key, value)
        end
      end

      OKSimpleStringInstance
    end

    def self.describe
      Describe.new('config', -2, [ 'admin', 'noscript', 'loading', 'stale' ], 0, 0, 0,
                   [ '@admin', '@slow', '@dangerous' ])
    end
  end
end
```
_listing 8.33 The `ConfigCommand` class_

The version of `CONFIG GET` we implemented is a simplified version of the one in Redis which supports glob-style patterns, with `*`.

With these two new commands, we can now update the config values in our test, which will allow us to lower the value of `hash_max_ziplist_entries` so that we don't have to add 513 items to hash for it to be converted to a `Dict`. Ideally we'll want to run all our tests under different combinations of configuration values.

The problem with the current approach to testing is that we spin up a new server for each test, which adds quite some time to each test as forking a new process and start a Ruby process within it takes some time. We will instead start a single process, and reuse it across our tests.

In order to do so, we need to do a little bit of work to make sure that the state of the server is clean for each tests. For instance, if a tests sends the `BRPOPLPUSH a b 1` command, we want to make sure that if a next test runs within a second, that the first client correctly disconnected.

We also need to make sure that the database is in a clean state, and for that we will implement the `FLUSHDB` command:

``` ruby
module BYORedis
  class FlushDBCommand < BaseCommand

    def initialize(db, args)
      @db = db
      @args = args
    end

    def call
      Utils.assert_args_length(0, @args)
      @db.flush

      OKSimpleStringInstance
    end

    def self.describe
      Describe.new('flushdb', 1, [ 'write' ], 1, -1, 1, [ '@keyspace', '@write', '@slow' ])
    end
  end
end
```
_listing 8.34 The `FlushDBCommend` class_

Let's add the `DB#flush` method:

``` ruby
module BYORedis
  class DB

    # ...

    def initialize
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      flush
    end

    def flush
      @data_store = Dict.new
      @expires = Dict.new
      @ready_keys = Dict.new
      @blocking_keys = Dict.new
      @client_timeouts = SortedArray.new(:timeout)
      @unblocked_clients = List.new
    end

    # ...
  end
end
```
_listing 8.35 The `DB#flush` method_

Ruby makes our lives really easy here, to flush the database, we can simply instantiate a few fresh `Dict`, `List` and `SortedArray` and call it a day, the garbage collector will take care of freeing the memory of the previous ones now that they're not referenced anymore.

We now need to make some changes to the `test_helper.rb` file.

``` ruby
# test_helper.rb

require 'timeout'
require 'stringio'
require 'logger'

ENV['LOG_LEVEL'] = 'FATAL' unless ENV['LOG_LEVEL']

require_relative '../server'

$child_process_pid = nil
$socket_to_server = nil

def restart_server
  kill_child
  $child_process_pid = nil
  start_server
  $socket_to_server = nil
end

def start_server
  if $child_process_pid.nil?

    if !!ENV['DEBUG']
      options = {}
    else
      options = { [ :out, :err ] => '/dev/null' }
    end

    start_server_script = <<~RUBY
    begin
      BYORedis::Server.new
    rescue Interrupt
    end
    RUBY

    $child_process_pid =
      Process.spawn('ruby', '-r', './server', '-e', start_server_script, options)
  end
end

start_server

# Make sure that we stop the server if tests are interrupted with Ctrl-C
Signal.trap('INT') do
  kill_child
  exit(0)
end

require 'minitest/autorun'

def do_teardown
  with_server do |socket|
    socket.write(to_query('FLUSHDB'))
    read_response(socket)
    args = BYORedis::Config::DEFAULT.flat_map do |key, value|
      [ key.to_s, value.to_s ]
    end
    socket.write(to_query('CONFIG', 'SET', *args))
    read_response(socket)
  end
end

class MiniTest::Test
  def teardown
    with_server do
      do_teardown
    end
  rescue Errno::EPIPE, IOError => e
    $socket_to_server&.close
    $socket_to_server = nil
    connect_to_server
    do_teardown
    p "Exception during teardown: #{ e.class }/ #{ e }"
  end
end

def kill_child
  if $child_process_pid
    Process.kill('INT', $child_process_pid)
    begin
      Timeout.timeout(1) do
        Process.wait($child_process_pid)
      end
    rescue Timeout::Error
      Process.kill('KILL', $child_process_pid)
    end
  end
rescue Errno::ESRCH
  # There was no process
ensure
  if $socket_to_server
    $socket_to_server.close
    $socket_to_server = nil
  end
end

MiniTest.after_run do
  kill_child
end

def connect_to_server

  return $socket_to_server if !$socket_to_server.nil? && !$socket_to_server.closed?

  # The server might not be ready to listen to accepting connections by the time we try to
  # connect from the main thread, in the parent process. Using timeout here guarantees that we
  # won't wait more than 1s, which should more than enough time for the server to start, and the
  # retry loop inside, will retry to connect every 10ms until it succeeds
  connect_with_timeout
rescue Timeout::Error
  # If we failed to connect, there's a chance that it's because the previous test crashed the
  # server, so retry once
  p "Restarting server because of timeout when connecting"
  restart_server
  connect_with_timeout
end

def connect_with_timeout
  Timeout.timeout(1) do
    loop do
      begin
        $socket_to_server = TCPSocket.new 'localhost', 2000
        break
      rescue StandardError => e
        $socket_to_server = nil
        sleep 0.2
      end
    end
  end
  $socket_to_server
end

def with_server
  server_socket = connect_to_server

  yield server_socket

  server_socket.close
end
```
_listing 8.36 Updates to the `test_helper.rb` file_

Bare with me for a minute, I know that global variables are frowned upon, but we're only using them to make our lives easier.
I would not describe global variables as something to never use, but instead, as something to be extremely careful with. They can indeed become really problematic if they're used a lot throughout a codebase, especially if the value they hold changes a lot. Doing so could require a lot of headache . By using a global variable, we make it easier to maintain a single instance of the child process, without having to create a class, instantiate it, and burying the logic, what we want is actually not that much:

- At the beginning of the test, spawn a new process in which we start the server, keep the pid of this process
- For each test, create a socket and connect it to the server. At the end of the test, disconnect the socket
- If the server crashes, we want to restart it so that subsequent tests work
- At the end of each test, we want to run the `FLUSHDB` command so that next tests start with a clean database

This big refactor of the test context now allows us to use the following helper. Using this approach, which creates many more tests, would have been really slow with the "start a new process for each test approach", but now, each of these tests only generates a few round trips to the server, which is really fast, in the sub millisecond range.

``` ruby
# test_helper.rb
def test_with_config_values(combinations)
  # This line goes from a hash like:
  # { config_1: [ 'config_1_value_1', 'config_2_value_2' ],
  #   config_2: [ 'config_2_value_1', 'config_2_value_2' ] }
  # to:
  # [ [ [:config_1, "config_1_value_1"], [:config_1, "config_2_value_2"] ],
  #   [ [:config_2, "config_2_value_1"], [:config_2, "config_2_value_2"] ] ]
  config_pairs = combinations.map { |key, values| values.map { |value| [ key, value ] } }

  # This line combines all the config values into an array of all combinations:
  # [ [ [ :config_1, "config_1_value_1"], [:config_2, "config_2_value_1" ] ],
  #   [ [ :config_1, "config_1_value_1"], [:config_2, "config_2_value_2" ] ],
  #   [ [ :config_1, "config_2_value_2"], [:config_2, "config_2_value_1" ] ],
  #   [ [ :config_1, "config_2_value_2"], [:config_2, "config_2_value_2" ] ] ]
  all_combinations = config_pairs[0].product(*config_pairs[1..-1])

  # And finally, using the Hash.[] method, we create an array of hashes and obtain:
  #  [ { :config_1=>"config_1_value_1", :config_2=>"config_2_value_1" },
  #    { :config_1=>"config_1_value_1", :config_2=>"config_2_value_2" },
  #    { :config_1=>"config_2_value_2", :config_2=>"config_2_value_1" },
  #    { :config_1=>"config_2_value_2", :config_2=>"config_2_value_2" } ]
  all_combination_hashes = all_combinations.map { |pairs| Hash[pairs] }

  all_combination_hashes.each do |config_hash|
    with_server do |socket|
      socket.write(to_query('FLUSHDB'))
      resp = read_response(socket)
      assert_equal("+OK\r\n", resp)

      config_parts = config_hash.flat_map { |key, value| [ key.to_s, value.to_s ] }
      socket.write(to_query('CONFIG', 'SET', *config_parts))
      resp = read_response(socket)
      assert_equal("+OK\r\n", resp)
    end

    yield
  end
end
```
_listing 8.37 the `test_with_config_values` helper in `test_helper.rb`_

You can find all the tests on GitHub, but here is an example of the tests we can now write with the `test_with_config_values` helper:

``` ruby
describe 'HVALS' do
  it 'returns an array of all the values in the hash' do
    test_with_config_values(hash_max_ziplist_entries: [ '512', '1' ]) do
      assert_command_results [
        [ 'HSET h f1 v1 f2 v2', ':2' ],
        [ 'HVALS h', unordered([ 'v1', 'v2' ]) ],
      ]
    end
  end
end
```
_listing 8.38 Example of a test using `test_with_config_values` for the `HVALS` command_

The implementation of the `HVALS` command is different depending on whether the `RedisHash` instance is using a `List` or `Dict` to store the key/value pairs, so ideally we'd want to test both cases. Given that the test themselves are identical, at the end of the day, we do want to test the same output, but with two different implementation, it would be really repetitive to write the tests twice.

This approach allows us to wrap the tests we want to run with the different config values, and the helper will use `FLUSHDB` and `CONFIG SET` to prepare the context before running the tests.


## Conclusion

As usual, you can find the [code on GitHub][code-github-link]. In the next chapter we will implement Sets, see you there!

[redis-doc-hashes]:https://redis.io/commands#hash
[redis-conf-file-hash-max-ziplist-entries]:https://github.com/redis/redis/blob/6.0.0/redis.conf#L1481
[redis-conf-file-hash-max-ziplist-value]:https://github.com/redis/redis/blob/6.0.0/redis.conf#L1482
[chapter-7-appendix-a]:/post/chapter-7-adding-list-commands/#appendix-a-ziplist-deep-dive
[chapter-7]:/post/chapter-7-adding-list-commands/
[chapter-6]:/post/chapter-6-building-a-hash-table/
[chapter-5]:/post/chapter-5-redis-protocol-compatibility/
[redis-doc-scan]:http://redis.io/commands/scan
[redis-src-dict-scan-doc]:https://github.com/antirez/redis/blob/6.0.0/src/dict.c#L778-L861
[redis-src-string2ll]:https://github.com/redis/redis/blob/6.0.0/src/util.c#L360-L424
[code-github-link]:https://github.com/pjambet/redis-in-ruby/tree/master/code/chapter-8
[ruby-doc-big-decimal-method]:https://ruby-doc.org/stdlib-2.7.1/libdoc/bigdecimal/rdoc/Kernel.html#BigDecimal-method
[c-std-strtold]:http://www.cplusplus.com/reference/cstdlib/strtold/
[floating-point-errors]:https://0.30000000000000004.com/
[wikipedia-twos-complement]:https://en.wikipedia.org/wiki/Two%27s_complement
[redis-doc-hash-commands]:https://redis.io/commands#hash
[redis-doc-scan-command]:http://redis.io/commands/scan
[redis-doc-incrby]:http://redis.io/commands/incrby
[redis-doc-incrbyfloat]:http://redis.io/commands/incrbyfloat
[redis-conf-max-items]:https://github.com/redis/redis/blob/6.0.0/redis.conf#L1481
[redis-conf-max-value]:https://github.com/redis/redis/blob/6.0.0/redis.conf#L1482
[ruby-doc-string-ord]:https://ruby-doc.org/core-2.7.1/String.html#ord-method
[wikipedia-long-double]:https://en.wikipedia.org/wiki/Long_double
[ruby-doc-float]:https://ruby-doc.org/core-2.7.1/Float.html
[ruby-doc-bigdecimal]:https://ruby-doc.org/stdlib-2.7.1/libdoc/bigdecimal/rdoc/BigDecimal.html
[wikipedia-e-notation]:https://en.wikipedia.org/wiki/Scientific_notation#E_notation
