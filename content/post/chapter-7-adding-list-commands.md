---
title: "Chapter 7 Adding List Commands"
date: 2020-10-02T17:25:58-04:00
lastmod: 2020-10-02T17:26:03-04:00
draft: false
comment: false
keywords: []
summary: "In this chapter we add support for a new data type, Lists. We implement all the commands related to lists, such as LPUSH, LRANGE & LLEN."
---

## What we'll cover

So far we've mainly worked with Strings, and arrays of Strings. A command is received as an array of Strings. `GET` accepts a String and return a String, or a nil String. `SET` works with two Strings, a key and a value. We've also handled a few integers here and there, like for the result value of the `TTL` & `PTTL` commands.

We are going to add full support for List related commands in this chapter.


## Redis Data Types

When it comes to key/value pairs stored in the main keyspace, that is, the `@data_store` `Dict` instance, Redis supports [other data types][redis-data-types-doc] beside Strings:

- Lists: An ordered collection of one or more Strings, sorted by order of insertion.
- Sets: An unordered collection of unique Strings
- Sorted Sets: An ordered collection of unique Strings. Strings are always added with a score, a float value, and the set orders them by their score. Uniqueness is enforced for the String value, multiple elements can have the same score. An example would be a list of user names, sorted by their age. The usernames are unique, but not the age: `{ <'pierre', 30>, <'james', 31>, <'jane', 32>, <'john', 32> }`
- Hashes: An unordered collection of key/value pairs, where keys are unique. Keys and values are Strings. This is essentially the same data type we implemented in [Chapter 6][chapter-6].
- Bit arrays (or bitmaps): This one is slightly different than the other data types as it uses Strings under the hood, but through dedicated commands allows users to perform operations using said Strings as arrays of bits. An example of such command is `BITOP AND dest-key k1 k2`, which will perform an `AND` operation between the values at keys `k1` and `k2` and store the value in `dest-key`.
- HyperLogLogs: A HyperLogLog (HLL for short) is a data structure optimized to trade space for efficiency when counting elements in a set. The main operation of an HLL is to count the number of unique elements. The result might not be exact, but the storage used by HLLs will always be extremely small regardless of how many items were counted. Similarly to Bit arrays, HLLs are implemented using Strings. A Set could be used to achieve a similar count operation, the difference being that the set will store each element, allowing it to return an accurate count, at the cost of its memory footprint being proportional to the size of the set. A HyperLogLog in Redis cannot grow larger than 12,288 (12k) bytes.
- Streams: An append-only collection, conceptually similar to a log structure. Elements are key/value pairs and are by default ordered by the time they were added to the stream. Streams are a fairly complicated data structure, well documented on [the official website][redis-streams-doc] and will not be covered in this book.

_Note about empty collections: Lists, Sets, Sorted Sets & Hashes cannot be empty, once the last element is deleted, the collection is removed from the keyspace._

_Note about integers: Redis handles integers in a way that can seem confusing. The type of a value is never specified, it is always a String. Whether you send the command `SET a-string-key a-string` or `SET an-integer-key 100`, the value is sent as a string. Internally Redis knows whether or not what was received as a String can be treated as an integer. In some cases it will optimize the way it stores the data based on whether or not it is an integer. There are even commands that will only work if a value can be treated as a number, such as the [INCR][redis-doc-incr] command._

**List related terms**

Let's clarify some list related terms first. A list is often represented as a left to right sequence of elements. The left-most element is called the _head_ and the right-most element is called the _tail_.

It is also common to use the term "tail" to describe all the elements after the head. According to this definition, the tail is another list, empty if the list itself has zero or one element, non empty otherwise. This is not the definition we'll use throughout this book.

An empty list has no _head_ and no _tail_, or it could be said that they're both nil. For a one-element list, the _head_ and the _tail_ are equal.

Elements can be added or removed from either side, left or right, as well as from within the list. Adding elements is commonly referred to as a _push_ and removing elements as a _pop_. The terms _shift_ and _unshift_ are also used, where _shift_ commonly means popping an element to the left of the list and _unshift_ pushing an element to the left.

Finally, the terms _append_ & _prepend_ can also be used. In this context _append_ is similar to a right _push_, and _prepend_ is similar to a left _push_, or _unshift_.

**Lists and other data structures**

Lists can be used as the backing data structures for various more specific structures such as [queues][wikipedia-queues] and [stacks][wikipedia-stacks].

A list can be used as a queue by implementing the push operation as a "left push", that is, new elements are added to the left of the list, and the new element becomes the head of the list. Elements are removed with a "right pop", that is, elements are removed from the right of the list, the tail is removed and the element that preceded the tail becomes the new tail. This satisfies the **F**irst **I**n **F**irst **O**ut constraint of a queue.

A list can be used as a stack by implementing the push operation as a "right push", that is, new elements are added to the right of the list, and the new element becomes the new tail. Elements are removed as a "right pop", similarly to how we described a queue above. This satisfies the **L**ast **I**n **F**irst **O**ut constraint of a stack.

The choice of right and left in these two examples is arbitrary but also very common within western countries where languages are read and written left to right.

Identical data structures could be implemented by reversing the side of each operations. A queue could be implemented by pushing new elements with a right push and removing them with a left pop. A stack could be implemented by pushing new elements with a left push and removing them with a left pop.

**Redis List commands**

Redis supports [eighteen list related commands][list-commands-docs]:

- LINDEX: Gets an element by its index
- LINSERT: Inserts an element
- LLEN: Returns the length of the list
- LPOP: Removes an element on the left of the list
- LPOS: Returns the position of one or elements
- LPUSH: Adds an element to the left of the list, creating it if needed
- LPUSHX: Same as LPUSH, but does not create a new list if it doesn't already exist
- LRANGE: Returns elements from the list
- LREM: Removes one or more elements from the list
- LSET: Replaces an element in a list
- LTRIM: Keeps a subset of the list
- RPOP: Removes an element on the right of the list
- RPOPLPUSH: Removes an element to the right of a list and adds it to the left of another list
- RPUSH: Adds an element to the right of the list, creating it if needed
- RPUSHX: Same as RPUSH, but does not create a new list if it doesn't already exist
- BLPOP: Blocking variant of LPOP
- BRPOP: Blocking variant of RPOP
- BRPOPLPUSH: Blocking variant of RPOPLPUSH


## How does Redis do it

We've actually already implemented a list in the previous chapter to handle hash collisions and store multiple pairs in the same bucket.

We could use a similar structure to implement all the list commands detailed previously but the list we used for the `Dict` implementation has a few limitations with regards to performance that would be problematic in some cases.

**The need for a doubly linked list**

The following is what we used to create a list of entries in a bucket:

``` ruby
module BYORedis
  class DictEntry

    attr_accessor :next, :value
    attr_reader :key

    def initialize(key, value)
      @key = key
      @value = value
      @next = nil
    end
  end
end
```
_listing 7.1 The linked list used for the `Dict` class_

We used the `class` syntax to control which getters and setters we wanted to be generated, with `attr_accessor` & `attr_reader`, but it could be simplified as the following:

``` ruby
Node = Struct.new(:value, :next)
```
_listing 7.2 The Node struct_

Given the nature of Ruby's type system, `value` could be anything, and could therefore be a key/value pair if we passed a two-element array such as `[ 'a-key', 'a-value' ]`.

This implementation is a "singly linked list". Each node has one "link" to its successor in the list, the `next` attribute in the previous example. If the link value is empty, then there's no successor and the element is the last one in the list.

One problem with this approach is that adding or deleting an element at the end of list requires to iterate through the whole list, which will become slower and slower as the list grows. We covered this problem in the previous chapter when we explained the need for growing the hash table.

A solution to this problem is to use what is commonly called a sentinel node, essentially a way to hold a reference to the tail of the list, for easy access to it, this would look like this:

``` ruby
class List
  def initialize
    @head = nil
    @tail = nil
  end

  def prepend(element)
    # The new node is created with its next value set to @head, if @head was nil, the list was
    # empty, and new_node is now the head, its next value will also be nil.
    # If @head was not nil, new_node now points to what used to be the head
    new_node = Node.new(element, @head)
    # The head of the list is now the new node
    @head = new_node
    if @tail.nil?
      # If @tail is nil, the list was empty, and has now one element, the head and the tail are
      # the same node
      @tail = new_node
    end

    new_node
  end

  def append(element)
    # new_node will be the new tail, so its next value must be nil
    new_node = Node.new(element, nil)
    if @tail
      @tail.next = new_node
    else
      # If the list is empty, both @head and @tail are nil
      @head = @tail
    end

    # Update the @tail value to now point to new_node
    @tail = new_node
    new_node
  end
end
```
_listing 7.3 A singly linked list with prepend & append operations_

The append operation is now O(1) with this implementation, it will require the same amount of steps regardless of the size of the list, it runs in constant time.

While this is a big win, another common list operation, removing the tail, also known as right pop, cannot be optimized from O(n) to O(1) with this approach:

``` ruby
def right_pop
  # If @tail is nil, the list is empty
  return nil if @tail.nil?

  # When the list has only one element, @head will be equal to @tail. In this case, popping from
  # the right or the left is effectively the same, and we can do so by setting both variables to
  # nil. The list is now empty
  if @head == @tail
    @head = nil
    @tail = nil

    nil
  elsif @head.next == @tail
    # If the second element, the one pointed at by @head.next is equal to @tail, the list has
    # only two elements
    @head.next = nil
    tail = @tail
    @tail = @head

    tail
  else
    # We now need a reference to the element pointing at @tail, so we can set its next value to
    # nil, instead of @tail, and set @tail to it, doing so will require iterating through all
    # the element in the list:
    cursor = @head.next

    # The number of steps this loop will go through is proportional to the size of the list,
    # making this method O(n)
    while cursor.next != @tail
      cursor = cursor.next
    end

    # We exited the loop, so the condition cursor.next == @tail is now true
    cursor.next = nil
    tail = @tail
    @tail = cursor

    tail
  end
end
```
_listing 7.4 right_pop operation for a singly linked list_

We could keep going down this road, and optimize the `right_pop` method by adding a third instance variable to the `List` class, one that always holds a value the second to last node. With such a value we would not have to iterate through the list in the `else` branch. Maintaining the value would require a few more steps in the `append`, `prepend` & `pop` operations to always keep it pointing at the second to last element.

This would still not allow us to efficiently implement all operations. The `LRANGE` command is used to return elements in the list, given two values `start` & `stop`. `LRANGE list 0 1` means "return the first two items of the list `list`, the one at index 0 and the one at index 1. `LRANGE list 0 3` means "return the first four items, the ones at indices 0, 1, 2 & 3, or put differently, all items between index 0 and 3. But the `LRANGE` command also supports a different syntax, with negative indices, to count elements starting from the tail. -1 means the last element in the list, -2, the second to last, and so on. Using this syntax we can return all the elements in the list with `LRANGE 0 -1`. We could also return the last three elements of the list with `LRANGE -3 -1`.

The main benefit of negative indices is that you don't need to know the size of the list to return the last n elements. In the previous examples, if we knew the list had 10 elements, we could have used `LRANGE 7 9` to return the last three elements. But if an 11th element is added to the list, we would need to now send `LRANGE 8 10`. Using the negative index syntax, we can always use `LRANGE -3 -1`, regardless of the size of the list.

As we've already discussed a few times, Redis should be able to handle really big data sets, in the case of lists, ideally ones with thousands, and even millions of elements. Being able to return the last n elements of large lists should ideally not require to iterate through the whole list, which, sadly, with a singly linked list, we'd be forced to do.

A solution to this problem is to use a doubly linked list, one where each node contains a link to the next element but also one to the previous element. It's important to address right away that this approach comes with an important trade-off, we're now storing twice as much metadata per node, in exchange for potential speedups.

A doubly linked list can be implemented with the following Ruby struct:

``` ruby
DoublyLinkedNode = Struct.new(:value, :prev_node, :next_node)
```
_listing 7.5 The Doubly Linked List Struct_

We can now use this `DoublyLinkedNode` class in combination with the sentinel approach to implement the `LRANGE` command in an efficient manner:

``` ruby
def range(start, stop)
  # The complete range method, which handles all the possible edge cases is implemented later
  # on, we're only showing a simplified version here, to highlight the benefits of the doubly
  # linked list
  # The "real" method should handle the case where only one of the two bounds is negative, such
  # as LRANGE 0 -1
  list_subset = []
  if start < 0 && stop < 0 && start <= stop
    current_index = -1
    cursor = @tail
    while current_index >= start
      if current_index <= stop
        list_subset.prepend(cursor.value)
      end
      cursor = cursor.prev_node
      current_index -= 1
    end
  end

  list_subset
end
```
_listing 7.6 A partial range method implementation leveraging a doubly linked list_

As detailed in the comment, the previous example is not complete, but it shows how we can use the `prev_node` field to iterate from right to left instead of being forced to iterated from left to right with a singly linked list.

We can also simplify the `right_pop` method. The `prev_node` field on each node removes the need for a third instance variable to hold the second to last node.

``` ruby
def right_pop
  return if @tail.nil?

  tail = @tail
  @tail = @tail.prev_node
  if @tail
    @tail.next_node = nil
  else
    # If the list had only element, we removed it and the list is now empty, @tail is now nil
    # and we also need to set @head to nil
    @head = nil
  end

  tail
end
```
_listing 7.7 The right_pop operation leveraging a doubly linked list_

Redis uses a doubly linked list approach, for the reasons we mentioned above, but it uses a more optimized version to reduce memory usage.

On a 64-bit CPU, which is what most modern CPUs use, a pointer is a 64-bit integer, which uses 8 bytes (8 * 8 = 64 bits). So regardless of what the node actually stores, you need two pointers per node, one for the next node, one for the previous node, that's 16 bytes, per node. In a common case where each node would store an integer, which in C can have different sizes, as small as one byte and as big as 8, it means that even if you're storing a large integer, one that takes 8 bytes, you end up with 16 bytes of metadata, for 8 bytes of actual data. The problem is potentially even worse if you're storing smaller integer, say that the integers stored are small, 2 bytes for instance, you would end up with a node of 18 bytes where only 2 bytes are the actual data. This problem might not seem too bad with small lists but as the list grows, it would cause the server to waste a lot of memory to store these pointers.

Redis uses two data structures to optimize for this problem. At the top level, it uses a structure it calls Quicklist, which is a doubly linked list where each element is a Ziplist. A Ziplist is a compact optimization of a doubly linked list that does not suffer from the metadata storage problem we described.

Briefly summarizing a Ziplist is a complicated task, but it is conceptually close to how arrays work. It allocates a contiguous chunk of memory with `malloc(3)`. A key difference is that all elements in an array have the same size, so there's not need for metadata beyond the size of the array. If you're trying to get the 5th element in an array, you do so by multiplying the size of an element in the array by five, and use the value as an offset from the beginning of the array, and the value you found at that location is the fifth element in the array.

Elements can have different sizes with a Ziplist, small integers, with a value between 0 and 12 will only use one byte, whereas larger integers, with values greater than 2,147,483,647 - the maximum value of an int32_t, which uses 4 bytes - will require 9 total bytes, 8 bytes for the value, an int64_t, and one byte of metadata. These variations in element sizes, which allows for greater optimizations, small elements take a small amount of space, means that the list needs to store some metadata to allow traversal of the list and access to the different elements. [Appendix A][appendix-a] explains in more details about Ziplist works, but does not provide a Ruby implementation.

The Ziplist approach also shares some similarities with how SQL database systems organize data into pages. For reference [this page][postgres-page-layout] of the Postgres documentation explains in details the page layout. A key difference is that pages have a fixed size, whereas a ziplist will grow until it reaches its max size. The similarity is in the fact that they both operate on a set amount of memory which starts with some metadata followed by the actual data, organized in a compact sequence.

The reason why Redis does not exclusively uses Ziplists is because as Ziplists grow, update operations such as adding or removing elements, become more and more costly. This mixed approach allows Redis to benefit from the best of both worlds, the quicklist maintains a list of ziplists, which actually hold the elements in the list. Each ziplist has a maximum size, and when it reaches it, a new ziplist node is created in the parent quicklist.

This implementation is fairly complex so for the sake of simplicity we will use the basic doubly linked list approach shown above in this chapter.

_The quicklist/ziplist approach might be implemented in a future chapter_

It is now time to add support for all the list commands to our server.

## Handling Incorrect Types

Now that our server is about to handle more than one type, Strings and Lists, we need to consider the type of each key/value pair when processing commands. For instance, if a pair is first created as a String, with the `SET` command, calling `LLEN` on it, which attempts to return the length of a List, is invalid:

```
127.0.0.1:6379> SET a-string a-value
OK
127.0.0.1:6379> LLEN a-string
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

Some commands work regardless of the type of the pair, such as `TTL` and `PTTL`:

```
127.0.0.1:6379> TTL a-string
(integer) -1
127.0.0.1:6379> RPUSH a-list 1
(integer) 1
127.0.0.1:6379> TTL a-list
(integer) -1
```

Redis has a [`TYPE`][redis-doc-type-command] command that can be used to return the type of a pair, the possible return values are: `string`, `list`, `set`, `zset`, `hash` and `stream`. If the key does not exist, it returns `none`.

Let's add support for the `TYPE` command.

So far we've repeated some code between all the commands, let's improve this by defining a `BaseCommand` class:

``` ruby
module BYORedis
  class BaseCommand

    def initialize(data_store, expires, args)
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      @data_store = data_store
      @expires = expires
      @args = args
    end

    def call
      raise NotImplementedError
    end
  end
end
```
_listing 7.8 The BaseCommand class, parent of all command classes_

We can now define the `TypeCommand` class, which inherits from `BaseCommand`:

``` ruby
module BYORedis
  class TypeCommand < BaseCommand

    def call
      Utils.assert_args_length(1, @args)

      key = @args[0]
      ExpireHelper.check_if_expired(@db, key)
      value = @db.data_store[key]

      case value
      when nil
        RESPSimpleString.new('none')
      when String
        RESPSimpleString.new('string')
      else
        raise "Unknown type for #{ value }"
      end
    end
  end
end
```
_listing 7.9 The TypeCommand class_

The command is not that useful for now, it either returns `none` or `string`. We will add a new branch to the `case/when` statement when we add the `List` type.

The next step is to add it to the `COMMANDS` constant in the `Server` class:

``` ruby
# ...
require_relative './pttl_command'
require_relative './type_command'
require_relative './command_command'

module BYORedis

  class Server

    COMMANDS = Dict.new
    COMMANDS.set('command', CommandCommand)
    # ...
    COMMANDS.set('type', TypeCommand)

    MAX_EXPIRE_LOOKUPS_PER_CYCLE = 20
    # ...
  end
end
```
_listing 7.10 Updates to the Server class to support the TYPE command_

All the command classes we've created need `describe` class method, which is in turn used by the `CommandCommand` class to return the description of the command. The approach worked well so far but it is a bit verbose, and as we add more commands, becomes more and more annoying.

Let's simplify this process with a struct, `Describe`, defined in `BaseCommand`:

``` ruby
module BYORedis
  class BaseCommand

    Describe = Struct.new(:name, :arity, :flags, :first_key_position, :last_key_position,
                          :step_count, :acl_categories) do
      def serialize
        [
          name,
          arity,
          flags.map { |flag| RESPSimpleString.new(flag) },
          first_key_position,
          last_key_position,
          step_count,
          acl_categories.map { |category| RESPSimpleString.new(category) },
        ]
      end
    end
  end

  # ...
end
```
_listing 7.11 The Describe Struct defined in the BaseCommand class_

We need to update the `CommandCommand` class as well, to use this new class:

``` ruby
module BYORedis
  class CommandCommand < BaseCommand

    # ...

    def call
      RESPArray.new(SORTED_COMMANDS.map { |command_class| command_class.describe.serialize })
    end

    def self.describe
      Describe.new('command', -1, [ 'random', 'loading', 'stale' ], 0, 0, 0,
                   [ '@slow', '@connection' ])
    end
  end
end
```
_listing 7.12 The CommandCommand class using the Describe Struct_

We can now define the `self.describe` method in the `TypeCommand` with the following:

``` ruby
module BYORedis
  class TypeCommand < BaseCommand

    ...

    def self.describe
      Describe.new('type', 2, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@keyspace', '@read', '@fast' ])
    end
  end
end
```
_listing 7.13 The TypeCommand class using the Describe struct_

We also need to update all the existing command classes: `DelCommand`, `GetCommand`, `SetCommand`, `TtlCommand`, & `PTtlCommand`. Doing so is fairly mechanical and is therefore not included here. You can find all the code from this chapter on [GitHub][code-github]

## Adding List Commands

Now that we've explored the landscape of lists in Redis, it's time to add these functionalities to our server. We're going to start with the two commands that allow us to create a list, `LPUSH` & `RPUSH`.

### Creating a list with LPUSH & RPUSH

There are two commands that allow clients to create a list, `LPUSH` & `RPUSH`. If the given key does not already exist and only one new element is passed to the command, they behave identically, they create a list and add the given element to it. The difference with this two commands occurs when the list already exists and has some elements in it or if multiple elements are passed. `LPUSH` adds them to the **L**eft and `RPUSH` adds them to the **R**ight.

Both commands accepts one or more values,

```
127.0.0.1:6379> LPUSH a-list a b c d
(integer) 4
127.0.0.1:6379> LRANGE a-list 0 -1
1) "d"
2) "c"
3) "b"
4) "a"
```

In the previous example we can see that `LPUSH` added four elements to the newly created list `a-list`. It first added `a`, then `b` to the left, as the new head, and then `c` and `d` to the left as well, the final list is `[ 'd', 'c', 'b', 'a' ]`.

We can try the same example but with `RPUSH`:

```
127.0.0.1:6379> DEL a-list
(integer) 1
127.0.0.1:6379> RPUSH a-list a b c d
(integer) 4
127.0.0.1:6379> LRANGE a-list 0 -1
1) "a"
2) "b"
3) "c"
4) "d"
```

The subsequent elements, `b`, `c` & `d` were added as new tails, to the right, and the final list is: `[ 'a', 'b', 'c', 'd' ]`.

Finally, if the key already exists and is not a List, Redis returns an error:

```
127.0.0.1:6379> SET a b
OK
127.0.0.1:6379> LPUSH a a
(error) WRONGTYPE Operation against a key holding the wrong kind of value
127.0.0.1:6379> RPUSH a a
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

Because most of the list commands will be small and related to each other, instead of creating one file per command as we've done so far, this time we'll create a single file `list_commands.rb` and define all the command classes in there. Let's start with `LPUSH`.

Before creating the `LPushCommand`, we will start by reorganizing how the `Server` class and the different command classes interact with each other. So far we've passed the `@data_store` and `@expires` `Dict` instances, as well as the command arguments, to each command class.

We will need to create more data structures to handle various list related commands, so to simplify this, we will wrap all these data structures inside a `DB` class. It's worth noting that this approach is conceptually similar to how the code is organized in the Redis codebase. Redis defines a [C struct for a DB type][redis-source-db-type].

The process of a looking up a list is slightly different than it is from a String. For commands such as `RPUSH` & `LPUSH`, we want to see if a list exists for the given key, but if it doesn't exist, we want to create a new list. Let's create a method in the `DB` class for this purpose:

``` ruby
module BYORedis
  class DB

    attr_reader :data_store, :expires
    attr_writer :ready_keys

    def initialize
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      @data_store = Dict.new
      @expires = Dict.new
    end

    def lookup_list(key)
      list = @data_store[key]
      raise WrongTypeError if list && !list.is_a?(List)

      list
    end

    def lookup_list_for_write(key)
      list = lookup_list(key)
      if list.nil?
        list = List.new
        @data_store[key] = list
      end

      list
    end
  end
end
```
_listing 7.14 The new DB class_

We'll define the `WrongTypeError` shortly, when we add a few other utility methods.

Let's now refactor the `BaseCommand` class to use the DB class:

``` ruby
module BYORedis
  class BaseCommand

    # ...

    def initialize(db, args)
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      @db = db
      @args = args
    end
  end
end
```
_listing 7.15 Updates to the BaseCommand class to support the DB class_

And finally, let's update the `Server` class:

``` ruby
# ...
module BYORedis

  class Server
    # ...
    def initialize
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL

      @clients = Dict.new
      @db = DB.new
      @server = TCPServer.new 2000
      # ...
    end

    # ...
    def handle_client_command(command_parts)
      @logger.debug "Received command: #{ command_parts }"
      command_str = command_parts[0]
      args = command_parts[1..-1]

      command_class = COMMANDS[command_str.downcase]

      if command_class
        command = command_class.new(@db, args)
        command.call
      else
        # ...
      end
    end
    # ...
  end
end
```
_listing 7.16 Updates to the Server class to support the DB class_

Both `LPUSH` & `RPUSH` accept at least two arguments, the key and one or more elements to add. The validation of the number of arguments is very similar across commands, so let's create a helper method for that. Let's first create a `Utils` module and add it there:

``` ruby
module BYORedis

  InvalidArgsLength = Class.new(StandardError) do
    def resp_error(command_name)
      RESPError.new("ERR wrong number of arguments for '#{ command_name }' command")
    end
  end
  WrongTypeError = Class.new(StandardError) do
    def resp_error
      RESPError.new('WRONGTYPE Operation against a key holding the wrong kind of value')
    end
  end

  module Utils
    def self.assert_args_length(args_length, args)
      if args.length != args_length
        raise InvalidArgsLength, "Expected #{ args_length }, got #{ args.length }: #{ args }"
      end
    end

    def self.assert_args_length_greater_than(args_length, args)
      if args.length <= args_length
        raise InvalidArgsLength,
              "Expected more than #{ args_length } args, got #{ args.length }: #{ args }"
      end
    end
  end
end
```
_listing 7.17 The new Utils module_

We can now call `Utils.assert_args_length_greater_than(1, @args)` to validate the number of arguments for the `LPUSH` & `RPUSH` commands. Doing so will raise a `InvalidArgsLength` exception. Let's add a `rescue` statement for this exception, as well as for the `WrongTypeError` we used earlier in the `DB` class. Let's do this in the `BaseCommand` so that all classes benefit from it:

``` ruby
module BYORedis
  class BaseCommand

    # ...
    def execute_command
      call
    rescue InvalidArgsLength => e
      @logger.debug e.message
      command_name = self.class.describe.name.upcase
      e.resp_error(command_name)
    rescue WrongTypeError => e
      e.resp_error
    end

    def call
      raise NotImplementedError
    end
  end
end
```
_listing 7.18 Updates to the BaseCommand class to catch shared exceptions_

This is an application of the [template method pattern][template-method-pattern]. The base class defines logic around the `call` method, and it is up to the subclasses to define the actual logic of the `call` method. We need to update the `Server` class to now call the `execute_command` method instead:

``` ruby
module BYORedis
  class Server

    # ...

    def handle_client_command(command_parts)
      @logger.debug "Received command: #{ command_parts }"
      command_str = command_parts[0]
      args = command_parts[1..-1]

      command_class = COMMANDS[command_str.downcase]

      if command_class
        command = command_class.new(@db, args)
        command.execute_command
      else
        # ...
      end
    end
  end
end
```
_listing 7.19 Updates to the Server class to use the execute_command method_

We already know we are going need a very similar feature soon, for the `RPUSH` command, so we're first defining a set of shared methods:

``` ruby
module BYORedis
  module ListUtils

    def self.common_lpush(list, elements)
      elements.each { |element| list.left_push(element) }
      RESPInteger.new(list.size)
    end

    def self.common_rpush(list, elements)
      elements.each { |element| list.right_push(element) }
      RESPInteger.new(list.size)
    end

    def self.common_find(args)
      Utils.assert_args_length_greater_than(1, args)

      yield args[0]
    end

    def self.find_or_create_list(db, args)
      common_find(args) do |key|
        db.lookup_list_for_write(key)
      end
    end
  end
end
```

With all this refactoring out of the way, we can now define the `LPushCommand`:

``` ruby
require_relative './list'

module BYORedis

  class LPushCommand < BaseCommand
    def call
      list = ListUtils.find_or_create_list(@db, @args)
      values = @args[1..-1]
      ListUtils.common_lpush(list, values)
    end

    def self.describe
      Describe.new('lpush', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write','@list', '@fast' ])
    end
  end
end
```
_listing 7.20 Adding list_commands.rb and the `LPushCommand` class_

We first use `Utils.assert_args_length_greater_than` to validate that we have at least two arguments. We then lookup a list, creating it if doesn't exist and failing if the key already exists and is of a different type. Finally, we iterate over all the remaining arguments and push them to the left of the list with `list.left_push` and return the size of list.

There's a major problem, the `List` class does not exist yet! Let's address this now:

``` ruby
module BYORedis

  class List

    ListNode = Struct.new(:value, :prev_node, :next_node)

    attr_accessor :head, :tail, :size

    def initialize
      @head = nil
      @tail = nil
      @size = 0
    end

    def left_push(value)
      new_node = ListNode.new(value, nil, @head)

      if @head.nil?
        @tail = new_node
      else
        @head.prev_node = new_node
      end

      @head = new_node
      @size += 1
    end
  end
end
```
_listing 7.21 The `List` class_

The class defines a doubly linked list, similar to what we shown earlier in the chapter. The list is initialized with a size of 0, and nil values for `@head` and `@tail`. The `left_push` method behaves differently depending on whether the list was empty or not. We start by creating a new node, with the given value, and setting the `prev_node` attribute to `nil`, given that the new element will be the new head, it does not have a previous element in the list. The next element is set to `@head`, which may be `nil` if the list was empty.

If the list was empty, which we check with `if @head.nil?`, then the new element is also the tail of the list, and we update the `@tail` attribute as such. Otherwise, we update the old head's previous node, which is still the value held by `@head`, to the new node. In other words, the old head now has a previous element in the list.

Finally, we update `@head` to the new node and increment the size of the list.

We need to require the `list_commands` file in the `server.rb` file as well as adding `LPushCommand` to the `COMMANDS` constant:

``` ruby
# ...
require_relative './list_commands'
# ...
module BYORedis

  class Server

    COMMANDS = Dict.new
    COMMANDS.set('command', CommandCommand)
    COMMANDS.set('del', DelCommand)
    COMMANDS.set('get', GetCommand)
    COMMANDS.set('set', SetCommand)
    COMMANDS.set('ttl', TtlCommand)
    COMMANDS.set('pttl', PttlCommand)
    COMMANDS.set('lpush', LPushCommand)
    # ...
  end
end
```
_listing 7.22 Updates to the Server class to support the LPUSH command_

Now that lists can be added to the keyspace, let's handle `List` values in the `ListCommand` class:

``` ruby
module BYORedis
  class TypeCommand < BaseCommand

    def call
      # ...
      case value
      when nil
        RESPSimpleString.new('none')
      when String
        RESPSimpleString.new('string')
      when List
        RESPSimpleString.new('list')
      else
        raise "Unknown type for #{ value }"
      end
    end
    # ...
  end
end
```
_listing 7.23 Update to the TypeCommand to support the List type_

The `RPushCommand` is very similar to the `LPushCommand`:

``` ruby
module BYORedis
  # ...

  class RPushCommand < BaseCommand
    def call
      list = ListUtils.find_or_create_list(@db, @args)
      values = @args[1..-1]
      ListUtils.common_rpush(list, values)
    end

    def self.describe
      Describe.new('rpush', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@list', '@fast' ])
    end
  end
end
```
_listing 7.24 The RPushCommand class_

The class is almost identical to `LPushCommand`, except that we call `right_push` on the list instead, so let's add this method to the `List` class:

``` ruby
module BYORedis

  class List

    # ...

    def right_push(value)
      new_node = ListNode.new(value, @tail, nil)

      if @head.nil?
        @head = new_node
      else
        @tail.next_node = new_node
      end

      @tail = new_node
      @size += 1
    end
  end
end
```
_listing 7.25 The List#right_push method_

The `right_push` method is also similar to `left_push` and also behaves differently whether or not the list was empty. We start by creating the new node, where the previous node value is set to what the tail was. If the list was empty, this value will be `nil` and the new element will be the only element in the list, otherwise its previous node value will point at the old tail. If the list was empty, we also need to update the head value. Otherwise, we need to update the next node value of the old tail to point to the new node.

Finally, we update the value of `@tail` and increment the size of list.

**The \*X variants, LPUSHX & RPUSHX**

There are two more commands, almost identical to `LPUSH` & `RPUSH`, `LPUSHX` & `RPUSHX`. The only difference is that these two commands only push new elements to the list if it already exists. If the list does not already exist, it returns 0, otherwise it returns the new size of the list, like `LPUSH` & `RPUSH` do.

Given that these commands share a good amount of logic, let's add a few more helpers to the `ListUtils` module:

``` ruby
module BYORedis

  # ...

  module ListUtils
    # ...
    def self.find_list(db, args)
      common_find(args) do |key|
        db.lookup_list(key)
      end
    end

    def self.common_xpush(list)
      if list.nil?
        RESPInteger.new(0)
      else
        yield
      end
    end
  end
  # ...
end
```
_listing 7.26 The ListUtils module_

We can now create the `LPushXCommand` and `RPushXCommand` classes:

``` ruby
module BYORedis

  # ...
  class LPushXCommand < BaseCommand
    def call
      list = ListUtils.find_list(@db, @args)
      values = @args[1..-1]
      ListUtils.common_xpush(list) do
        ListUtils.common_lpush(list, values)
      end
    end

    def self.describe
      Describe.new('lpushx', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@list', '@fast' ])
    end
  end
end
```
_listing 7.27 The LPushXCommand_

The only difference is that we call `@db.lookup_list(key)` instead of `@db.lookup_list_for_write(key)`.

``` ruby
module BYORedis

  # ...
  class RPushXCommand < BaseCommand
    def call
      list = ListUtils.find_list(@db, @args)
      values = @args[1..-1]
      ListUtils.common_xpush(list) do
        ListUtils.common_rpush(list, values)
      end
    end

    def self.describe
      Describe.new('rpushx', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@list', '@fast' ])
    end
  end
end
```
_listing 7.28 The RPushXCommand_

We now have four List related commands, which allow us to add elements to a list, but we don't have any way to read the content of the list. This is what the `LRANGE` command was created for.


### Reading a list with LRANGE

The `LRANGE` command accepts three arguments, a key, a start index and a stop index. It returns all the elements in the list stored at the given key, between the start and stop indices. As discussed earlier in the chapter, it supports a special syntax, with negative indices, to index element from the right instead of from the left.

Another important piece of the `LRANGE` command is that it does not return errors for out of bound indices or for out of order start and stop values. Let's look at few examples first.

```
127.0.0.1:6379> RPUSH a-list a b c d e f
(integer) 6
```

The `a-list` list contains the following elements, `[ 'a', 'b', 'c', 'd', 'e', 'f' ]`. The index of the element, `'f'` is 5, but `LRANGE` silently ignores this if we ask for all the elements up to index 10 for instance:

```
127.0.0.1:6379> LRANGE a-list 0 10
1) "a"
2) "b"
3) "c"
4) "d"
5) "e"
6) "f"
```

It returned all the elements in the given range, and ignored the fact that there are no elements between index 6 and 10. The same logic works the other way, with negative indices, `'f'` is at index `-1`, and `'a'` is at index `-6`. There is no element at index `-7` and beyond:

```
127.0.0.1:6379> LRANGE a-list -10 -6
1) "a"
```

And Redis returns empty arrays if the whole request is outside the list:

```
127.0.0.1:6379> LRANGE a-list -10 -7
(empty array)
127.0.0.1:6379> LRANGE a-list 6 10
(empty array)
```

Finally, the following commands don't make any sense, since the start value is after the stop value, and Redis returns an empty array in this case:

```
127.0.0.1:6379> LRANGE a-list 2 1
(empty array)
127.0.0.1:6379> LRANGE a-list -1 -2
(empty array)
```

Let's implement the logic for the `LRANGE` command:

``` ruby
module BYORedis
  # ...

  class LRangeCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      key = @args[0]
      start = OptionUtils.validate_integer(@args[1])
      stop = OptionUtils.validate_integer(@args[2])
      list = @db.lookup_list(key)

      if list.nil?
        EmptyArrayInstance
      else
        ListSerializer.new(list, start, stop)
      end
    end

    def self.describe
      Describe.new('lrange', 4, [ 'readonly' ], 1, 1, 1, [ '@read', '@list', '@slow' ])
    end
  end
end
```
_listing 7.29 The LRange Command_

We use another argument length validator, in this case we always require three arguments. We then need to validate that the second and third arguments are integers, we do so with a new validator method, `validate_integer`:

``` ruby
module BYORedis

  ValidationError = Class.new(StandardError) do
    def resp_error
      RESPError.new(message)
    end
  end

  module OptionUtils

    def self.validate_integer(str)
      Integer(str)
    rescue ArgumentError, TypeError
      raise ValidationError, 'ERR value is not an integer or out of range'
    end

    def self.validate_float(str, field_name)
      Float(str)
    rescue ArgumentError, TypeError
      raise ValidationError, "ERR #{ field_name } is not a float or out of range"
    end
  end
end
```
_listing 7.30 The OptionsUtils module_

The module also includes a `validate_float` method, which is very similar to the `validate_integer` method. We will need the `validate_float` method later on in this chapter.

Similarly to `InvalidArgsLength`, we need to handle `ValidationError` in the `execute_command` method in `BaseCommand`:

``` ruby
module BYORedis
  class BaseCommand
    # ...
    def execute_command
      call
    rescue InvalidArgsLength => e
      @logger.debug e.message
      command_name = self.class.describe.name.upcase
      e.resp_error(command_name)
    rescue WrongTypeError, ValidationError => e
      e.resp_error
    end
  end
end
```
_listing 7.31 Updates to the BaseCommand class to support ValidationError exceptions_

We delegate the actual serialization logic to a dedicated class, `ListSerializer`, given that the logic requires the handling of many edge cases. Let's look at the class:

``` ruby
module BYORedis
  # ...

  class ListSerializer

    attr_reader :start, :stop, :list
    attr_writer :start, :stop

    def initialize(list, start, stop)
      @list = list
      @start = start
      @stop = stop
    end

    def serialize
      @stop = @list.size + @stop if @stop < 0
      @start = @list.size + @start if @start < 0

      @stop = @list.size - 1 if @stop >= @list.size
      @start = 0 if @start < 0

      return EmptyArrayInstance.serialize if @start > @stop

      response = ''
      size = 0
      distance_to_head = @start
      distance_to_tail = @list.size - @stop

      if distance_to_head <= distance_to_tail
        iterator = List.left_to_right_iterator(@list)
        within_bounds = ->(index) { index >= @start }
        stop_condition = ->(index) { index > @stop }
        accumulator = ->(value) { response << RESPBulkString.new(value).serialize }
      else
        iterator = List.right_to_left_iterator(@list)
        within_bounds = ->(index) { index <= @stop }
        stop_condition = ->(index) { index < @start }
        accumulator = ->(value) { response.prepend(RESPBulkString.new(value).serialize) }
      end

      until stop_condition.call(iterator.index)
        if within_bounds.call(iterator.index)
          accumulator.call(iterator.cursor.value)
          size += 1
        end

        iterator.next
      end

      response.prepend("*#{ size }\r\n")
    end
  end
end
```
_listing 7.32 `ListSerializer` in list.rb_

The class implements an interface similar to the classes in `resp_types.rb`, which allows us to return it from the `call` method, and let the `Server` class call `.serialize` on the value returned by `execute_command`.

The `serialize` method is fairly long, so let's look at it one line at a time:

The first two lines take care of negative indices. If either `start` or `stop` are negative, we convert them to a 0-based index by adding the negative value to the size of list. Let's illustrate this with an example.

``` ruby
list = [ 'a', 'b', 'c' ] # list.size == 3
stop = -1
stop = 3 + stop # stop == 2
list[stop] == 'c' # stop is the last index of the array
start = -2
start = 3 + start # start == 1
list[start] == 'b' # start is the second to last index of the array
```

The next two lines take care of out of bound indices. If `stop` is greater than the last index of the list, `size - 1`, then we set it to that value. There's no need to keep iterating once we reached the last element. Similarly, if stop is lower than `0`, we set it to `0`, there is no element before the one at index `0`, so there's no need to look there.

Once the indices have been sanitized, we can return early if `start > stop`, as we've shown above, this is nonsensical and we return an empty array right away.

The next step is an optimization to speed up the iteration process. Depending on the values of `start` & `stop`, it might be more interesting to start iterating from the head of the list, or from the tail. Imagine a list with one million elements it would be great if `LRANGE -1 -1`, which returns the tail of the list, would run in O(1) time, and not in O(n) time. Our `List` class holds a reference to the tail, so it is feasible to return it without iterating through the list.

We achieve this with the `distance_to_head` & `distance_to_tail` variables. If `start` is `0`, there is no need for "empty iterations" to reach the first element we need to return, but if `start` were, say, 100, we would need to iterate 100 times to reach the first element we need to return.

The same goes for `distance_to_tail`, if `list.size - stop` is equal to 0, the last element of the list needs to be returned, so there's no need for empty iterations, but if stop were, say, 2, in a list of 100 elements, we'd need 98 empty iterations from the right to reach the last element that needs to be returned.

If `distance_to_head` is smaller than `distance_to_tail`, it'll be faster to reach the sublist that needs to be returned if we start from the head, and if `distance_to_tail` is smaller, then it'll be faster to start from the tail.

We could have written two `while/until` loops in each branch of the `if/else` condition, but instead we chose to define a few `proc`s that will determine the direction of the iteration, so that we can write a single loop below.

Because this is a common pattern across list commands, we define an `Iterator` struct as well as two helpers to return the two common iterators:

``` ruby
module BYORedis
  class List
    # ...
    Iterator = Struct.new(:cursor, :index, :cursor_iterator, :index_iterator) do
      def next
        self.cursor = cursor_iterator.call(cursor)
        self.index = index_iterator.call(index)
      end
    end
    # ...
    def self.left_to_right_iterator(list)
      # cursor, start_index, iterator, index_iterator
      Iterator.new(list.head, 0, ->(node) { node.next_node }, ->(i) { i + 1 })
    end

    def self.right_to_left_iterator(list)
      # cursor, start_index, iterator, index_iterator
      Iterator.new(list.tail, list.size - 1, ->(node) { node.prev_node }, ->(i) { i - 1 })
    end
    # ...
  end
end
```
_listing 7.33 The new Iterator class in the List class_

We define the starting index for the iteration, `0` in the left to right iteration, `size - 1` for a left to right iteration.

The `cursor` attribute of the `Iterator` instance will be set to each node of the list, its initial value is `list.head` if we start from the left and `list.tail` if we start from the right.

The `cursor_iterator` attribute is a proc that determines how to get the following node in the list. If we're iterating from left to right, we need to get the node at `next_node`, and if we're iterating from right to left, we need to get the node at `prev_node`.

Similarly, `index_iterator` is a proc that updates the current index value, it increments it for a left to right iteration and decrements it for a right to left iteration.

`within_bounds` is a proc that returns a boolean for an index, which indicates if the current index is within the bounds of the requested range. If we're iterating from left to right, as soon as the current index reaches the value of `start`, we need to start accumulating the elements we find. The condition is different for a right to left iteration, we need to start accumulating elements as soon as the current index is lower or equal to stop. Let's look at few examples to illustrate this:

``` ruby
list = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g' ] # size is 7, last index is 6
```

If `start` is `1` and `stop` is `2`, the returned list should be `[ 'b', 'c' ]` and we'll start iterating from the head. `index` will be initialized at `0`, on the next iteration it'll be `1`, which will cause the `within_bounds` proc to return `true` since `1 >= 1`. On the next iteration `index` will be two and `within_bounds` will return `true` again. On the next iteration `index` will be 3, which it outside the bounds of the given range, so we could return at this point.

We achieve this with the `stop_condition` proc. In this example it'll be `index > stop`, and since `3 > 2`, the `until` loop will stop.

The accumulation process works with the `response` string. In a left to right iteration we append new values to this string with `<<` and in a right to left operation we prepend to it with the `prepend` method.

The last step of the method is to add the size of the RESP array at the beginning of the string, to follow the RESP protocol. We do this with `response.prepend("*#{ size }\r\n")`.

Now that we can add elements to a list, and read elements from a list, it's time to add commands to remove elements.

### Removing items with LPOP & RPOP

The main two commands to remove elements from a list are `LPOP` & `RPOP`. `LPOP` pops an element from the **L**eft and `RPOP` pops an element from the **R**ight. Both commands accept a single arguments, the key for the list. If a pair exists for this key, but is not a list, an error is returned. If no pairs exist, a `nil` string is returned. Redis does not keep empty lists in memory, as can be shown in the next example, so if the last element is popped form the list, we need to remove the pair from `@db.data_store`:

```
127.0.0.1:6379> RPUSH a-list a b
(integer) 2
127.0.0.1:6379> LPOP a-list
"a"
127.0.0.1:6379> LPOP a-list
"b"
127.0.0.1:6379> TYPE a-list
none
```

Let's create the `LPopCommand` class, but first, similarly to how we approached the `LPUSH` & `RPUSH` commands, we know we're going to need very similar functionality with `LPOP` & `RPOP`, so let's define shared methods in `ListUtils` first:

``` ruby
module BYORedis
  # ...
  module ListUtils
    # ...
    def self.common_pop(db, args)
      Utils.assert_args_length(1, args)
      key = args[0]
      list = db.lookup_list(key)

      if list.nil?
        NullBulkStringInstance
      else
        value = yield key, list
        RESPBulkString.new(value)
      end
    end
  end
  # ...
  class LPopCommand < BaseCommand

    def call
      ListUtils.common_pop(@db, @args) do |key, list|
        @db.left_pop_from(key, list)
      end
    end

    def self.describe
      Describe.new('lpop', 2, [ 'write', 'fast' ], 1, 1, 1, [ '@write', '@list', '@fast' ])
    end
  end
end
```
_listing 7.34 The LPopCommand using a shared method from ListUtils_

We could call the not implemented yet `left_pop` method from the `call` method directly, but because the process of checking if the list is now empty and removing it from the keyspace is so common, we wrap this logic in the `DB#left_pop_from` method. Let's create this method:

``` ruby
module BYORedis
  class DB
    # ...
    def left_pop_from(key, list)
      generic_pop_wrapper(key, list) do
        list.left_pop
      end
    end

    private

    def generic_pop_wrapper(key, list)
      popped = yield
      @data_store.delete(key) if list.empty?

      if popped
        popped.value
      else
        @logger.warn("Unexpectedly popped from an empty list or a nil value: #{ key }")
        nil
      end
    end
  end
end
```
_listing 7.35 The proxy method in the DB class to handle deletions of empty lists_

`left_pop_from` starts by calling `generic_pop_wrapper` with the `key` & `list` values as well as with a block calling `left_pop` on the list.

`generic_pop_wrapper` allows us to define logic that is agnostic of which pop operations we're performing. It starts by calling `yield`, which is assuming to pop a value from the list, either from the left or from the right. It then proceeds to delete the list from `@data_store` if the list is now empty. It is expected that `popped` will never be `nil`, since we only keep non-empty lists in `@data_store`, but as a way to be extra cautious, we log a warning and return `nil` if `popped` happened to be `nil`.

We can now define `RPopCommand`:

``` ruby
module BYORedis
  # ...
  class RPopCommand < BaseCommand
    def call
      ListUtils.common_pop(@db, @args) do |key, list|
        @db.right_pop_from(key, list)
      end
    end

    def self.describe
      Describe.new('rpop', 2, [ 'write', 'fast' ], 1, 1, 1, [ '@write', '@list', '@fast' ])
    end
  end
end
```
_listing 7.36 The RPopCommand class_

Let's also define `right_pop_from` in `DB`:

``` ruby
module BYORedis
  class DB
    # ...
    def right_pop_from(key, list)
      generic_pop_wrapper(key, list) do
        list.right_pop
      end
    end
  end
end
```
_listing 7.37 The proxy method in the DB class to handle empty list deletions after a right pop_


### Removing from a list and adding to another with RPOPLPUSH

A common use case for Redis is to use lists as queues, where elements are added from the left, with `LPUSH`, and removes from the right, with `RPOP`, this makes the list a FIFO, **F**irst **I**n **F**irst **O**ut, queue, where the order of insertion defines the order of processing.

For an extra layer of reliability, it can be convenient to keep the popped element in another list to make sure that even if the process in charge of processing the message fails to complete it, the message won't be lost and will still be in the processing queue.

**A use case for `RPOPLPUSH`**

An example of such use case is an e-commerce site with a subscription component. Let's imagine that there is a system in charge of processing subscriptions, each day at 1am it processes the subscriptions for the day, creating orders for customers and charging them for the amount of the order. We could implement this process by calling `LPUSH` for each subscription that needs to be processed that day and having one or worker processes continuously calling `RPOP` for that list. Each message would contain enough information to process the order.

This approach of queuing messages for processing by separate workers is fairly common as it allows for easy scaling of the number of workers. It only needs a single process to run on a schedule, find all the subscriptions that need to be processed and adds them to the queue. On the other end of the queue, there could be one or more workers, picking up messages and processing them. There could even be an auto-scaling systems that adds workers based on the size of the queues. If tens of thousand of subscriptions need to be processed, many workers could be started to speed up the process.

Now, imagine that the system in charge of handling the payment portion is having some issues and is causing some errors, we would still want to process these orders later on, when the payment system is back online. This is where the idea of a queue dedicated to keeping the items being processed is useful.

The worker processes would call `RPOP`, and before processing the message, would call `LPUSH` to add the message to the processing queue. With this approach, regardless of what happens to the worker, the message is safe in the processing list. There might be another process in charge of scanning the processing list and retrying the message at a later point. If no errors happen, the worker must delete the message from the processing queue, to signify that it is fully processed.

The problem with this approach is that things can still go wrong, networks encounter issues, and there is no guarantee that the `LPUSH` operation will work, even if the `RPOP` operation was successful. In other words, with `RPOP`, the messages leaves the Redis server completely, and even if we try to put it back with `RPUSH`, this is not guaranteed to succeed.

Another worst-case scenario, which is not that uncommon in my experience, is that something goes wrong on the client side, after receiving the message from `RPOP` but before sending it back with `RPUSH`. There are different ways in which _something_ could go wrong, like an unhandled exception for instance. That being said, many workers are setup with some `try/catch`/`begin/rescue` wrappers to handle such cases, but the likelihood of a bug is still there.

Additionally, with some platforms such as the JVM, it is also possible to observe exceptions such as `OutOfMemoryError`. These errors are usually rare, but still likely to happen, at _some_ point, for any large enough project.

This is the problem that `RPOPLPUSH` solves, in one operation, it pops an element from the right of a list, and pushes it to the left of another. The message never leaves Redis.

**The `RPopLPushCommand` class**

If the source key is nil, a `nil` string is returned, and if it exists but is not a list, an error is returned. Otherwise, it pops an element, again deleting the list from the keyspace if the list ends up empty, and pushes the element to the destination. Destination is created if it does not already exist, and an error is returned if it already exists and is not a list:

``` ruby
module BYORedis
  class RPopLPushCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)

      source_key = @args[0]
      source = @db.lookup_list(source_key)

      if source.nil?
        NullBulkStringInstance
      else
        destination_key = @args[1]

        if source_key == destination_key && source.size == 1
          source_tail = source.head.value
        else
          destination = @db.lookup_list_for_write(destination_key)
          source_tail = @db.right_pop_from(source_key, source)
          destination.left_push(source_tail)
        end

        RESPBulkString.new(source_tail)
      end
    end

    def self.describe
      Describe.new('rpoplpush', 3, [ 'write', 'denyoom' ], 1, 2, 1,
                   [ '@write', '@list', '@slow' ])
    end
  end
end
```
_listing 7.38 The RPopLPushCommand class_

We do not use any new methods here. We initially call `lookup_list` for the source list, and return early if it doesn't exist. We then call `lookup_list_for_write` for the destination, creating the list if it does not already exist.

We then pop from the source, with the `right_pop_from` method, which deletes the list if it is now empty, and then call `left_push` to the `destination` list. The returned value is the element that was popped and pushed.

It's worth nothing that `RPOPLPUSH` can be used with the same list as source and destination, which ends up moving the tail of the list to the head. This creates an edge case if the list has a single element, if so, we don't have to do anything, and we can simply return the head of the list, or the tail, since they're the same value. We have to do this because otherwise calling `@db.right_pop_from(source_key, source)` would cause the deletion of the list, which we want to avoid.

### A bunch of useful utility commands

Redis defines a few more list commands:

**`LINDEX` - The element at a given index**

`LINDEX` accepts two arguments, a key and the index of the element we want to return. Similarly to `LRANGE`, `LINDEX` accept negative indices. If there is an existing pair and it is not a list, an error is returned. If the index is out of bounds, a `nil` string is returned.

``` ruby
module BYORedis
  # ...
  class LIndexCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)

      key = @args[0]
      index = OptionUtils.validate_integer(@args[1])
      list = @db.lookup_list(key)

      if list.nil?
        NullBulkStringInstance
      else
        value_at_index = list.at_index(index)
        if value_at_index
          RESPBulkString.new(value_at_index)
        else
          NullBulkStringInstance
        end
      end
    end

    def self.describe
      Describe.new('lindex', 3, [ 'readonly' ], 1, 1, 1, [ '@read', '@list', '@slow' ])
    end
  end
end
```
_listing 7.39 The LIndex class_

Let's add the `List#at_index` method:

``` ruby
module BYORedis
  class List
    # ...
    def at_index(index)
      index += @size if index < 0
      return if index >= @size || index < 0

      distance_to_head = index
      distance_to_tail = @size - index

      if distance_to_head <= distance_to_tail
        iterator = List.left_to_right_iterator(self)
      else
        iterator = List.right_to_left_iterator(self)
      end

      while iterator.cursor
        return iterator.cursor.value if iterator.index == index

        iterator.next
      end
    end
  end
end
```
_listing 7.40 The List#index method_

The `at_index` method makes use of the `Iterator` helpers, since we also want to iterate from the right side if the given index is closer to the tail. We also perform the same sanitation step to transform a negative index into a positive 0-based index.

**`LPOS` - The index (or indices) of element(s) matching the argument**

The `LPOS` command supports three options: `COUNT`, `MAXLEN` & `RANK`. `COUNT` determines the maximum number of elements that can be returned if multiple elements match the argument. By default `LPOS` returns one or zero index, for the first element being equal to the argument, starting from the left, but if `COUNT` is given, it returns an array, empty if no elements were found, or containing all the indices of element being equal to the argument.

`RANK` can be used to skip some matches and return the n-th one, from the left with a positive rank and from the right with a negative rank. A rank value cannot be zero or negative, and a rank value of 1 is the default, return the first match starting from the head. A rank value of -1 will return the last match. A rank value of 2 will return the second match, from the left, and -2, the second to last element, or second element from the right, and so on.

The last option, `MAXLEN`, can be used to limit the number of elements that will be scanned. By default `LPOS` will scan up to the whole list, but with `MAXLEN n`, it will stop after n elements.

The options can be combined, for instance `LPOS a-list element RANK -1 MAXLEN 10` will only look at the last ten elements of the list when trying to find the index of element.

``` ruby
module BYORedis

  ZeroRankError = Class.new(StandardError) do
    def message
      'ERR RANK can\'t be zero: use 1 to start from the first match, 2 from the second, ...'
    end
  end
  NegativeOptionError = Class.new(StandardError) do
    def initialize(field_name)
      @field_name = field_name
    end

    def message
      "ERR #{ @field_name } can\'t be negative"
    end
  end

  # ...
  class LPosCommand < BaseCommand

    def initialize(db, args)
      super
      @count = nil
      @maxlen = nil
      @rank = nil
    end

    def call
      Utils.assert_args_length_greater_than(1, @args)

      key = @args.shift
      element = @args.shift
      list = @db.lookup_list(key)

      parse_arguments unless @args.empty?

      if list.nil?
        NullBulkStringInstance
      else
        position = list.position(element, @count, @maxlen, @rank)
        if position.nil?
          NullBulkStringInstance
        elsif position.is_a?(Array)
          RESPArray.new(position)
        else
          RESPInteger.new(position)
        end
      end
    rescue ZeroRankError, NegativeOptionError => e
      RESPError.new(e.message)
    end

    def self.describe
      Describe.new('lpos', -3, [ 'readonly' ], 1, 1, 1, [ '@read', '@list', '@slow' ])
    end

    private

    def parse_arguments
      until @args.empty?
        option_name = @args.shift
        option_value = @args.shift
        raise RESPSyntaxError if option_value.nil?

        case option_name.downcase
        when 'rank'
          rank = OptionUtils.validate_integer(option_value)
          raise ZeroRankError if rank == 0

          @rank = rank
        when 'count'
          count = OptionUtils.validate_integer(option_value)
          raise NegativeOptionError, 'COUNT' if count < 0

          @count = count
        when 'maxlen'
          maxlen = OptionUtils.validate_integer(option_value)
          raise NegativeOptionError, 'MAXLEN' if maxlen < 0

          @maxlen = maxlen
        else
          raise RESPSyntaxError
        end
      end
    end
  end
end
```
_listing 7.41 The LPosCommand class_

The `LPosCommand` class takes care of parsing the list of arguments and storing the values in the `@count`, `@maxlen` & `@rank` instance variables. We perform a few validations to make sure that if a rank is given it is an integer, but not zero and that `MAXLEN` & `COUNT` are positive integers.

We add the `RESPSyntaxError` class to the `utils.rb` file now that we need to use it from more than one command, for `SET` & `LPOS`.

``` ruby
module BYORedis
  # ...
  RESPSyntaxError = Class.new(StandardError) do
    def message
      'ERR syntax error'
    end
  end
  # ...
end
```
_listing 7.42: Adding `RESPSyntaxError` to utils.rb_

We also need to add it to the list of exceptions rescued in `BaseCommand`.

``` ruby
module BYORedis
  class BaseCommand
    # ...
    def execute_command
      call
    rescue InvalidArgsLength => e
      @logger.debug e.message
      command_name = self.class.describe.name.upcase
      e.resp_error(command_name)
    rescue WrongTypeError, RESPSyntaxError, ValidationError => e
      e.resp_error
    end
  end
end
```
_listing 7.43 Updates to the BaseCommand class to support RESPSyntaxError exceptions_

The actual logic is in the `List#position` method:

``` ruby
module BYORedis
  class List
    # ...
    def position(element, count, maxlen, rank)
      return if count && count < 0
      return if @size == 0
      return if rank && rank == 0

      match_count = 0
      maxlen = @size if maxlen == 0 || maxlen.nil?
      indexes = [] if count

      if rank.nil? || rank >= 0
        iterator = List.left_to_right_iterator(self)
      else
        iterator = List.right_to_left_iterator(self)
      end

      while iterator.cursor
        if (rank.nil? || rank >= 0) && iterator.index >= maxlen
          break
        elsif (rank && rank < 0) && (@size - iterator.index - 1) >= maxlen
          break
        end

        if element == iterator.cursor.value
          match_count += 1

          reached_rank_from_head = rank && rank > 0 && match_count >= rank
          reached_rank_from_tail = rank && rank < 0 && match_count >= (rank * -1)

          if rank.nil? || reached_rank_from_head || reached_rank_from_tail
            return iterator.index if indexes.nil?

            indexes << iterator.index
          end

          return indexes if indexes && indexes.size == count
        end

        iterator.next
      end

      indexes
    end
  end
end
```
_listing 7.44 The List#position method_

There's a lot going on in `List#position`, let's break it down one step at a time:

The first three lines are sanity checks for the three optional arguments, if any of these values are invalid, there's no need to continue.
We initialize a variable to count the number of matches, `match_count`. We also give `maxlen` a default value of the size of the list if it wasn't set. If a count value is given then we instantiate an array to store all the indices that need to be returned.

If no rank is given or if rank is positive, we create a left to right iterator, otherwise we create a right to left iterator, we then iterate through the list with the newly created iterator.

If we are in a left to right iteration, checked with the `if (rank.nil? || rank >= 0)` condition, we can stop iterating once the index of the iterator reached `maxlen`, we've seen enough elements, there's no need to continue. We perform a similar check in the case of a right to left iteration, checked with `if (rank && rank < 0)`, but this time we know that we've seen enough elements if `@size - iterator.index - 1` is greater than or equal to `maxlen`. Using a list of size 10 as an example, if `maxlen` is set to 3, and rank is negative, we need to inspect the last three values, at index 9, 8 & 7. So once we reach index 6, `10 - 6 - 1` is equal to `3`, which is the value of `maxlen` and the loop will exit. If neither of these conditions match, we need to keep going through the list.

If the current element does not equal `element`, we can jump to the next element in the list, and ignore it, otherwise, we found a match, and the steps to take depend on the `rank` and `maxlen` arguments.

Regardless of the arguments value, we found a new match, so we increment `match_count`.

If a rank value was given, we need to check if the current match should be accounted for based on the rank value. In other words, we might have to ignore the match. For instance, if rank is 3, and this is the first match, we need to ignore the match, neither `reached_rank_from_head` or `reached_rank_from_tail` will be initialized. On the other hand, if we're dealing with the third match, then `reached_rank_from_head` will be true. The `reached_rank_from_tail` variable works the same way but for negative ranks.

If no rank was given, or if either of the two previous variables was set to true, we have found a valid match. If `indexes` is nil, there's no array to accumulate the values into, no count was given and we can return right away. Otherwise, we add the match to the `indexes` variable.

Once these checks have been performed for the rank value, we need to check if we can return right away, which is the case if no count was given, or if there is a count and we have found enough matches.

**`LINSERT` - Add a new element before or an after an element in the list**

`LINSERT` inserts a new element before or after a given pivot. If no values in the list match the pivot, it does nothing and return -1.

``` ruby
module BYORedis
  class LInsertCommand < BaseCommand
    def call
      Utils.assert_args_length(4, @args)

      if ![ 'before', 'after' ].include?(@args[1].downcase)
        raise RESPSyntaxError
      else
        position = @args[1].downcase == 'before' ? :before : :after
      end

      pivot = @args[2]
      element = @args[3]
      list = @db.lookup_list(@args[0])

      return RESPInteger.new(0) if list.nil?

      new_size =
        if position == :before
          list.insert_before(pivot, element)
        else
          list.insert_after(pivot, element)
        end
      RESPInteger.new(new_size)
    end

    def self.describe
      Describe.new('linsert', 5, [ 'write', 'denyoom' ], 1, 1, 1,
                   [ '@write', '@list', '@slow' ])
    end
  end
end
```
_listing 7.45 The LInsertCommand_

The `LInsertCommand` class parses the list of arguments to see if the new element needs to be added before or after the pivot and delegates the operation to the `List` class.

We now need to add the `List#insert_before` and `List#insert_after` methods:

``` ruby
module BYORedis
  class List
    # ...
    def insert_before(pivot, element)
      generic_insert(pivot) do |node|
        new_node = ListNode.new(element, node.prev_node, node)
        if @head == node
          @head = new_node
        else
          node.prev_node.next_node = new_node
        end

        node.prev_node = new_node
      end
    end

    def insert_after(pivot, element)
      generic_insert(pivot) do |node|
        new_node = ListNode.new(element, node, node.next_node)
        if @tail == node
          @tail = new_node
        else
          node.next_node.prev_node = new_node
        end

        node.next_node = new_node
      end
    end

    private

    def generic_insert(pivot)
      cursor = @head

      while cursor
        break if cursor.value == pivot

        cursor = cursor.next_node
      end

      if cursor.nil?
        -1
      else
        @size += 1

        yield cursor

        @size
      end
    end
  end
end
```
_listing 7.46 The List#insert_before & List#insert_after methods_

The first step is the same for each method, and we share the logic in the `generic_insert` method. We iterate from the left, until we find the pivot, the difference occurs when we do find the pivot.

In the `insert_before` case, we create a new node with the new value, with its `prev_node` value to the node that was before the pivot, `node.prev_node` and its `next_node` value to the pivot, `node`. If the pivot was not the head, it will have a `prev_node` value, and we need to update the `next_node` value on that node to now point to the new node. Otherwise, if it was the head, then we need to update the `@head` reference to be the new node.
The element preceding the pivot is now the new node with `node.prev_node = new_node`.

The logic in `insert_after` is quite similar. We start by creating a new node, where `prev_node` is set to the pivot, `node`, and its `next_node` is set to the element that follows the pivot, `node.next_node`. If the pivot was the tail, then we need to update the `@tail` reference, otherwise, we need to update the `prev_node` value of the element following pivot to now point to the new node, `node.next_node.prev_node = new_node`.
The element following the pivot is now the new node with `node.next_node = new_node`.

**`LLEN` - Return the length of a list**

The `LLenCommand` class is more straightforward than the previous ones. It takes a single argument, the key for the list, and returns its length, or 0 if the list does not exist:

``` ruby
module BYORedis
  class LLenCommand < BaseCommand

    def call
      Utils.assert_args_length(1, @args)
      key = @args[0]
      list = @db.lookup_list(key)

      if list.nil?
        RESPInteger.new(0)
      else
        RESPInteger.new(list.size)
      end
    end

    def self.describe
      Describe.new('llen', 2, [ 'readonly', 'fast' ], 1, 1, 1, [ '@read', '@list', '@fast' ])
    end
  end
end
```
_listing 7.47 The LLenCommand class_

**`LREM` - Remove one more element from a list**

The `LRemCommand` commands accepts three arguments, the key for the list, a count value and the element that we intend to remove. The meaning of the count argument is the following:

- count > 0: Remove elements equal to element moving from head to tail.
- count < 0: Remove elements equal to element moving from tail to head.
- count = 0: Remove all elements equal to element.

``` ruby
module BYORedis
  class LRemCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      key = @args[0]
      count = OptionUtils.validate_integer(@args[1])
      element = @args[2]
      list = @db.lookup_list(key)

      if list.nil?
        RESPInteger.new(0)
      else
        RESPInteger.new(list.remove(count, element))
      end
    end

    def self.describe
      Describe.new('lrem', 4, [ 'write' ], 1, 1, 1, [ '@write', '@list', '@slow' ])
    end
  end
end
```
_listing 7.48 The LRemCommand class_

The `LRemCommand` classes performs the necessary validations, such as validating that the value for count is an integer, and then delegates the actual removal operation to the `List#remove` method:

``` ruby
module BYORedis
  class List
    def remove(count, element)
      delete_count = 0
      if count >= 0
        iterator = List.left_to_right_iterator(self)
      else
        iterator = List.right_to_left_iterator(self)
      end

      while iterator.cursor
        cursor = iterator.cursor
        if cursor.value == element
          if @head == cursor
            @head = cursor.next_node
          else
            cursor.prev_node.next_node = cursor.next_node
          end

          if @tail == cursor
            @tail = cursor.prev_node
          else
            cursor.next_node.prev_node = cursor.prev_node
          end

          delete_count += 1
          @size -= 1

          if count != 0 && (delete_count == count || delete_count == (count * -1))
            break
          end
        end

        iterator.next
      end

      delete_count
    end
  end
end
```
_listing 7.49 The List#remove method_

The first steps should look familiar at this point. Because of the difference in logic between a negative or positive/zero count, we will need to iterate from the left or the right, and create the necessary iterator.

The main loop does nothing if the current element does not match the given element we want to remove. If there is a match, we always start by deleting the node. The deletion step in a doubly linked list requires a few steps:

- If the cursor is the head, then we update the head to be the element following the cursor
- Otherwise we update the `next_node` value of the element preceding the cursor, to point at the element following the cursor
- If the cursor is the tail, then we update the list to be the element preceding the cursor
- Otherwise we update the `prev_node` value of the element following the cursor to the element preceding the cursor

Finally, we increment the `delete_count` variable, and decrement the size of the list. The last step of the loop is to check if we should exit based on the count value. If count is zero, we have to iterate through the whole list to delete all matches, so we do not break early. Otherwise, we stop if we have deleted enough elements.

**`LSET` - Update the value of the element at the given index**

The `LSET` command takes three arguments, the key for the list, the index of the element to be updated, and the new value. `ERR index out of range` is returned if the index is out of range. If the key does not exist, it returns the `ERR no such key` error.

Like many of the previous commands, `LSET` supports negative indices.

``` ruby
module BYORedis
  class LSetCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      key = @args[0]
      index = OptionUtils.validate_integer(@args[1])
      new_value = @args[2]
      list = @db.lookup_list(key)

      if list.nil?
        RESPError.new('ERR no such key')
      elsif list.set(index, new_value)
        OKSimpleStringInstance
      else
        RESPError.new('ERR index out of range')
      end
    end

    def self.describe
      Describe.new('lset', 4, [ 'write', 'denyoom' ], 1, 1, 1, [ '@write', '@list', '@slow' ])
    end
  end
end
```
_listing 7.50 The LSetCommand class_

The `LSetCommand` class performs the necessary validations, and delegates the update process to the `List#set` method. If the method returns `nil`, then `LSetCommand#call` returns the out of range error back to the `Server` class.

``` ruby
module BYORedis
  class List
    # ...
    def set(index, new_value)
      # Convert a negative index
      index += @size if index < 0

      return if index < 0 || index >= @size

      distance_from_head = index
      distance_from_tail = @size - index - 1

      if distance_from_head <= distance_from_tail
        iterator = List.left_to_right_iterator(self)
      else
        iterator = List.right_to_left_iterator(self)
      end

      while iterator.index != index
        iterator.next
      end

      iterator.cursor.value = new_value
    end
  end
end
```
_listing 7.51 The List#set method_

Like the previous commands supporting negative indices, we first convert the index to a 0-based index and return `nil` if the index is out of bounds. We then perform the same optimizations we performed earlier to decide if we should initiate the iteration from the head or from the tail.

Finally, we iterate until we reach the desired index and update the value in place.

**`LTRIM` - Keep a subset of the list and discard the rest**

The last utility command is `LTRIM`, it accepts three arguments, the key for the list, a start index and a stop index. It only keeps the elements in the range delimited by the start and stop indices, all other elements are discarded. If the range is empty, then the list is deleted. Like other commands, it supports negative indices.

``` ruby
module BYORedis
  class LTrimCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      key = @args[0]
      start = OptionUtils.validate_integer(@args[1])
      stop = OptionUtils.validate_integer(@args[2])
      list = @db.lookup_list(key)

      if list
        @db.trim(key, list, start, stop)
      end
      OKSimpleStringInstance
    end

    def self.describe
      Describe.new('ltrim', 4, [ 'write' ], 1, 1, 1, [ '@write', '@list', '@slow' ])
    end
  end
end
```
_listing 7.52 The LTrimCommand class_

The `LTrimCommand` class validates that the start and stop indices are integer, and delegates the trim operation to the `DB#trim` method. It always returns `+OK`, even if the list does not exist.

We now need to add the `DB#trim` & `List#trim` methods:

``` ruby
module BYORedis
  class DB
    # ...

    def trim(key, list, start, stop)
      list.trim(start, stop)
      @data_store.delete(key) if list.empty?
    end
  end
end
```
_listing 7.53 The DB#trim proxy method to handle deletions of empty lists_

The `DB#trim` method deletes the list from the database if the resulting list is empty.

``` ruby
module BYORedis
  class List
    # ...
    def trim(start, stop)
      current_head = @head

      # Convert negative values
      stop = @size + stop if stop < 0
      stop = @size - 1 if stop >= @size
      start = @size + start if start < 0
      start = 0 if start < 0

      if start >= @size || start > stop
        @size = 0
        @head = nil
        @tail = nil
        return
      end

      return if start == 0 && stop == @size - 1

      distance_to_start = start
      distance_to_stop = @size - stop - 1

      if distance_to_start <= distance_to_stop
        iterator = List.left_to_right_iterator(self)
        target_index = start
      else
        iterator = List.right_to_left_iterator(self)
        target_index = stop
      end

      new_head = nil
      new_tail = nil

      while iterator.index != target_index
        iterator.next
      end

      # We reached the closest element, either start or stop
      # We first update either the head and the nail and then find the fastest way to get to the
      # other boundary
      if target_index == start
        new_head = iterator.cursor
        target_index = stop
        # We reached start, decide if we should keep going right from where we are or start from
        # the tail to reach stop
        if distance_to_stop < stop - iterator.index
          iterator = List.right_to_left_iterator(self)
        end
      else
        new_tail = iterator.cursor
        target_index = start
        # We reached stop, decide if we should keep going left from where we are or start from
        # the head to reach start
        if distance_to_start < iterator.index - start
          iterator = List.left_to_right_iterator(self)
        end
      end

      while iterator.index != target_index
        iterator.next
      end

      # We now reached the other boundary
      if target_index == start
        new_head = iterator.cursor
      else
        new_tail = iterator.cursor
      end

      @head = new_head
      @head.prev_node = nil

      # If start == stop, then there's only element left, and new_tail will not have been set
      # above, so we set here
      if start == stop
        new_tail = new_head
        @size = 1
      else
        # Account for the elements dropped to the right
        @size -= (@size - stop - 1)
        # Account for the elements dropped to the left
        @size -= start
      end

      @tail = new_tail
      @tail.next_node = nil
    end
  end
end
```
_listing 7.54 The List#trim method_

We perform the usual steps with negative indices, converting to 0-based indices. We then check if they're out of order and if so, clear the list right away, there's no need to iterate in this case. We also check that if `start == 0 && stop == -1`, in this case, there is nothing to do, we can keep the list as is.

This method is pretty long and pretty verbose in order to minimize the number of empty iterations required to reach the nodes at indices `start` & `stop`. The algorithm can be described as:

- First, find the fastest node to get to. It is `start` if the distance between the head and `start` is lower than the distance between `stop` and the tail. It is `stop` otherwise.
- Once the closest node is reached, try to reach the second boundary in the most efficient manner, there are four options:
  - if the node we reached is `start`, we can reach `stop` in two ways:
    - continue with the same iterator if `stop` is closer to the iterator than it is from the tail
    - iterate from the tail with a new iterator otherwise
  - if the node we reached is `stop`, we can reach `start` in two ways:
    - continue with the same iterator if `start` is closer to the iterator than it is from the head
    - iterate from the head with a new iterator otherwise
- Once we found each nodes, we update `@head` & `@tail` accordingly and adjust the size to accommodate for the dropped nodes.

This algorithm means that calling `LTRIM 997 998` on a 1,000 element list will start from the right, reach the `stop` node in one iteration, and continue with the same iterator to reach the `start` node.

Likewise, calling `LTRIM 1 2` on a 1,000 element list will start from the left, reach the `start` node in one iteration and continue with the same iterator to reach the `stop` node.

Finally, calling `LTRIM 1 998` on a 1,000 element list will start from the left, reach the `start` node in one iteration and create a new iterator, to start from the tail to reach the `stop` node in one iteration.

We've now implemented most of the commands, there are only three more commands, that implement existing logic, but in a blocking manner.

### The blocking variants, BLPOP, BRPOP & BRPOPLPUSH

The last three commands we need to implement are almost identical to `LPOP`, `RPOP` & `RPOPLPUSH`, with the difference that they are **B**locking.

Let's start with the `BLPOP` command. It accepts two arguments or more. The first one is the key for a list, and the last one must be a number, integer or float, which is the timeout the command can block for. It accepts more than one list keys. The following are valid `BLPOP` commands: `BLPOP a 1`, `BLPOP a b c 1.2`. But the following is invalid `BLPOP a b`, `b` is not a valid number and cannot be used to describe a timeout. It's worth noting that since keys can be numbers, which Redis represents at Strings internally, the following is valid `BLPOP 1 1`. It means: "Block for up to 1s for the key '1'".

A timeout value of `0` means an infinite timeout. The server will only unblock the client if a new element is added to one of the lists it is blocked on.

Redis looks at all the keys from left to right, and as soon as it finds a list, it will pop an element from the left and return it, alongside the key, so that the client knows which list the element was popped from:

```
127.0.0.1:6379> LPUSH b b1
(integer) 1
127.0.0.1:6379> BLPOP a b 1
1) "b"
2) "b1"
```

In the previous example, if the list at key `a` had contained at least one element, it would have been returned instead.

The blocking behavior happens when all the lists are empty, in this case Redis will not send a response to the client, effectively blocking it. Any other commands received while blocked will be accumulated and replied to once the blocking operation is unblocked.

There are three conditions that can unblock a client:

- One of the keys the client is blocked on receives a push before the timeout expires
- The timeout expires before any of the list receives a push
- The client disconnects before any of the two previous conditions are met

In the first case, the return value is the same as if the command returned right away without blocking, it returns the key of the list and the element that was popped.

In the second case, it returns a nil array.

In the last example, the client disconnects, so whether one of the list receives a push, or the timeout expires, nothing will happen, the server needs to clear its internal state to make sure it doesn't actually pop any elements since there's no clients to send the data to. Let's look at an example:

Let's block for 100 seconds in one session and close it with `Ctrl-C` right after starting it:

```
127.0.0.1:6379> BLPOP a 100
^C
```

Reopen another `redis-cli` shell and push a value to the list `a`:

```
127.0.0.1:6379> RPUSH a a1
(integer) 1
127.0.0.1:6379> LRANGE a 0 -1
1) "a1"
```

If all went well, we did perform all these operations under 100s, but the element we pushed to the list `a` was not popped, because when the client disconnected, the server knew that there was no client blocked anymore.

If we'd perform the same operations with two sessions, without closing the first one, this is what would happen:

```
127.0.0.1:6379> BLPOP a 100

```

In a second session:

```
127.0.0.1:6379> RPUSH a a1
(integer) 1
```

And back in the first session, we see the result of `BLPOP` alongside the amount of time it was blocked for thanks to `redis-cli`:

```
127.0.0.1:6379> BLPOP a 100
1) "a"
2) "a1"
(19.50s)
```

If we let the timeout expire, we can see that the server returns a nil array:

```
127.0.0.1:6379> BLPOP a 1
(nil)
(1.05s)
```

Note that `redis-cli` displays nil strings and nil arrays the same way, with `(nil)`, we can see which one it is with `nc`:

``` bash
> nc -c localhost 6379
BLPOP a 1
*-1
```


**Blocking operations use cases**

Before implementing the command, it's worth stopping to discuss the benefits of the blocking variants.

There are two main benefits to the blocking variants of the `LPOP`, `RPOP` & `RPOPLPUSH` commands:

- It reduces the latency between the moment when an element is pushed to a list and when it is read by a client
- It reduces the number of potential "empty"/"useless" commands between clients and the server

**Latency improvement**

The latency improvement aspect is interesting if there is a benefit to elements added to the list being popped and sent to a client as fast as possible. A common example of such system is a web application processing jobs in the background.

Let's use a password reset flow as an example. It is not unusual for web applications to offer a password reset flow, so that users can reset their passwords if they forgot it. The idea is that a user provides their email address and they'll receive an email providing information about the steps to take to reset their password.

While it could be considered over-engineered, it's not unusual for web applications to use a background worker process system to implement such flow. The motivation behind this approach is that web applications often rely on third party providers to actually send emails, such as SendGrid, MailChimp or Braze. While often reliable, it is always possible that the APIs provided by these services occasionally fail, but once the email address has been read, the requests can be retried until it finally succeeds, this is where a background worker can be useful. The web server receives the email from the client, queues the request in Redis with an `LPUSH` command, and returns a successful response to the client. The user will see a successful response even though the email might not have yet been actually sent. It is then up to a background worker to fetch the message from the list, with `RPOP` and attempt to actually send the email, retrying if it fails.

In this scenario, we'd like the worker to pick up the message as soon as the message is pushed, but without a blocking variant, the only option we have is to use a polling mechanism. The worker issues sends `RPOP` commands, if something is returned, it processes it, otherwise, it optionally waits and issues another `RPOP` command. Rinse & repeat.

The problem with this approach is that if we program our workers to wait after receiving an empty response, to prevent spamming the server with unnecessary work, we could end up with a delay between when a message is pushed and when it is popped. Let's imagine that workers wait thirty seconds after each empty pop, if we're unlucky, a worker might send a pop command right before the message is pushed and only process it thirty seconds later.

While the impact in this example might be minimal, a thirty second delay for an email could be considered negligible, the fastest we process the email, the more likely we are to prevent confusion for the user. Any delays might cause confusion on their end, wondering if the process worked as expected.

**Empty responses reduction**

One approach that could improve the problem mentioned in the previous section would be to reduce the wait time between each poll. We started with thirty seconds in the previous example, and could decide to reduce is to one hundred milliseconds.

With such a short interval, we would we be guarantee to process the email within 100 milliseconds of the user requesting it. The problem is that the worker is now sending up to 10 requests per second, for a system that might see less than one message per minute.

This approach would create a lot of empty responses and effectively send a lot of useless commands. While Redis is optimized to process responses fast, no commands to process will always be faster that some.

The blocking variants avoid these issues by blocking the clients until an element is available, or until the timeout expires. With this approach, and with a long enough timeout, the number of empty responses can be reduced to almost zero. A worker would send the `RPOP email-reset-queue 60` command, to either get directly a message from the `email-reset-queue` or wait up to 60s until a message is added to the queue. An empty response would only happen if no customers request a password reset within 60s.

The blocking commands also solve the latency issue since there's now no delay between the moment when the message is pushed to the queue and when it is received by a worker.

**Blocking commands caveats**

There are some caveats that should be considered when using blocking commands. Redis will accumulate any further commands sent while the client that initiated the command is blocked. This means that if the application that communicates with Redis needs to perform other related operations, it would need to create a new connection.

This can be a problem for applications using connection pooling. The idea behind connection pooling is that an application will create a bunch of connections, the pool, and keep them available for future use. This removes the need for opening and closing connections, which is unnecessary since once a response has been sent, the same connection can be reused to send a different command.

This approach is particularly useful for web application where one or more processes or threads might be running to serve all the incoming HTTP requests. Let's use a multi-threaded web server as an example, running 100 threads. Let's imagine that about a tenth of the incoming requests require a connection to Redis.

A naive approach would be give each thread a connection, but this would be wasteful, given that most connections would be idle most of the time based on the 10% example we defined above. Instead, we could create a smaller number of connections, in a pool, and make each thread take a connection from the pool, use it and return it to the pool. Concurrency access concerns are really important here as only one thread should use a connection to prevent unexpected things to happen, such as two threads writing their own command to the connection, it would be impossible to read the responses and assign them to each thread.

Now, while connection pools are great to avoid creating too many connections and reuse existing connections to prevent unnecessary work with closing and creating connections, there might be issues if the pool has a fixed size and cannot grow. Different connection pool libraries might provide different features, but it is very common to have a maximum size of connections. The [connection_pool][connection-pool-gh] gem is a common library in Ruby, and [HikariCP][hikari-cp-gh] is a very popular one in Java, both use fixed size pools.

In our example, if the pool was created with ten connections, and nine threads send a `BLPOP` command with a long timeout, only one connection is left available for the other ninety threads, which would cause delay as each of these threads would need to wait for the previous command to complete.

**The `BLPopCommand` class**

Implementing the blocking behavior will require some changes to the `Server` class, but let's start by adding the `BLPopCommand` class in `list_commands.rb`:

``` ruby
module BYORedis
  module ListUtils
    # ...
    def self.timeout_timestamp_or_nil(timeout)
      if timeout == 0
        nil
      else
        Time.now + timeout
      end
    end

    def self.common_bpop(db, args, operation)
      Utils.assert_args_length_greater_than(1, args)

      timeout = OptionUtils.validate_float(args.pop, 'timeout')
      list_names = args

      list_names.each do |list_name|
        list = db.lookup_list(list_name)

        next if list.nil?

        popped = yield list_name, list
        return RESPArray.new([ list_name, popped ])
      end

      BYORedis::Server::BlockedState.new(ListUtils.timeout_timestamp_or_nil(timeout),
                                         list_names, operation)
    end
  end
  # ...
  class BLPopCommand < BaseCommand
    def call
      ListUtils.common_bpop(@db, @args, :lpop) do |list_name, list|
        @db.left_pop_from(list_name, list)
      end
    end

    def self.describe
      Describe.new('blpop', -3, [ 'write', 'noscript' ], 1, -2, 1,
                   [ '@write', '@list', '@slow', '@blocking' ])
    end
  end
end
```
_listing 7.55 The BLPopCommand using a shared method from ListUtils_

Most of the code in the `common_bpop` method uses existing methods, the only difference is if none of the lists in `list_names` exist, we return an instance of `BlockedState`. This is a new struct defined in the `Server` class:


``` ruby
module BYORedis
  class Server
    # ...
    BlockedState = Struct.new(:timeout, :keys, :operation, :target, :client)
    # ...
  end
end
```
_listing 7.56 The BlockedState Struct_

The `BlockedState` struct allows us to store information about blocked clients in the `Server` class. This is necessary to handle both timeouts and unblocking clients when lists they are blocked on are pushed to and a response should be sent to the blocked clients. We also need to remember which type of operation was initially sent, so that we use `left_pop` or `right_pop` when it is time to unblock the client.

The `process_poll_events` method is starting to get big, so we're extracting logic from it when processing client buffers.

``` ruby
module BYORedis
  class Server
    # ...
    def process_poll_events(sockets)
      sockets.each do |socket|
        if socket.is_a?(TCPServer)
          socket = safe_accept_client
          next unless socket

          @clients[socket.fileno.to_s] = Client.new(socket)
        elsif socket.is_a?(TCPSocket)
          client = @clients[socket.fileno.to_s]
            # ...
          if client_command_with_args.nil?
            # ...
          else
            client.buffer += client_command_with_args

            process_client_buffer(client)
          end
        else
          raise "Unknown socket type: #{ socket }"
        end
      end
    end

    def process_client_buffer(client)

      split_commands(client.buffer) do |command_parts|
        return if client.blocked_state

        response = handle_client_command(command_parts)
        if response.is_a?(BlockedState)
          block_client(client, response)
        else
          @logger.debug "Response: #{ response.class } / #{ response.inspect }"
          serialized_response = response.serialize
          @logger.debug "Writing: '#{ serialized_response.inspect }'"
          unless Utils.safe_write(client.socket, serialized_response)
            disconnect_client(client)
          end

          handle_clients_blocked_on_keys
        end
      end
    rescue IncompleteCommand
      # Not clearing the buffer or anything
    rescue ProtocolError => e
      client.socket.write e.serialize
      disconnect_client(client)
    end

    def block_client(client, blocked_state)
      if client.blocked_state
        @logger.warn "Client was already blocked: #{ blocked_state }"
        return
      end

      blocked_state.client = client

      # Add the state to the client
      client.blocked_state = blocked_state
      if blocked_state.timeout
        @db.client_timeouts << blocked_state
      end

      # Add this client to the list of clients waiting on this key
      blocked_state.keys.each do |key|
        client_list = @db.blocking_keys[key]
        if client_list.nil?
          client_list = List.new
          @db.blocking_keys[key] = client_list
        end
        client_list.right_push(client)
      end
    end
    # ...
  end
end
```
_listing 7.57 The blocking related method in the Server class_

The `blocked_state` instance is only added to `client_timeouts` if there is a timeout. If the client specified a value of `0`, then the server will never unblock the client after a timeout and there is therefore no need to keep track of it here.

We added a new method to the `Utils` module, to wrap the logic around writing to a socket, while handling potential exceptions if the socket was closed and the write operation fails:

``` ruby
module BYORedis
  module Utils
    # ...
    def self.safe_write(socket, message)
      socket.write(message)
      true
    rescue Errno::ECONNRESET, Errno::EPIPE
      false
    end
  end
end
```
_listing 7.58 The Utils.safe_write helper method_

The `block_client` method is making use of a few new elements. First it stores the `BlockedState` instance in the client, so we need to update the `Client` struct to add a new attribute:

``` ruby
module BYORedis
  class Server
    # ...
    Client = Struct.new(:socket, :buffer, :blocked_state) do
      attr_reader :id

      def initialize(socket)
        @id = socket.fileno.to_s
        self.socket = socket
        self.buffer = ''
      end
    end
    # ...
  end
end
```
_listing 7.59 Updates to the Client Struct to support_

We also need to add a few more data structures to the `DB` class, namely: `ready_keys`, `:blocking_keys`, `client_timeouts` & `unblocked_clients`.

The first two, `ready_keys` and `blocking_keys`, are `Dict` instances. With `ready_keys` we use a `Dict` even though we only care about keys, and will use `nil` for all values. The purpose of this dictionary is essentially to be a set, a collection that does not store duplicates. Whenever a list receives a push, we will add the list's key to `ready_keys` if it was blocked on. There is no need to store this information more than once, so using a `Dict` as a set works perfectly. We will be able to know if it was blocked by inspecting `blocking_keys`. `blocking_keys` is also a dictionary where keys will be keys of lists that are being blocked on, and the values will be lists of clients blocked. By using a list as the value and appending to the list, we can maintain an order so that the first client to block for a key will be the first client to receive a response when elements will be pushed to the list.

`client_timeouts` is necessary to handle clients that should receive an empty response if the timeout expired before elements were pushed to the list. Without an extra data structure to store timeouts, we would have to iterate over all the connected clients, and manually inspect their `blocked_state` attribute to check if it expired or not. This would be extremely inefficient, especially given that we need to perform this check on each iteration of the event loop. This operation would have an O(n) complexity, where n is the number of connected clients. If no clients are blocked, even if thousands of clients are connected, we want to be able to know that there's no need to check for expired clients.

This is the problem that the `client_timeouts` attribute on `DB` solves. It is a sorted array, which we provide an implementation for in [Appendix B][appendix-b]

Whenever a blocked command is received and there is no element to return right away, a `BlockedState` instance will be pushed to `client_timeouts` unless the timeout was `0`. Using a regular array to store these `BlockedState` instances would already be an improvement compared to the scenario described above. We would only check if clients are expired within the set of blocked clients, but we would still have to check all the clients, one by one, to see if they are expired or not. This is also a O(n) operation, where n is the number of blocked clients.

Using a sorted array turns the operation into an O(1) operation, we can inspect the first element in the sorted array, if it is not expired, there's no need to inspect any other clients, we know their expiration is later than the first one. If the first client in the sorted array is expired, we need to handle it as such and look at the next element, and stop as soon as we find a non expired client.

Using a sorted array also helps with the cleanup tasks required once a client was unblocked after the list it was blocked on received a push. When this happens, we need to remove the client from the `client_timeouts` list since it is not blocked anymore. The sorted array class guarantees that this operation happens in O(logn) time, where n is the number of blocked clients, instead of O(n) if the array was not sorted.

This implementation is a deviation from the Redis one. Redis uses a [Radix Tree][wikipedia-radix-tree] to store client timeouts. A Radix tree provides similar time complexity for the iteration of clients from the ones with the earliest expirations, but it provides a better time complexity for the deletion use case.

We decided to not implement a Radix tree here given the complexity of such implementation. A sorted array provides a good middle ground approach, by being fairly short to implement and still being a great improvement compared to a regular array.

Finally, `unblocked_clients` is a `List` instance, which will be appended to when clients are unblocked, either because their timeouts expired or if the list they were blocked on received a push. Regardless of the reason, we need to check if the client sent commands while it was blocked, and process them.

``` ruby
module BYORedis
  class DB

    attr_reader :data_store, :expires, :ready_keys, :blocking_keys, :client_timeouts,
                :unblocked_clients
    attr_writer :ready_keys

    def initialize
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
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
_listing 7.60 New data structures in the DB class_

---

At this point, once we're done processing a `BLPOP` command, we stored the `BlockedState` information in `client_timeouts` and in the `Client` instance. We also added a new entry if necessary in `blocking_keys`, where the value is a list containing the client that issued the `BLPOP` command.

We're now ready to handle the two possible outcomes for a blocked client. Either its timeout expires, in which case we return a `nil` array, and process any commands sent while blocked. If one of the lists the client was blocked on receives a push before the timeout expires, we return the head of the list to the client, alongside the list key and also process any commands sent while blocked.

**Handling timeouts**

Let's first add two new steps to the event loop, `handle_blocked_clients_timeout` and `process_unblocked_clients`:

``` ruby
module BYORedis
  class Server
    # ...
    def start_event_loop
      loop do
        handle_blocked_clients_timeout
        process_unblocked_clients

        timeout = select_timeout
        @logger.debug "select with a timeout of #{ timeout }"
        result = IO.select(client_sockets + [ @server ], [], [], timeout)
        sockets = result ? result[0] : []
        process_poll_events(sockets)
        process_time_events
      end
    end
    # ...
  end
end
```
_listing 7.61 Updates to the event loop in the Server class to handle blocked client timeouts_

We now start each iteration of the event loop by first checking if any of the blocked clients are expired, and then processing any commands sent while blocked. As mentioned in the previous section, the time complexity of these two methods is important since they would otherwise add a delay at the beginning of each event loop iteration.

``` ruby
module BYORedis
  class Server
    # ...

    def handle_blocked_clients_timeout
      @db.client_timeouts.delete_if do |blocked_state|
        client = blocked_state.client
        if client.blocked_state.nil?
          @logger.warn "Unexpectedly found a non blocked client in timeouts: #{ client }"
          true
        elsif client.blocked_state.timeout < Time.now
          @logger.debug "Expired timeout: #{ client }"
          unblock_client(client)

          unless Utils.safe_write(client.socket, NullArrayInstance.serialize)
            @logger.warn "Error writing back to #{ client }: #{ e.message }"
            disconnect_client(client)
          end

          true
        else
          # Impossible to find more later on since client_timeouts is sorted
          break
        end
      end
    end

    def unblock_client(client)
      if client.socket.closed?
        @logger.warn 'RETURNING EARLY'
        return
      end
      @db.unblocked_clients.right_push client

      return if client.blocked_state.nil?

      # Remove this client from the blocking_keys lists
      client.blocked_state.keys.each do |key2|
        list = @db.blocking_keys[key2]
        if list
          list.remove(1, client)
          @db.blocking_keys.delete(key2) if list.empty?
        end
      end
      client.blocked_state = nil
    end

    def process_unblocked_clients
      return if @db.unblocked_clients.empty?

      cursor = @db.unblocked_clients.left_pop

      while cursor
        client = cursor.value

        if @clients.include?(client.id)
          process_client_buffer(client)
        else
          @logger.warn "Unblocked client #{ client } must have disconnected"
        end

        cursor = @db.unblocked_clients.left_pop
      end
    end
  end
end
```
_listing 7.62 Updates to the Server class to process timeouts for blocked clients_

We need to add one more piece of logic to the server with regards to timeouts. If a client disconnects before its timeout expired, we want to clean things of up so that we don't keep track of this client anymore. We need to more that deleting the client from the `@clients` array in `process_poll_events`.

``` ruby
module BYORedis
  class Server
    # ...
    def safe_accept_client
      @server.accept
    rescue Errno::ECONNRESET, Errno::EPIPE => e
      @logger.warn "Error when accepting client: #{ e }"
      nil
    end

    def safe_read(client)
      client.socket.read_nonblock(1024, exception: false)
    rescue Errno::ECONNRESET, Errno::EPIPE
      disconnect_client(client)
    end

    def process_poll_events(sockets)
      sockets.each do |socket|
        if socket.is_a?(TCPServer)
          socket = safe_accept_client
          next unless socket

          @clients[socket.fileno.to_s] = Client.new(socket)
        elsif socket.is_a?(TCPSocket)
          client = @clients.find { |client| client.socket == socket }
          client_command_with_args = socket.read_nonblock(1024, exception: false)

          client = @clients[socket.fileno.to_s]
          client_command_with_args = safe_read(client)

          if client_command_with_args.nil?
            disconnect_client(client)
          elsif client_command_with_args == :wait_readable
            # There's nothing to read from the client, we don't have to do anything
            next
          elsif client_command_with_args.empty?
            @logger.debug "Empty request received from #{ socket }"
          else
            # ...
          end
        end
      end
    end

    def disconnect_client(client)
      @clients.delete(client.id)
      @db.unblocked_clients.remove(1, client)

      if client.blocked_state
        @db.client_timeouts.delete(client.blocked_state)
        client.blocked_state.keys.each do |key|
          list = @db.blocking_keys[key]
          if list
            list.remove(1, client)
            @db.blocking_keys.delete(key) if list.empty?
          end
        end
      end

      client.socket.close
    end
    # ...
  end
end
```
_listing 7.63 Updates to the Server class to handle clients disconnection_


**Handling blocked clients before timeout**

We need another handler, for when a list that is being blocked on, for instance either `a` or `b` after receiving `BLPOP a b 1`, receives a push and is created. New lists are always created from `DB#lookup_list_for_write`, so let's add a condition there, that will notify that one key that is being blocked on can now be used to unblock one or more clients:

``` ruby
module BYORedis
  class DB
    def lookup_list_for_write(key)
      list = lookup_list(key)
      if list.nil?
        list = List.new
        @data_store[key] = list

        if @blocking_keys[key]
          @ready_keys[key] = nil
        end
      end

      list
    end
  end
end
```
_listing 7.64 Updates to the DB#lookup_list_for_write to notify for list creation_

`blocking_keys` is populated when we process the result of a blocking command. It is a dictionary that contains a list of clients blocked for that key. When a new list is created, we inspect `blocking_keys`, if there's no entry, then no clients are blocked on this key, on the other hand, if there is one or more clients, then we know that the key is being blocked on and we add it to `@ready_keys`. The value of the pair does not matter here, we only care about having an entry in the dictionary.

``` ruby
module BYORedis
  class Server
    # ...
    def initialize
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL

      @clients = Dict.new
      @db = DB.new
      @blocked_client_handler = BlockedClientHandler.new(self, @db)
      @server = TCPServer.new 2000
      @time_events = []
      @logger.debug "Server started at: #{ Time.now }"
      add_time_event(Time.now.to_f.truncate + 1) do
        server_cron
      end

      start_event_loop
    end
    # ...
    def handle_clients_blocked_on_keys
      return if @db.ready_keys.used == 0

      @db.ready_keys.each do |key, _|
        unblocked_clients = @blocked_client_handler.handle(key)

        unblocked_clients.each do |client|
          unblock_client(client)
        end
      end

      @db.ready_keys = Dict.new
    end
  end
end
```
_listing 7.65 Blocked client handling in the Server class_

`handle_clients_blocked_on_keys` is called from `process_poll_events`, after processing the commands from the clients. If the command did not result in the creation of a list, then nothing will happen, no clients can be unblocked. If the command did create a list, then `@db.ready_keys` will have received elements and `@blocked_client_handler.handle` will be called for each of these keys.

The code in charge of handling blocked client lives in its own class, `BlockedClientHandler`.

``` ruby
module BYORedis
  class BlockedClientHandler
    def initialize(server, db)
      @server = server
      @db = db
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
    end

    def handle(key)
      clients = @db.blocking_keys[key]
      unblocked_clients = []

      list = @db.data_store[key]

      if !list || !list.is_a?(List)
        @logger.warn "Something weird happened, not a list: #{ key } / #{ list }"
        raise "Unexpectedly found nothing or not a list: #{ key } / #{ list }"
      end

      raise "Unexpected empty list for #{ key }" if list.empty?

      cursor = clients.left_pop

      while cursor
        client = cursor.value

        if handle_client(client, key, list)
          unblocked_clients << client
        end

        if list.empty?
          break
        else
          cursor = clients.left_pop
        end
      end

      @db.blocking_keys.delete(key) if clients.empty?

      unblocked_clients
    end

    def handle_client(client, key, list)
      blocked_state = client.blocked_state

      # The client is expected to be blocked on a set of keys, we unblock it based on the key
      # arg, which itself comes from @db.ready_keys, which is populated when a key that is
      # blocked on receives a push
      # So we pop (left or right) from the list at key, and send the response to the client
      if client.blocked_state

        response = pop_operation(key, list, blocked_state.operation, blocked_state.target)

        serialized_response = response.serialize
        @logger.debug "Writing '#{ serialized_response.inspect } to #{ client }"

        unless Utils.safe_write(client.socket, serialized_response)
          # If we failed to write the value back, we put the element back in the list
          rollback_operation(key, response, blocked_state.operation, blocked_state.target)
          @server.disconnect_client(client)
          return
        end
      else
        @logger.warn "Client was not blocked, weird!: #{ client }"
        return
      end

      true
    end

    private

    def pop_operation(key, list, operation, target)
      case operation
      when :lpop
        RESPArray.new([ key, @db.left_pop_from(key, list) ])
      when :rpop
        RESPArray.new([ key, @db.right_pop_from(key, list) ])
      when :rpoplpush
        raise "Expected a target value for a brpoplpush handling: #{ key }" if target.nil?

        ListUtils.common_rpoplpush(@db, key, target, list)
      else
        raise "Unknown pop operation #{ operation }"
      end
    end

    def rollback_operation(key, response, operation, target_key)
      list = @db.lookup_list_for_write(key)
      case operation
      when :lpop
        element = response.underlying_array[1]
        list.left_push(element)
      when :rpop
        element = response.underlying_array[1]
        list.right_push(element)
      when :rpoplpush
        target_list = @db.lookup_list(target_key)
        element = target_list.left_pop
        @db.data_store.delete(target_key) if target_list.empty?
        list.right_push(element.value)
      else
        raise "Unknown pop operation #{ operation }"
      end
    end
  end
end
```
_listing 7.66 The BlockedClientHandler class_

This is the final step required to handle blocked clients. We are processing all the keys that can be used to unblock clients. A command can push more than one element, so we might be able to unblock more than one client. We start this process by getting the list of blocked clients for this key, in `@db.blocked_clients`. Clients are added with right push operations, so we use `left_pop` here to get them in order of insertions. The first client to block on this list will be processed first. We also get the recently created list that will pop elements from to unblock clients.

For each client, we call `handle_client`. In `handle_client` we pop an element from the list, according to the operation that the client blocked with, either `left_pop` or `right_pop`, and we send the response back to the client. In case of a failure, we return the element back to the list and disconnect the client.

Back in `BlockedClientHandler#handler`, we accumulate a list of clients that were unblocked, which will in turn be returned to the `Server` class. This list is used to call `unblock_client`, which will flag the client to be processed in case there were accumulated commands sent while blocked.
We then check if the list is empty, if it is, we cannot unblock anymore clients and exit the iteration. Otherwise we continue and unblock the next client. `unblock_client` also takes care of cleaning up the server state. Now that a client is unblocked, it should not be in any of the lists in `@db.blocking_keys`. We also need remove the entry in the sorted array of timeouts.

If we were able to unblock all the clients, we remove the key from `blocking_keys`.

With all these changes, we can implement the similar `BRPopCommand` class:

``` ruby
module BYORedis
  # ...
  class BRPopCommand < BaseCommand
    def call
      ListUtils.common_bpop(@db, @args, :rpop) do |list_name, list|
        @db.right_pop_from(list_name, list)
      end
    end

    def self.describe
      Describe.new('brpop', -3, [ 'write', 'noscript' ], 1, -2, 1,
                   [ '@write', '@list', '@slow', '@blocking' ])
    end
  end
end
```

The implementation is almost identical to `BLPopCommand`. The last class to add is `BRPopLPushCommand`:

``` ruby
module BYORedis
  module ListUtils
    # ...
    def self.common_rpoplpush(db, source_key, destination_key, source)
      if source_key == destination_key && source.size == 1
        source_tail = source.head.value
      else
        destination = db.lookup_list_for_write(destination_key)
        source_tail = db.right_pop_from(source_key, source)
        destination.left_push(source_tail)
      end

      RESPBulkString.new(source_tail)
    end
  end
  # ...
  class RPopLPushCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)

      source_key = @args[0]
      source = @db.lookup_list(source_key)

      if source.nil?
        NullBulkStringInstance
      else
        destination_key = @args[1]
        ListUtils.common_rpoplpush(@db, source_key, destination_key, source)
      end
    end
    # ...
    end
  end
  # ...
  class BRPopLPushCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      source_key = @args[0]
      source = @db.lookup_list(source_key)
      timeout = OptionUtils.validate_float(@args[2], 'timeout')
      destination_key = @args[1]

      if source.nil?
        BYORedis::Server::BlockedState.new(ListUtils.timeout_timestamp_or_nil(timeout),
                                           [ source_key ], :rpoplpush, destination_key)
      else
        ListUtils.common_rpoplpush(@db, source_key, destination_key, source)
      end
    end

    def self.describe
      Describe.new('brpoplpush', 4, [ 'write', 'denyoom', 'noscript' ], 1, 2, 1,
                   [ '@write', '@list', '@slow', '@blocking' ])
    end
  end
end
```

The code here is almost identical to the one in `RPopLPushCommand`, with the difference that it returns `BlockedState` instance if the source list does not exist. One difference with the previous two commands is that we need to store the target of the pop, the list in which we'll push the element to.

We also extracted some shared logic between `RPopLPushCommand` & `BRPopLPushCommand` to `ListUtils.common_rpoplpush` to prevent code repetition. This shared code is important as it handles the edge case where both `source` & `destination` are the same list and only contains one element.

## Tests

We added a few more test files in this chapter, and placed all of them in the `test/` sub folder, we also added and a task in `Rakefile` to run all the tests:

``` ruby
require 'rake/testtask'

Rake::TestTask.new do |t|
  t.pattern = 'test/*test.rb'
end
```
_listing 7.67 Rakefile content_

The new test files allow us to easily run a sub section of the test suite, the following is a list of the new files:

- `list_test.rb`
- `dict_unit_test.rb`
- `command_test.rb`
- `sorted_array_unit_test.rb`
- `list_unit_test.rb`

We can run all the tests with `rake test`, or single files with `ruby list_test.rb` for instance.

## Conclusion

In this chapter we added full list support to our Server. We also added the `TYPE` command which allows us to inspect the type of values added to the database. The current types are `string`, `list` and `none` if the given key does not exist.

We also made a lot of changes to the server to support blocking commands.

In the next chapter we'll add support for another Redis data type, Hashes.

### Code

You can find the code [on GitHub][code-github]

## Appendix A: Ziplist Deep Dive

Ziplists are implemented in the [`ziplist.h`][redis-source-ziplist-h] & [`ziplist.c`][redis-source-ziplist-c] files. The  [`ziplist.c`][redis-source-ziplist-c] file contains a great explanation of how ziplists work. In this appendix we provide a few more details as well as some examples.

Ziplists were added to Redis in 2014 by Twitter engineers who apparently needed a more compact data structure to store Twitter timelines, essentially lists of ids. You can read more about it on the [Pull Request page on GitHub][ziplist-gh-pr] and on the blog of one of the authors [here][ziplist-part-1] and [there][ziplist-part-2].

A ziplist is a contiguous chunk of memory, that can store integers and strings, with the following layout:

```
<zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
```

- `zlbytes` is an unsigned 32-bit integer (uint32_t), which describes the number of bytes used by the ziplist, including itself.
- `ztail` is also an unsigned 32-bit integer, which is an offset to the last element in the list. It serves a purpose similar to `@tail` in the implementation used in this chapter.
- `zllen` is an unsigned 16-bit integer (uint16_t), it keeps count of the number of entries. Because it is an unsigned 16 bit integer, its maximum value is 65,535 (2^16 - 1). In order to allow ziplist to hold more elements, this ziplist implementation knows that the maximum value that can be described by this integer is 65,534 (2^16 - 2), and if the value of the integer is 65,535, it will scan the whole list to count the number of items. In practice, Redis caps the size of the ziplists it creates, the default is 2Kb, and this scenario is extremely unlikely to happen.
- `zlend` is a single byte (uint8_t), and is set to 255, the maximum value of a byte, 2^8 - 1, or `1111 1111` / `FF` in binary and hexadecimal representations respectively.

The following elements are the actual entries in the list. Each entry has the following layout:

```
<prevlen> <encoding> <entry-data>
```

- `prevlen` represents the length of the previous entry, allowing for right to left. It serves a purpose similar to the `prev_node` attribute in the list used in this chapter, but uses a dynamic size and will often be smaller. If the length of the previous entry in bytes is smaller than 254 bytes, prevlen will be stored in a single byte (uint8_t). The maximum value of a uint8_t is 255 (2^8 - 1). Otherwise, it will consumes 5 bytes, the first byte will be set to 254, the maximum value of a byte, and the following. As a reminder, 255 cannot be used since this value is used to flag the end of the list. This means that an entry cannot have a length greater than what can fit in a 32 bit integer, since the length is stored over 4 bytes, 32 bits. This sets the maximum length of a string stored by a ziplist to 4,294,967,295 (2^32 - 1). Which is, admittedly, a very large string.
- `encoding` describes how the entry data is actually stored. It will take either one, two or five bytes. The first two bits of the encoding determines the type, `11` means an integer, and anything else, that is either `00`, `01` or `10`, means a string.

**String encoding**

- If the encoding starts with `00`, then the encoding itself will only occupy a single byte. This means that we can only use the remaining 6 bits of the encoding byte to encode the length of the string. The maximum value that can be encoded with 6 bits is 63 (2^6 - 1). This means that if the encoding byte is `0000 0001`, the string that is stored has a length of 1. With `0000 0100`, the string has a length of 4, and with `0011 1111`, it has a length of 63.
- If the encoding starts with `01`, then the encoding occupies two bytes, which leaves us with 14 bits, the 6 bits of the first byte, and the 8 bits of the second bytes. This encoding can describe a string with a length up to 16,383 (2^14 - 1). As an examples, `0100 0001 0000 0000` describes a string of length 256.
- If the encoding starts with `10`, then the encoding will use five bytes. The 6 extra bits of the first byte are not used since we can use the other four bytes to hold a 32-bit value, which is the maximum length of a string in a ziplist. This encoding will only be used if strings have a length greater than 16,383.

**Integer encoding**

There are six variants of the integer encoding:

- If the encoding is `1100 0000`, it describes a signed integer that occupies 2 bytes (int16_t), which value can go up to 32,767 (2^15 - 1)
- If the encoding is `1101 0000`, it describes a signed integer that occupies 4 bytes (int32_t), which value can go up to 2,147,483,648 (2^31 - 1)
- If the encoding is `1110 0000`, it describes a signed integer that occupies 8 bytes (int64_t), which value can go up to 18,446,744,073,709,551,616 (2^63 - 1)
- If the encoding is `1111 0000`, it describes a signed integer that occupies 3 bytes (24 bits), which value can go up to 8,388,607 (2^23 - 1)
- If the encoding is `1111 1110`, it describes a signed integer that occupies 1 byte (8 bits), which value can go up to 128 (2^7 - 1)
- Finally, if the first four bits are `1111`, then the last four bits are used to hold an unsigned integer value itself. This means that technically we'd be bound to storing integers from 0 up to 15 (2^4 - 1), but the reality is actually more restrictive. We cannot store 0 because `1111 0000` means the 3 byte encoding described above. We also cannot store 15, because `1111` would make the whole byte `1111 1111` and this value, 255, is reserved for the end of list marker. We also cannot store 14, `1110`, because `1111 1110` means the 1 byte encoding described above. This narrows the range of possible values from 1 (`0001`) to 13 (`1101`). Redis actually applies an offset here, to allow numbers from 0 to 12 to be stored with this encoding, so if you store 0 in a Ziplist, it actually stores 1, if you store 12, it actually stores 13. Storing -1 or 13 would require the next encoding type, the one using a full extra byte.

**The `DEBUG ZIPLIST` command**

Redis provides a few commands for debug purposes. One of them prints details about the ziplist if the key is itself stored as a ziplist. As it turns out, a list is never stored as a ziplist, it is always a quicklist of ziplists. But other Redis data types can use ziplists, for instance, a hash with a few items, less than 512 entries and the values are shorter than 64 bytes. Note that depending on how you run Redis on your machine, things will be different. The `DEBUG ZIPLIST` command does not print the results in the REPL but instead prints them on standard out. I often run a manually built from source Redis, which makes it easy to get the logs since they are by default printed to the terminal where you started the server, but if you run Redis through homebrew, the logs will be located somewhere else.

Let's try it:

```
127.0.0.1:6379> hset foo name pierre
(integer) 1
127.0.0.1:6379> DEBUG ZIPLIST foo
Ziplist structure printed on stdout
```

And on stdout:

```
{total bytes 25} {num entries 2}
{tail offset 16}
{
        addr 0x7f8f1350d8ca,
        index  0,
        offset    10,
        hdr+entry len:     6,
        hdr len 2,
        prevrawlen:     0,
        prevrawlensize:  1,
        payload     4
        bytes: 00|04|6e|61|6d|65|
        [str]name
}
{
        addr 0x7f8f1350d8d0,
        index  1,
        offset    16,
        hdr+entry len:     8,
        hdr len 2,
        prevrawlen:     6,
        prevrawlensize:  1,
        payload     6
        bytes: 06|06|70|69|65|72|72|65|
        [str]pierre
}
{end}
```

The previous shows an example of a ziplist containing two items, each is a string. The first entry occupies 6 bytes, and the second one eight. Let's look closer at all the bytes:

The first byte, `00`, represents the length of the previous item, which is zero since this is the head of the list. The second byte is `04`, which is the first encoding byte. The encoding can be between one and 5 bytes, so we need to look at the values, bit by bit, to determine which encoding it is, which will tell us how the data is stored. `04` has the following binary representation: `0000 0100`. Looking at the "String encoding" section above, this fits in the first bullet point, a string described with a single byte, because it starts with `00`. We then need to look at the other six bytes to determine the length of the string, `000100` is the number four, so the string has a length of four.

The last four bytes in the first entry are `00`, `04`, `6e`, `61`, `6d` & `65`. The string stored is `name`, which holds four bytes: `6e`, `61`, `6d` & `65`. The values are the ascii representation of the letters `n`, `a`, `m` & `e`.

We need to continue as long as we don't encounter the `FF` byte, or `255`. The next six bytes are `06`, `06`, `70`, `69`, `65`, `72`, `72` & `65`.

The first byte is the length of the previous item, `06`, the hex representation of the number 6, which is what we found when looking at the previous entry, so far so good. The second byte is the first byte of the encoding, `06`, or `0000 0110` in binary. Similarly as for the previous string, `00` tells us it's a string with a length lower than 63, and we can see it has a length of 6. The next six bytes represent the letters `p`, `i`, `e`, `r`, `r`, & `e` in ASCII and the whole entry has a length of 8.

We can now see why the `total bytes` value is 25, 4 for the `zlbytes` field, 4 for the `zltail` field, 2 for the `zllen` field, 6 for the first entry and 8 for the second entry and 1 for the end of list marker:
`4 + 4 + 2 + 6 + 8 + 1 = 25`. A basic doubly linked list would have required 20 bytes for the first node, 2 8-byte pointers and 4 bytes of data, 22 bytes for the second node, 2 8-byte pointers and 6 bytes of data, as well as 16 bytes for the head and tail pointers, for a grand total of 62. In this small example a ziplist is less than half the size!

The problem with ziplists is that adding and removing element require a reallocation of memory, and insertions/deletions in the middle of the list become expensive as the list grows since the whole list needs to be rearranged.

The `tail offset` field is 16 since the list header occupies 10 bytes, 4, 4 & 2, and the first item takes 6, that's 16.

Let's look at two more examples, one with an integer, and one with a string using a different encoding:

```
127.0.0.1:6379> hset foo age 30
(integer) 1
127.0.0.1:6379> DEBUG ZIPLIST foo
```

The following is the debug information, the first two entries are the same as previously and were omitted.

```
{total bytes 33} {num entries 4}
{tail offset 29}
...
{
        addr 0x7f8681004338,
        index  2,
        offset    24,
        hdr+entry len:     5,
        hdr len 2,
        prevrawlen:     8,
        prevrawlensize:  1,
        payload     3
        bytes: 08|03|61|67|65|
        [str]age
}
{
        addr 0x7f868100433d,
        index  3,
        offset    29,
        hdr+entry len:     3,
        hdr len 2,
        prevrawlen:     5,
        prevrawlensize:  1,
        payload     1
        bytes: 05|fe|1e|
        [int]30
}
{end}
```

Looking at the new bytes, `08` is the length of the previous entry, 8, `03` is the encoding for `age`, a string of length 3, and the three following bytes are the three letters. For the last entry, the length of the previous entry is 5, and the encoding is `fe`, or `1111 1110` in binary. This is the one byte integer encoding, which makes sense given that 30 is too big to use the "encoded within encoding header" approach, where the max value is 12 and is small enough to fit within a single byte, where the max value is 127. If we had set the age to 128, the bytes would have been `05`, `c0`,`80` & `00`. `c0` is the hex representation of 192/`1100 0000`, which represents the 2 byte signed integer encoding, which is why it is followed by two bytes, `80` & `00`. `80` is the hex representation of `1000 0000`, aka 128. Note that 128 is encoded as `80` `00` and not `00` `80`. This is because Redis represents integers as little endian in Ziplists, the least significant bytes come first. There are two exceptions where integers are stored in big endian, for two of the different string encoding, the one using two bytes and the one using five bytes.

Let's now look at a sixty-four byte long string:

```
127.0.0.1:6379> hset foo 64-char-string aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
(integer) 0
127.0.0.1:6379> DEBUG ZIPLIST foo
```

The following is the debug information:

```
{total bytes 116} {num entries 6}
{tail offset 48}
...
{
        addr 0x7f867f705880,
        index  4,
        offset    32,
        hdr+entry len:    16,
        hdr len 2,
        prevrawlen:     3,
        prevrawlensize:  1,
        payload    14
        bytes: 03|0e|36|34|2d|63|68|61|72|2d|73|74|72|69|6e|67|
        [str]64-char-string
}
{
        addr 0x7f867f705890,
        index  5,
        offset    48,
        hdr+entry len:    67,
        hdr len 3,
        prevrawlen:    16,
        prevrawlensize:  1,
        payload    64
        bytes: 10|40|40|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|61|
        [str]aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...
}
{end}
```

Looking at the last entry, we see that the length of the previous entry is `10`, which the hexadecimal representation of the number 16. As a quick reminder, this is because the two characters represent 4 bits each, `1` represents `0001` and 0 is `0000`, and the byte `0001 0000` is the value 16, 2^4. The string `"64-char-string"` has 14 characters, and there are two more bytes, one of the `prevlen` field, and one for the encoding. That's 16. The encoding takes two bytes for the last entry, `40` & `40`. `40` is the representation of the number 64, or `0100 0000` in binary.

The first two bits of the encoding are `01`, so we know that the overall encoding will use two bytes, and the length of the string will use 14 of the 16 bytes. These 14 bits are: `00 0000 0100 000`, which is the number 64, the length of the string coming up next.

Finally, we can see that if we set a string with a length longer than 64, Redis will convert the hash from a Ziplist to a Hash table

```
127.0.0.1:6379> hset foo 64-char-string aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11
(integer) 0
127.0.0.1:6379> DEBUG ZIPLIST foo
(error) ERR Not an sds encoded string.
127.0.0.1:6379> DEBUG OBJECT foo
Value at:0x7f867f62f8e0 refcount:1 encoding:hashtable serializedlength:47 lru:7660156 lru_seconds_idle:36
```

Note that the error message is incorrect, it should say: "Not a ziplist encoded object". This was fixed in a more [recent version of Redis][gh-my-commit].

## Appendix B: Sorted Array

The following is a sorted array implementation that provides O(logn) `push/<<` & `delete`. The  implementation uses a plain Ruby array as the underlying storage, which it delegates most of its operations to.

Elements are kept sorted in the `push/<<` method by using the built-in `bsearch_index` method. Using this method, we compute the index at which the element should be added in the array and maintain the ordering.

The delete method also relies on `bsearch_index` to find the index of the elements it needs to remove. Given that multiple elements could meet the conditions, it needs to find all the indices for the elements that should be removed. `bsearch_index` will return the leftmost index of an element that should be deleted, and we keep looking to the right as look as we find matching elements and mark them for deletion.

The `SortedArray` is meant to be used to hold classes holding multiple attributes, such as `Struct`s, and order them based on one their fields. Its main use case in the `BYORedis` codebase is to store `BlockedState` instances, sorted by their timeouts.


``` ruby
module BYORedis
  class SortedArray
    def initialize(field)
      @underlying = []
      @field = field.to_sym
    end

    def push(new_element)
      if @underlying.empty?
        index = 0
      else
        index = @underlying.bsearch_index do |element|
          element.send(@field) >= new_element.send(@field)
        end
      end

      index = @underlying.size if index.nil?
      @underlying.insert(index, new_element)
    end
    alias << push

    def [](index)
      @underlying[index]
    end

    def size
      @underlying.size
    end

    def shift
      @underlying.shift
    end

    def delete_if(&block)
      @underlying.delete_if(&block)
    end

    def delete(element)
      index = @underlying.bsearch_index { |x| x.send(@field) >= element.send(@field) }
      return if index.nil?

      element_at_index = @underlying[index]
      indices_for_deletion = []
      while element_at_index
        if element_at_index == element
          indices_for_deletion << index
        end

        index += 1
        next_element = @underlying[index]
        if next_element && next_element.send(@field) == element.send(@field)
          element_at_index = next_element
        else
          break
        end
      end

      indices_for_deletion.each { |i| @underlying.delete_at(i) }
    end
  end
end
```
_listing 7.68 The SortedArray class_

The following is a test suite for the two main methods of the `SortedArray` class, `push/<<` & `delete`:

``` ruby
# coding: utf-8

require_relative './test_helper'
require_relative './sorted_array'

describe BYORedis::SortedArray do
  TestStruct = Struct.new(:a, :timeout)

  describe 'push/<<' do
    it 'appends elements while keeping the array sorted' do
      sorted_array = new_array(:timeout)

      sorted_array << TestStruct.new('a', 1)
      sorted_array << TestStruct.new('b', 2)
      sorted_array << TestStruct.new('c', 10)
      sorted_array << TestStruct.new('d', 20)
      sorted_array << TestStruct.new('e', 15)
      sorted_array << TestStruct.new('f', 8)

      assert_equal(6, sorted_array.size)
      assert_equal(1, sorted_array[0].timeout)
      assert_equal(2, sorted_array[1].timeout)
      assert_equal(8, sorted_array[2].timeout)
      assert_equal(10, sorted_array[3].timeout)
      assert_equal(15, sorted_array[4].timeout)
      assert_equal(20, sorted_array[5].timeout)
    end
  end

  describe 'delete' do
    it 'deletes the element from the array' do
      sorted_array = new_array(:timeout)

      sorted_array << TestStruct.new('a', 10)
      sorted_array << TestStruct.new('b1', 20)
      sorted_array << TestStruct.new('b2', 20)
      sorted_array << TestStruct.new('b3', 20)
      sorted_array << TestStruct.new('c', 30) # array is now a, b3, b2, b1, c

      sorted_array.delete(TestStruct.new('d', 40)) # no-op
      sorted_array.delete(TestStruct.new('b1', 20))

      assert_equal(4, sorted_array.size)
      assert_equal(10, sorted_array[0].timeout)
      assert_equal(TestStruct.new('b3', 20), sorted_array[1])
      assert_equal(TestStruct.new('b2', 20), sorted_array[2])
      assert_equal(30, sorted_array[3].timeout)
    end
  end

  def new_array(field)
    BYORedis::SortedArray.new(field)
  end
end
```
_listing 7.69 Basic unit tests for the SortedArray class_

[code-github]:https://github.com/pjambet/redis-in-ruby/tree/master/code/chapter-7
[list-commands-docs]:https://redis.io/commands#list
[redis-data-types-doc]:https://redis.io/topics/data-types-intro
[chapter-6]:/post/chapter-6-building-a-hash-table/
[redis-streams-doc]:https://redis.io/topics/streams-intro
[postgres-page-layout]:https://www.postgresql.org/docs/current/storage-page-layout.html
[redis-doc-type-command]:http://redis.io/commands/type
[redis-source-db-type]:https://github.com/antirez/redis/blob/6.0.0/src/server.h#L640-L653
[template-method-pattern]:https://en.wikipedia.org/wiki/Template_method_pattern
[appendix-b]:#appendix-b-sorted-array
[appendix-a]:#appendix-a-ziplist-deep-dive
[wikipedia-radix-tree]:https://en.wikipedia.org/wiki/Radix_tree
[wikipedia-queues]:https://en.wikipedia.org/wiki/Queue_(abstract_data_type)
[wikipedia-stacks]:https://en.wikipedia.org/wiki/Stack_(abstract_data_type)
[connection-pool-gh]:https://github.com/mperham/connection_pool
[hikari-cp-gh]:https://github.com/brettwooldridge/HikariCP
[redis-source-ziplist-h]:https://github.com/antirez/redis/blob/6.0.0/src/ziplist.h
[redis-source-ziplist-c]:https://github.com/antirez/redis/blob/6.0.0/src/ziplist.c
[gh-my-commit]:https://github.com/redis/redis/commit/d52ce4ea1aa51457aed1d63a5bf784f94b2768c3
[ziplist-gh-pr]:https://github.com/redis/redis/pull/2143
[ziplist-part-1]:https://matt.sh/redis-quicklist
[ziplist-part-2]:https://matt.sh/redis-quicklist-visions
[redis-doc-incr]:https://redis.io/commands/incr
