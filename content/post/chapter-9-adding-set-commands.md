---
title: "Chapter 9 Adding Set Commands"
date: 2020-11-10T09:01:31-05:00
lastmod: 2020-11-10T09:01:42-05:00
draft: false
comment: false
keywords: []
summary:  "In this chapter we add support for one more data type, Sets. We implement most of the SET commands such as SADD, SINTER, SUNION and SDIFF"
---

## What we'll cover

Our server now supports Strings, albeit not all string commands are implemented, Lists and Hashes. Redis supports three more native data types, Sets, Sorted Sets and Streams. Streams being a recent addition to Redis, and being a fairly complicated topic, will not be covered in this book. Other data types exists, such as Bitmaps, HyperLogLog and Geospatial items, but all of these are implemented on top of the String native type.

In this chapter we will add support for the Set type. Conceptually a Set is a collection of unique items, and can therefore not contain duplicates. A common operation for Sets, beside the ability to add items to a set, is testing for the presence of an item. Such operation would ideally require constant time, that is a complexity of O(1).

Wikipedia defines [a set][wikipedia-set-type] as:

> In computer science, a set is an abstract data type that can store unique values, without any particular order.

Sets, in computer science are very similar to [Finite Sets][wikipedia-finite-sets] in Mathematics. Redis Sets only store strings.

Given these constraints, it looks like our `Dict` data structure would be a great fit to implement an API for Sets. The `Dict` class stores key/value pairs, where keys are strings, so we wouldn't even need to add any values in the dict, adding key/value pairs with `nil` values would be sufficient.

It is interesting to consider that many data structures can be used to provide a set API. For instance Ruby arrays can be used as set with the following methods:

``` ruby
def new_set
  []
end

def include?(set, member)
  set.include?(member)
end

def add(set, member)
  if include?(set, member)
    false
  else
    set << member
    true
  end
end
```
_listing 9.1 A Set using a Ruby array_


These three methods do provide the API for a set, but the performance, especially in the worst case scenario, will degrade quickly as the set grows. The `include?` method needs to iterate over the entire array to return `false`, so calling `include?` for a member that is not in the array, will _always_ require a full iteration of the array.

The `List` class we created in [Chapter 7][chapter-7] could also be used to implement a set but would suffer from the exact same performance issues.

Ruby has a [`Set` class][ruby-set-class], but we will not use it to follow the "build from scratch" approach we've been using so far.

Redis supports [sixteen set commands][redis-set-commands]:

- **SADD**: Add one or more members to a set, creating it if necessary
- **SCARD**: Return the _cardinality_ of the set, the number of members
- **SDIFF**: Compute the difference of sets, from left to right
- **SDIFFSTORE**: Same as SDIFF but store the result in another key instead of returning it
- **SINTER**: Compute the intersection of all given sets
- **SINTERSTORE**: Same as `SINTER` but store the result in another key instead of returning it
- **SUNION**: Compute the union of all given sets
- **SUNIONSTORE**: Same as `SUNION` but store the result in another key instead of returning it
- **SISMEMBER**: Return `0` or `1` depending on whether or not the given member is in the given set
- **SMEMBERS**: Return all the members of the given set
- **SMISMEMBER** (_new in 6.2.0_): Variadic version of `SISMEMBER`, that is, it accepts multiple arguments and returns an array
- **SMOVE**: Move a member from one set to another
- **SPOP**: Remove _a_ member from the given set
- **SRANDMEMBER**: Return _a_ member from the given set, but leave it in the set
- **SREM**: Remove the given member from the given set
- **SSCAN**: Similar to `SCAN`, `HSCAN` & `ZSCAN`. We will not implement it for the same reason we did not implement `HSCAN` in [Chapter 8][chapter-8]

## How does Redis do it?

As mentioned in the previous chapter, the set commands are implemented in the [`t_set.c`][redis-src-tset] file, in which Redis uses two different data structures to store the data. As long as the elements, also called members in a set, can be represented as integers, and that the size of the set if below the [`set-max-intset-entries`][redis-config-max-intset-entries] config value, which has a default value of `512`, Redis uses an intset, and otherwise uses the dictionary from `dict.c`. We already created the `Dict` class in [Chapter 6][chapter-6], but the intset is a new data structure.

Using an `IntSet` provides two interesting benefits, the memory footprint of the set is smaller, and most set operations will be faster than with a `dict` when the number of members is small. Note that while sets do not have to be ordered, int sets are actually ordered, which is how it provides a reasonably fast way to check for the existence of an element in a set, through binary search.

Everything is a string in a `dict`, this means that the number `1` would be stored as the character `'1'`, which uses one byte of memory, eight bits, and the number `1,000` would be stored as the string `'1000'` and use four bytes, thirty two bits. This means that large numbers, such as, `1,000,000,000,000`, one trillion, which is composed of thirteen digits, would use thirteen bytes, one hundred and four bits.

Storing these values as 64-bit integers would improve things for large numbers, one trillion would only require sixty four bits instead of one hundred and four, but it would actually make things worse for small numbers. If we were to store the number one as a 64-bit integer, it would use sixty four bits instead of eight if we stored it as a character.

Redis' `intset` type uses an `encoding` variable, which determine the size of the integers stored. Because the `intset` structure uses an array, it has to use the same underlying type for all elements. This is what allows indexed access in O(1) time. If we want to access the element at index `i`, we can multiply `i` by the size of the variables stored in the array, and we'll land on the i-th element. The content of the `intset` is an array of `int8_t`, so that each element uses exactly one byte. This allows Redis to store all the integers in this array, in 8-bit chunks, the actual length of each integer is determined by the encoding of the `intset`.

Redis uses three possible encoding values, `int16_t`, `int_32_t` & `int64_t`, respectively 16-bit integers, 32-bit integers, and 64-bit integers. When an `intset` is created, its encoding is set to 16-bit integers. Whenever new members are added to the set, Redis tries to find the smallest encoding that can hold the value. `int16_t` can range from `-2^15` to `2^15 - 1`, `-32,768` to `32,767`, `int32_t` from `-2^31` to `2^31 - 1`, `-2,147,483,648` to `2,147,483,647` and `int64_t` from `-2^63` to `2^63 - 1`, `-9,223,372,036,854,775,808` to `9,223,372,036,854,775,807`.

With this approach, storing numbers between `0` & `9` would still be more expensive, because they would be store as 16-bit integers, but we're already breaking even for negative numbers from `-1` to `-9`, and we're saving space for numbers lower than or equal to `-10`, which would use three bytes as strings, and only use two as 16-bit integers, and for numbers greater than or equal to `100`. The benefits become even greater as the number grow in either direction.

Speed wise, the benefits are the same that what we discussed in the previous chapter in the ziplist vs dict section. Using an `intset` will gradually become slower as the `intset` grows, but all operations will be really fast when the number of members is small. Large `intset` structure suffer from the same issues than ziplists and become slow to manipulate as they grow, because the whole memory chunk needs to be reallocated and items within it moved, to make space for new elements.

The intset functionalities are implemented in [`intset.c`][redis-src-intset]. An intset is essentially a sorted array of integers. We already implemented a sorted array, the `SortedArray` class, but the requirements are a little bit different here so we will create an `IntSet` class to mimic the Redis behavior.

As mentioned in the previous section, a dictionary provide a solid foundation to implement the Set API. It turns out that this is exactly what Redis does. When handling the `SADD` command, if the underlying data structure is a `dict`, it calls the `dictAdd` function with the member as the key, and nothing as the value, this is done in the [`setTypeAdd` function][redis-src-dictadd].

Our `Dict` class already exists, and will only require a few changes, to make sure that it handles entries with `nil` values without any hiccups, on the other hand, we need to build an `IntSet` class from scratch, so let's get to it.

### The IntSet data structure

Because the intset structure uses a sorted array, this makes the time complexity of a lookup O(logn), which will be worse than O(1) when the array gets bigger. Additionally, because arrays are contiguous chunks of memory, inserting an element requires a lot of work, to shift all the elements to the right of the new element. This makes arrays more and more expensive to manipulate as they grow in size, and explains why Redis only uses it if sets contain `512` items or less.

Redis stores all values in little endian in an intset. This means that the number `1`, which has the following binary representation as a 16-bit integer:

``` ruby
irb(main):025:0> ('%016b' % 1).chars.each_slice(8).map { |byte| byte.join }.join(' ')
=> "00000000 00000001"
```

The two bytes, `\x00` & `\x01` need to be reversed in little endian, because the least significant bytes come first, so the bytes of `1`, in little endian are `[ "\x01", "\x00" ]`.

Let's look at a larger example, the representation of `1,000,000` as a 32-bit integer:

``` ruby
irb(main):027:0> ('%032b' % 1_000_000).chars.each_slice(8).map { |byte| byte.join }.join(' ')
=> "00000000 00001111 01000010 01000000"
```

The big endian bytes are `[ "\x00", "\x0F", "\x42", "\x40" ]`, and `[ "\x40", "\x42", "\x0F", "\x00 ]` as little endian.

Another way to play with these values is to use the [`Array#pack`][ruby-doc-array-pack] method, with the `l` format, which is the format for signed 32-bit integer, which uses the native endianness of the platform by default, but can be forced to use big endian with `l>` and little endian with `l<`:

``` ruby
irb(main):043:0> [1_000_000].pack('l')
=> "@B\x0F\x00"
irb(main):044:0> [1_000_000].pack('l>')
=> "\x00\x0FB@"
irb(main):045:0> [1_000_000].pack('l<')
=> "@B\x0F\x00"
```

This example illustrates that the default endianness of my machine, a macbook pro, is little endian. Note that some of these bytes don't look like the others, `"@"` & `"B"` vs `"\x0F"` and `"\x00"`. This is because Ruby attempts to display the characters as ASCII by default, and it turns out that `"\x42"` is the hex representation of the decimal `66`, the character `'B'` in ASCII and `"\x40"` is the hex representation of the decimal `64`, the character `'@'` in ASCII.

Our class will implement the following public methods:

- `add(member)`
- `cardinality`
- `each`
- `contains?(member)`
- `members`
- `pop`
- `random_member`
- `empty?`
- `remove(member)`

The methods above will allow us to implement all the Set related commands. All the following methods are implemented in the `int_set.rb` file, under the `BYORedis::IntSet` class, let's start with the constructor and the `add` method:

``` ruby
module BYORedis
  class IntSet

    INT16_MIN = -2**15 # -32,768
    INT16_MAX = 2**15 - 1 # 32,767
    INT32_MIN = -2**31 # -2,147,483,648
    INT32_MAX = 2**31 - 1 # 2,147,483,647
    INT64_MIN = -2**63 # -9,223,372,036,854,775,808
    INT64_MAX = 2**63 - 1 # 9,223,372,036,854,775,807

    # Each of the constant value represents the number of bytes used to store an integer
    ENCODING_16_BITS = 2
    ENCODING_32_BITS = 4
    ENCODING_64_BITS = 8

    def initialize
      @underlying_array = []
      @encoding = ENCODING_16_BITS
    end

    def add(member)
      raise "Member is not an int: #{ member }" unless member.is_a?(Integer)

      # Ruby's Integer can go over 64 bits, but this class can only store signed 64 bit integers
      # so we use this to reject out of range integers
      raise "Out of range integer: #{ member }" if member < INT64_MIN || member > INT64_MAX

      encoding = encoding_for_member(member)

      return upgrade_and_add(member) if encoding > @encoding

      # search always returns a value, either the position of the item or the position where it
      # should be inserted
      position = search(member)
      return false if get(position) == member

      move_tail(position, position + 1) if position < size

      set(position, member)

      true
    end

    private

    def set(position, member)
      @encoding.times do |i|
        index = (position * @encoding) + i
        @underlying_array[index] = ((member >> (i * 8)) & 0xff).chr
      end
    end

    def move_tail(from, to)
      @underlying_array[(to * @encoding)..-1] = @underlying_array[(from * @encoding)..-1]
    end

    def search(member)
      min = 0
      max = size - 1
      mid = -1
      current = -1

      # the index is always 0 for an empty array
      return 0 if empty?

      if member > get(max)
        return size
      elsif member < get(min)
        return 0
      end

      while max >= min
        mid = (min + max) >> 1
        current = get(mid)

        if member > current
          min = mid + 1
        elsif member < current
          max = mid - 1
        else
          break
        end
      end

      if member == current
        mid
      else
        min
      end
    end

    def get(position)
      get_with_encoding(position, @encoding)
    end

    def get_with_encoding(position, encoding)
      return nil if position >= size

      bytes = @underlying_array[position * encoding, encoding]

      # bytes is an array of bytes, in little endian, so with the small bytes first
      # We could iterate over the array and "assemble" the bytes into in a single integer,
      # by performing the opposite we did in set, that is with the following
      #
      # bytes.lazy.with_index.reduce(0) do |sum, (byte, index)|
      #   sum | (byte << (index * 8))
      # end
      #
      # But doing do would only work if the final result was positive, if the first bit of the
      # last byte was a 1, then the number we're re-assembling needs to be a negative number, we
      # could do so with the following:
      #
      # negative = (bytes[-1] >> 7) & 1 == 1
      #
      # And at the end of the method, we could apply the following logic to obtain the value,
      # get the 1 complement, with `~` and add 1. We also need to apply a mask to make sure that
      # the 1 complement result stays within the bounds of the current encoding
      # For instance, with encoding set to 2, the mask would be 0xffff, which is 65,535
      #
      # if negative
      #   mask = (2**(encoding * 8) - 1)
      #   v = -1 * ((~v & mask) + 1)
      # end
      #
      # Anyway, we can use the pack/unpack methods to let Ruby do that for us, calling
      # bytes.pack('C*') will return a string of bytes, for instance, the number -128 is stored
      # in the intset as [ 128, 255 ], calling, `.pack('C*')` returns "\x80\xFF". Next up, we
      # pick the right format, 's' for 16-bit integers, 'l' for 32 and 'q' for 64 and we let
      # Ruby put together the bytes into the final number.
      # The result of unpack is an array, but we use unpack1 here, which is a shortcut to
      # calling unpack() followed by [0]
      #
      # What this whole thing tells us is that we could have used `.pack('s').bytes` in the
      # set method, but using >> 8 is more interesting to understand actually what happens!
      format = case encoding
               when ENCODING_16_BITS then 's'
               when ENCODING_32_BITS then 'l'
               when ENCODING_64_BITS then 'q'
               end

      bytes.join.unpack1(format)
    end

    def encoding_for_member(member)
      if member < INT32_MIN || member > INT32_MAX
        ENCODING_64_BITS
      elsif member < INT16_MIN || member > INT16_MAX
        ENCODING_32_BITS
      else
        ENCODING_16_BITS
      end
    end

    def upgrade_and_add(member)
      current_encoding = @encoding
      current_size = size
      new_size = current_size + 1
      @encoding = encoding_for_member(member)

      prepend = member < 0 ? 1 : 0
      @underlying_array[(new_size * @encoding) - 1] = nil # Allocate a bunch of nils

      # Upgrade back to front
      while (current_size -= 1) >= 0
        value = get_with_encoding(current_size, current_encoding)
        # Note the use of the prepend variable to shift all elements one cell to the right in
        # the case where we need to add the new member as the first element in the array
        set(current_size + prepend, value)
      end

      if prepend == 1
        set(0, member)
      else
        set(size - 1, member)
      end

      true
    end
  end
end
```
_listing 9.2 The `IntSet#add` method, and all the private methods it requires_

The `add` method starts with a few checks to make sure that we can indeed add the given member to the set. If the value is not an integer or is out of range, we reject it right away. The next step is to find the smallest encoding that can fit `member`. Once we found the encoding, either `2`, `4` or `8`, we compare it to the current encoding of the set. If the new encoding is greater, then we know that `member` is either going to be the smallest entry in the set, or the largest, because it would otherwise have used the same encoding.

The `upgrade_and_add` method takes care of migrating all current elements to the new encoding, and inserts the new member, either at index `0` or as the last element in the array.

Let's look at the three main steps of the `upgrade_and_add` method, first it increases the size of the array, with `@underlying_array[(new_size * @encoding) - 1] = nil`, which pads the array to the right with `nil` values until the new size, let's look at an example:

``` ruby
irb(main):029:0> l = ["\x01", "\x00", "\x02", "\x00"]
```

This array is what the `@underlying_array` of an `IntSet` storing the values `1` and `2` would be. `"\x01"` and `"\x00"` is the little endian representation of the number `1` in a 16-bit integer, `0000 0001 0000 0000`. The first byte, `0000 0001`, written as `0x01` in the Ruby hex literal syntax is the byte for the number `1`, since `2^0 == 1`. The second byte, the most significant one only contains zeroes: `0000 0000`. If the number had been `258`, the two bytes would have been `0000 0010 0000 0001`. The first byte is the least significant one, `0000 0010`, these are the bits between index `0` and `7`, representing the number `2`, and the most significant one is `0000 0001`, the bits between index `8` and `15`, representing the number `1`. Putting it all together we get `2^1 + 2^8 == 258`. Another way to write this number is `"\x02\x01"`

`"\x02"`, `"\x00"`, is the little endian representation of the number `2`, `0000 0010 0000 0000`, written as `0x0002` as a hex literal. Note that hex literals in Ruby follow the "natural" order and assume big-endian. So `0x0002` returns `1`, whereas `0x0200` returns `512`, because `0000 0010 0000 0000` is `512` in big endian, `2^9 == 512`.

If we were to increase the encoding to 32-bit, to store numbers larger than `32,767`, each number would now need to use four bytes, so the array would now need to be `12` item long, `4 bytes x 3 members`, as we can see, calling `l[11]` adds all the `nil` values:

``` ruby
irb(main):030:0> l[11] = nil
irb(main):031:0> l
=> ["\x01", "\x00", "\x02", "\x00", nil, nil, nil, nil, nil, nil, nil, nil]
```

The next step, upgrading all the elements, is done in the `while` loop. In our previous example, `current_size` would be `1`. Calling `get_with_encoding(2, 2)` would read the numbers `2` and `0`, and return `2`. Calling `set(1 + 0, 2)` would insert the four bytes representing `2` at the right place in the array. If we were to add `32,768`, then `prepend` would be `0`.

The `set` method splits the number into the number of bytes for the encoding, with the new encoding being `4`, to store numbers as 32-bit integer, we would iterate four times with the `@encoding.times` loop. In the first iteration `index` would be `(1 * 4) + 0`, `4` and the value would be `"\x01"` because `((member >> 0) 0xff).chr` returns `"\x01"` if member is `1`.

`0xFF` acts as a mask here, it is the hex literal for the number `255`, a byte of only ones, we could have written `0b11111111`, it just happens to be easier to use `0xff`, but they're identical. We use `0xff` in combination of the bitwise `AND` operator, `&`, which effectively only selects the rightmost byte of any numbers.

We call `.chr`, which is equivalent to calling `.pack('C')`. This allows us transform the `Integer` instance into a one-byte string. As we've already discussed, Ruby has a special handling for numbers, which is why it can handle numbers greater than what a 64-bit integer can handle, but it also means that we don't have any direct visibility in what is actually allocated when we have an instance of the `Integer` class. Using `.pack('C')` treats the number in the array as an 8-bit integer, a byte. What we get in return is a one-character string, representing the byte value. Let's look at an example:

``` ruby
irb(main):001:0> [1].pack('C')
=> "\x01"
irb(main):002:0> 1.chr
=> "\x01"
irb(main):003:0> [255].pack('C')
=> "\xFF"
irb(main):004:0> 255.chr
=> "\xFF"
irb(main):005:0> [128].pack('C')
=> "\x80"
irb(main):006:0> [1].pack('C')
=> "\x01"
irb(main):007:0> [255].pack('C')
=> "\xFF"
irb(main):008:0> [128].pack('C')
=> "\x80"
irb(main):009:0> [128].pack('C').size
=> 1
irb(main):010:0> [128].pack('C').chars
=> ["\x80"]
```

Even though it looks like the strings returned by `pack`/`chr` contains four characters, a double-quoted Ruby string handles the `\x` prefix in a special way, and as we can see with the last two examples, it only contains a single character.

The next iteration would give `index` the value `5`, and set the value `0` at index `5`. The last two iterations would set the value `0` for index `6` and `7`.

Back to `upgrade_and_add`, the `while` loop would run one more time with `current_size == 0` and upgrade the encoding of `1`. After the while loop the array would contain the following, the existing inters have been migrated from 16-bit integers to 32-bit integers, in little endian, which result in right padding them with bytes containing zeroes:

``` ruby
[
  "\x01", "\x00", "\x00", "\x00",
  "\x02", "\x00", "\x00", "\x00",
  nil, nil, nil, nil,
]
```

The last step of the method is to add the new member, in this example `prepend` is not set to `1` and it is inserted as the last item with `set(size - 1, member)`, where `size == 3`. `set` will split `32,768` into four bytes, which would be represented in big endian as the four bytes: `0x00`, `0x00`, `0x80`, & `0x00`:

``` ruby
# '%032b' % 32_768 =>
   MSB                           LSB    (Most Significant Byte/Least Significant Byte)
|-------| |-------| |-------| |-------|
7       0 7       0 7       0 7       0 (indices within each byte)
∨       ∨ ∨       ∨ ∨       ∨ ∨       ∨
0000 0000 0000 0000 1000 0000 0000 0000
∧                   ∧    ∧         ∧  ∧
31                  15   11        3  0 (indices within a 32-bit integer)
```

The only `1` in the previous binary number is at index `15`, `2^15 = 32,768`. The four bytes have the decimal values, `0`, `0`, `128` & `0`. The second byte from the right have the value `128` because within that byte, the only `1` is at index `7` and `2^7 = 128`.

Redis stores these numbers in little endian, so we store them in reverse order with the least significant bytes first: `[ 0, 128, 0, 0 ]`. The hex literal representation of `128` is `0x80`.

With that, the final value of `@underlying_array` is:

``` ruby
[
  "\x01", "\x00", "\x00", "\x00",
  "\x02", "\x00", "\x00", "\x00",
  "\x00", "\x80", "\x00", "\x00",
]
```

The next step in `add` is to search for `member` in the array, which we do with the `search` method. The method always returns an integer, which is either the position of `member` in the array or the position where it should be inserted.

The `search` method uses a divide and conquer approach to find the element. First, it checks if the element should go at the front of the array, if the new member is smaller than the element at index 0, or at the end of the array if it is greater than the last element in array.

If neither of these checks are true, we look at the value at index `mid`, because we know that the array is sorted, we compare that value with `member`, if it is greater than `member`, then we know that `member` will either be on the left side, or not present and we set the `max` to `mid - 1`, effectively narrowing the range to the left side of the array. We perform the opposite operation if the value at index `mid` is lower than `member`, then we want to keep searching on the right half of the array, and we do so with `min = mid + 1`. We stop iterating if neither of these checks are true, that is, if `current == member`, in which case `mid` is the index of `member`.

The other condition causing the loop to exit is if `min` becomes greater than `max`, which means that we did not find `member` in the array, in which case `min` is the index where it should be inserted. Let's look at an example to illustrate this:

``` ruby
array = [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65]
```

The variable `array` contains twelve elements, so `min` will start at `0`, `max` at `11` and `mid` & `current` at `-1`.

If we were to search for `56`, we would enter the `while` loop and set `mid` to `(min + max) >> 1`, which is `(0 + 11) >> 1`, `5`, this is because `11` is `0b1011` in binary, and shifting it to the right one time results in `101`, `2^0 + 2^2`, `5`. `array[5] == 35`, so `member > current` and we set `min` to `mid + 1`, `6`.


Using a bitwise right shift of `1` is a "trick" to divide a number by two. Let's look at an example to illustrate it. With the previous example, `min` is `0`, `max` is `11`. Given that the array has an even number of items, there's no index where there would be the same number of elements to the left and to the right, but we can still use the integer division to find an index that is close to that, `11 / 2 == 5`, so far so good.

```
   1011 (11)
>> 1
   ----
   0101 (5)
```

This property, that shifting by one to the right is the same as dividing by two happens to be the result from how we represent numbers in binary. A `1` in binary represent a power of two, so `11` is binary is really the sum of `2^0`, `2^1` & `2^3`. Shifting by one to the right returns the number that is the sum of `2^0` and `2^1`, which happens to be the result of an integer division by two.

This also works with even numbers:

```
   1100 (10)
>> 1
   ----
   0110 (6)
```

Tada!

If it feels a little bit like magic, I encourage you to experiment with different numbers, either on paper or with `irb`, it took me a little while until it "clicked".

Back to the `search` method.

On the next iteration `mid` becomes `8`, because `(6 + 11) >> 1` is `0b10001 >> 1`, which is `0b1000`, `8`. `array[8] == 50`, so we set `min` to `9`.

On the next iteration, `mid` becomes `10`, because `(9 + 11) >> 1` is `0b10100 >> 1`, `0b1010`, `10`. `array[10] == 60`, so this time we set `max` to `mid - 1`, `9`.

On the next and last iteration, `mid` is `9`, because `(9 + 9) >> 1` is `0b10010 >> 1`, `0b1001`, `9`. `array[9] == 55`, so `min` becomes `10`, and the loop exits because `min > max`.

`9` happens to be the index where `56` should be inserted to maintain the array sorted.

Reading values from the array, which is what the `get` method does, is more complicated than in a regular array. Our set members span across multiple cells in the array, because each cell contains one byte. So a member spans across two cells with the 16-bit encoding, four with the 32-bit encoding and eight with the 64-bit encoding.

The `get_with_encoding` method grabs all the bytes and uses the `unpack1` method to reconstruct the integer from its byte parts. Ruby knows how to convert the bytes based on the format string, `s` means `signed short integer`, `int16_t`, `l` means `signed long integer`, `int32_t` and `q` means `signed long long integer`, `int64_t`.

These methods give us the foundation to write the remaining methods, let's look at `each`, `members`, `empty?` & `size`:

``` ruby
module BYORedis
  class IntSet

    # ...

    def empty?
      @underlying_array.empty?
    end

    def each(&block)
      members.each(&block)
    end

    def members
      size.times.map do |index|
        get(index)
      end
    end

    def size
      @underlying_array.size / @encoding
    end
    alias cardinality size
    alias card cardinality

    private

    # ...

  end
end
```
_listing 9.3 The `empty?`, `each`, `members` and `size` methods in the `IntSet` class_

The `empty?` method relies on the `Array#empty?` method, we don't need to change anything to its default behavior. The `size` method uses `Array#size` and divides the result by the size of encoding. This is because with the 16-bit encoding each integer will be split over two elements in the array, four elements with 32-bit and eight elements with 64-bit.

`members` uses `size` to determine how many times to iterate with the `Integer#times` method and passes the index to the `get` method we wrote earlier, which knows how to reassemble multiple array items into integers. Doing this in the block to `map` returns an array of `Integer` instances.

Finally the `each` method forwards its `block` argument to `Array#each` on the result of `members`.

An important method of the `IntSet` class is the `include?` method, which we can express in terms of `search` and `get`:

``` ruby
module BYORedis
  class IntSet

    # ...

    def include?(member)
      return false if member.nil?

      index = search(member)
      get(index) == member
    end
    alias member? include?

    # ...
  end
end
```
_listing 9.4 The `IntSet#include?` method, aliased to `member?`_

`pop` and `rand_member` are very similar, we use `Kernel#rand` with `size` as the exclusive upper boundary, which returns an index we can feed to `get` to return any elements of the set. In the `pop` case we do want to remove the item from the set and we do so with `Array#slice!`. This method takes two argument, the first one is the start index of the range we wish to delete and the second one is the length of the range.

`remove` uses `Array#slice!` in a similar way to how `pop` does it:

``` ruby
module BYORedis
  class IntSet

    # ...

    def pop
      rand_index = rand(size)
      value = get(rand_index)
      @underlying_array.slice!(rand_index * @encoding, @encoding)
      value
    end

    def random_member
      rand_index = rand(size)
      get(rand_index)
    end

    def remove(member)
      index = search(member)
      if get(index) == member
        @underlying_array.slice!(index * @encoding, @encoding)
        true
      else
        false
      end
    end

    private

    # ...
  end
end
```
_listing 9.5 The `pop`, `random_member` and `remove` methods in the `IntSet` class_

And with this the `IntSet` class is now feature complete.

## Adding Set commands

### Creating a Set with `SADD`

Let's start the same way we started in the previous in chapters, with the ability to create a new element in the main keyspace. Sets are created with the `SADD` command, which usually adds members to a set, if necessary, and creates a set if the key is not already used by a value of a different type.

``` ruby
require_relative './redis_set'

module BYORedis

  class SAddCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      key = @args.shift
      new_member_count = 0

      set = @db.lookup_set_for_write(key)

      @args.each do |member|
        added = set.add(member)
        new_member_count += 1 if added
      end

      RESPInteger.new(new_member_count)
    end

    def self.describe
      Describe.new('sadd', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end
end
```
_listing 9.6 The `SAddCommand` class_

The structure of the `call` method for the `SAddCommand` class is similar to `HSet` in the previous chapter. The first argument is the key for the hash, and the following arguments are the members to add to the set. Once the set is loaded, we iterate over the argument and use the `RedisSet#add` method.

In the previous chapter we created the `RedisHash` class, so let's create the `RedisSet` class, with the `add` method:

``` ruby
module BYORedis
  class RedisSet
    attr_reader :underlying

    def initialize
      @underlying = IntSet.new
    end

    def add(member)
      case @underlying
      when IntSet
        int_member = convert_to_int_or_nil(member)
        if int_member
          added = @underlying.add(int_member)

          if added && cardinality + 1 > Config.get_config(:set_max_intset_entries)
            convert_intset_to_dict
          end

          added
        else
          convert_intset_to_dict
          @underlying.set(member, nil)
        end
      when Dict then @underlying.set(member, nil)
      else
        raise "Unknown type for structure: #{ @underlying }"
      end
    end

    private

    def convert_intset_to_dict
      dict = Dict.new
      @underlying.each do |member|
        dict[Utils.integer_to_string(member)] = nil
      end

      @underlying = dict
    end

    def convert_to_int_or_nil(member)
      Utils.string_to_integer(member)
    rescue InvalidIntegerString
      nil
    end
  end
end
```
_listing 9.7 The `RedisSet#add` method_

We are going to use the same `case/when` pattern we introduced in the previous chapter but for `IntSet` and `Dict` instead of `List` and `Dict`. In the `add` method, if `@underlying` is an `IntSet` then we start by checking if the new member is a string that represents an integer by calling the `convert_to_int_or_nil` method. This method uses the `Utils.string_to_integer` method we introduced in the previous chapter.

If `member` can be converted to an integer, then we proceed with the `IntSet` instance and call the `IntSet#add` method. If the element was added to the set, that is, if it was not already in the set, then we check the cardinality of the set to see if it exceeded the `set_max_intset_entries` config value. If it did, the set is now too big to be an `IntSet` and we convert it to a `Dict`.

Luckily the conversion from `IntSet` to `Dict`, which we perform in the `convert_intset_to_dict` private method, does not require a lot of steps. We create a new `Dict` instance, and then iterate through all the `IntSet` members with the `IntSet#each` method and use the `Dict#set` method through its `[]=` alias to add the members to the `Dict`. We only need the keys, so we set the value to `nil` for each new `DictEntry` we add.

If `@underlying` is an `IntSet`, but the new member cannot be represented as an integer, such as the string `'abc'`, then we convert the `IntSet` to a `Dict`, regardless of its current size, and add the new member with `Dict#set`. We don't use the `[]=` alias here because doing so ignores the method's return value, and we want `RedisSet#add` to return the boolean returned by `Dict#set`, indicating whether or not the member was added to the set.

Finally, if `@underlying` is a `Dict`, which would be `true` in two cases, the set is either too big to be an `IntSet`, more than 512 members by default, or a member that cannot be converted to an integer was previously added. Regardless, we're now dealing with a `Dict` and we call `Dict#set` to add the member to the set.

Back to `SAddCommand`, we now need to add the `lookup_set_for_write` method to the `DB` class:

``` ruby
module BYORedis
  class DB

    # ...

    def lookup_set(key)
      set = @data_store[key]
      raise WrongTypeError if set && !set.is_a?(RedisSet)

      set
    end

    def lookup_set_for_write(key)
      set = lookup_set(key)
      if set.nil?
        set = RedisSet.new
        @data_store[key] = set
      end

      set
    end
  end
end
```
_listing 9.8 The `DB#lookup_set_for_write` method_

Now that we introduced a new type, `set`, we need to update the `TypeCommand`:

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
      when RedisSet  then RESPSimpleString.new('set')
      else raise "Unknown type for #{ value }"
      end
    end

    # ...

  end
end
```
_listing 9.9 Adding `set` to the `TypeCommand` class_

In the next section we will add the six commands implementing set operations, namely difference, union and intersection, and their `*STORE` variants.

### Set operations with SDIFF, SINTER, SUNION and their *STORE variants

**Set difference**

``` ruby
module BYORedis

  # ...

  class SDiffCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      sets = @args.map { |other_set| @db.lookup_set(other_set) }

      RESPArray.new(RedisSet.difference(sets).members)
    end

    def self.describe
      Describe.new('sdiff', -2, [ 'readonly', 'sort_for_script' ], 1, -1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end
end
```
_listing 9.10 The `SDiffCommand` class_

The `SDIFF` command performs the "set difference" operation, starting with the leftmost set, it then iterates through all the other sets and removes their members from the first set, if it contained them, let's look at some examples:

``` bash
127.0.0.1:6379> SADD s1 2 4 6 8 10 12
(integer) 6
127.0.0.1:6379> SADD s2 1 2 3 4
(integer) 4
127.0.0.1:6379> SADD s3 5 6 7 8 9 10 11 12 13
(integer) 9
127.0.0.1:6379> SDIFF s1
1) "2"
2) "4"
3) "6"
4) "8"
5) "10"
6) "12"
127.0.0.1:6379> SDIFF s1 s2
1) "6"
2) "8"
3) "10"
4) "12"
127.0.0.1:6379> SDIFF s1 s2 s3
(empty array)
127.0.0.1:6379> TYPE s4
none
127.0.0.1:6379> SDIFF s1 s4
1) "2"
2) "4"
3) "6"
4) "8"
5) "10"
6) "12"
```

Set difference is also called relative complement in [set theory][wikipedia-set-relative-complement].

The difference of single set always returns the same set, you can compare it to subtracting `0` to any number, the result is always the same number. In the second example, we compute the difference of `s1` and `s2`, in other words we subtract all the elements in `s2` from `s1`, so we remove `1`, `2`, `3`, & `4` from `s1`. We end up with `6`, `8`, `10` & `12`. `2` & `4` were removed and `1` & `3` were ignored since they were not present in `s1`.

In the last example, we start with the same operation from the second example, we subtract `s2` from `s1`, and we subtract `s3` from that result. Removing `5`, `6`, `7`, `8`, `9`, `10`, `11`, `12` & `13` from the intermediary set containing `6`, `8`, `10` & `12` yields an empty set.

Non existing sets are treated as empty sets, which are ignored in the difference operation, similar to a subtraction by zero.

The difference operation is performed on multiple sets, and return a new set, which makes implementing it as an instance method a little bit odd since it operates on multiple sets, as opposed to a single set like most other instance methods. We therefore implement it as a class method on the `RedisSet` class:

``` ruby
module BYORedis
  class RedisSet

    # ...

    def self.difference(sets)
      first_set = sets[0]
      return RedisSet.new if first_set.nil?

      # Decide which algorithm to use
      #
      # Algorithm 1 is O(N*M) where N is the size of the element first set
      # and M the total number of sets.
      #
      # Algorithm 2 is O(N) where N is the total number of elements in all
      # the sets.
      algo_one_work = 0
      algo_two_work = 0
      sets.each do |other_set|
        algo_one_work += sets[0].cardinality
        algo_two_work += other_set ? other_set.cardinality : 0
      end
      # Directly from Redis:
      # Algorithm 1 has better constant times and performs less operations
      # if there are elements in common. Give it some advantage:
      algo_one_work /= 2
      diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2

      if diff_algo == 1
        if sets.length > 1
          sets[0..0] + sets[1..-1].sort_by! { |s| -1 * s.cardinality }
        end
        difference_algorithm1(sets)
      else
        difference_algorithm2(sets)
      end
    end
  end
end
```
_listing 9.11 The `RedisSet.difference` class method_

Redis uses two different algorithms to perform the set difference operation, while it not possible to know for sure which one will be more efficient, it tries to guess, depending on the size of the sets, which one will be faster.

The first algorithm works by iterating through the first set, and for each element, we look into each other set, as soon we find the element, we stop and move to the next element in the first set. If we make it through all the other sets without finding the element, we add the member to a new set, acting the result set. In other words, the main criteria dictating the "cost" of this algorithm is the size of the first set. The bigger the first set, the more iteration we'll have to perform.

The second algorithm works by creating a new set with all the elements from the first set and then iterate through all the elements in all the other sets, removing each member from the new set. The "cost" of this algorithm is the sum of the cardinalities of all the sets, given that each set will be iterated once.

Redis gives the first algorithm an edge because it seems like it tends to be faster, what this means in practice is that algorithm 2 will only be picked if the first set is significantly bigger than every other sets, in which case we know that its performance would not be ideal by having to iterate over all the items in the first set no matter what and through all the other sets for each member in the first set, and we have a potential shortcut with algorithm 2.

``` ruby
module BYORedis
  class RedisSet

    # ...

    def self.difference_algorithm1(sets)
      return RedisSet.new if sets.empty? || sets[0].nil?

      dest_set = RedisSet.new

      sets[0].each do |element|
        i = 0
        other_sets = sets[1..-1]
        while i < other_sets.length
          other_set = other_sets[i]
          # There's nothing to do when one of the sets does not exist
          next if other_set.nil?
          # If the other set contains the element then we know we don't want to add element to
          # the diff set
          break if other_set == self

          break if other_set.member?(element)

          i += 1
        end

        if i == other_sets.length
          dest_set.add(element)
        end
      end

      dest_set
    end
    private_class_method :difference_algorithm1

    def self.difference_algorithm2(sets)
      return self if sets.empty? || sets[0].nil?

      dest_set = RedisSet.new

      # Add all the elements from the first set to the new one
      sets[0].each do |element|
        dest_set.add(element)
      end

      # Iterate over all the other sets and remove them from the first one
      sets[1..-1].each do |set|
        set.each do |member|
          dest_set.remove(member)
        end
      end

      dest_set
    end
    private_class_method :difference_algorithm2

    def include?(member)
      return false if member.nil?

      case @underlying
      when IntSet then
        if member.is_a?(Integer)
          member_as_int = member
        else
          member_as_int = Utils.string_to_integer_or_nil(member)
        end

        if member_as_int
          @underlying.member?(member_as_int)
        else
          false
        end
      when Dict then @underlying.member?(member)
      else raise "Unknown type for structure #{ @underlying }"
      end
    end
    alias member? include?

    # ...
  end
end
```
_listing 9.12 The `difference_algorithm1` & `difference_algorithm2`  class methods in `RedisSet`_

Each of these two methods does not need to be exposed and we make them private with `private_class_method`.

The `difference_algorithm1` method implements the first algorithm described above where the first set is iterated over, and all the other sets are iterated over as long as the current member of the first set is not found. We need to add the `include?` method, aliased to `member?` to check for the presence of a member in a set.

The `IntSet` case of `include?` needs to account for the fact that the argument might already be an `Integer` or might be a `String` representing an `Integer`. We add the `Utils.string_to_integer_or_nil` method to help for this:


``` ruby
module BYORedis
  module Utils

    # ...

    def self.string_to_integer_or_nil(string)
      begin
        string_to_integer(string)
      rescue InvalidIntegerString
        nil
      end
    end

    # ...
  end
end
```
_listing 9.13 The `string_to_integer_or_nil` method in the `Utils` module_

In the `Dict` case of `RedisSet#include?` we delegate to the `Dict#include?` method, through its `member?` alias, which implements the exact interface we need, so let's add the alias:

``` ruby
module BYORedis
  class Dict

    # ...

    def include?(key)
      !get_entry(key).nil?
    end
    alias member? include?

    # ...

  end
end
```
_listing 9.14 Adding the `Dict#member?` aliased to `Dict#include?`_

The second algorithm intends to provide an alternative in case the first set is significantly larger than the other sets.

The next command, `SDIFFSTORE` is similar to `SDIFF`, with the difference being that the result is stored in the given key, and the return value is the cardinality of that set.

We have two other very similar methods coming next with `SINTERSTORE` & `SUNIONSTORE` so we're using a helper method with the shared logic:

``` ruby
module BYORedis
  module SetUtils

    # ...

    def self.generic_set_store_operation(db, args)
      Utils.assert_args_length_greater_than(1, args)
      destination_key = args.shift
      sets = args.map { |other_set| db.lookup_set(other_set) }
      new_set = yield sets

      if new_set.empty?
        db.data_store.delete(destination_key)
      else
        db.data_store[destination_key] = new_set
      end

      RESPInteger.new(new_set.cardinality)
    end
  end

  # ...

  class SDiffStoreCommand < BaseCommand
    def call
      SetUtils.generic_set_store_operation(@db, @args) do |sets|
        RedisSet.difference(sets)
      end
    end

    def self.describe
      Describe.new('sdiffstore', -3, [ 'write', 'denyoom' ], 1, -1, 1,
                   [ '@write', '@set', '@slow' ])
    end
  end
end
```
_listing 9.15 The `SDiffStoreCommand` class_

The `SDiffStoreCommand` class is extremely similar to `SDiffCommand`, with the exception that it needs to handle an extra argument at the beginning of the argument list, for the destination key. It then stores the result set at that key, or delete what was there before if the result set is empty. Finally, it returns the cardinality of the result set.

**Set Union**

The next set operation we will implement is [set union][wikipedia-set-union], which is defined as:

> The union of two sets A and B is the set of elements which are in A, in B, or in both A and B

Let's start by creating the `SUnionCommand` class:

``` ruby
module BYORedis

  # ...

  class SUnionCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      sets = @args.map { |set_key| @db.lookup_set(set_key) }.compact

      RESPArray.new(RedisSet.union(sets).members)
    end

    def self.describe
      Describe.new('sunion', -2, [ 'readonly', 'sort_for_script' ], 1, -1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end
end
```
_listing 9.16 The `SUnionCommand` class_

We are implementing the `union` method as a class method on `RedisSet` for the same reasons we decided to implement `difference` as a class method.

``` ruby
module BYORedis
  class RedisSet

    # ...

    def self.union(sets)
      if sets.empty?
        RedisSet.new
      else
        union_set = RedisSet.new
        sets.each do |set|
          set.each { |member| union_set.add(member) }
        end

        union_set
      end
    end

    # ...
  end
end
```

The set union operation is simpler, we iterate over all sets, over all of their members and add each member to a new set, once we're done, we return the set.

Similarly to how we first added the `SDIFF` command followed by the `SDIFFSTORE` command, we're now adding the `SUnionStoreCommand` class.

``` ruby
module BYORedis

  # ...

  class SUnionStoreCommand < BaseCommand
    def call
      SetUtils.generic_set_store_operation(@db, @args) do |sets|
        RedisSet.union(sets)
      end
    end

    def self.describe
      Describe.new('sunionstore', -3, [ 'write', 'denyoom' ], 1, -1, 1,
                   [ '@write', '@set', '@slow' ])
    end
  end
end
```
_listing 9.17 The `SUnionStoreCommand` class_


**Set Intersection**

The final set operation we're going to implement is [set intersection][wikipedia-set-intersection], with the `SINTER` command.

Set intersection is defined as:

> In mathematics, the intersection of two sets A and B, denoted by A ∩ B, is the set containing all elements of A that also belong to B (or equivalently, all elements of B that also belong to A).

``` ruby
module BYORedis
  module SetUtils
    def self.generic_sinter(db, args)
      sets = args.map do |set_key|
        set = db.lookup_set(set_key)

        return RedisSet.new if set.nil?

        set
      end

      RedisSet.intersection(sets)
    end
  end
  # ...

  class SInterCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      intersection = SetUtils.generic_sinter(@db, @args)

      RESPArray.new(intersection.members)
    end

    def self.describe
      Describe.new('sinter', -2, [ 'readonly', 'sort_for_script' ], 1, -1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end
end
```
_listing 9.18 The `SInterCommand` class_

The logic in `SInterCommand` shares a lot of logic with `SInterStoreCommand`, which we'll create next, so we go ahead and create a method with the shared logic `BYORedis::SetUtils.generic_sinter`. In order to implement this method, we need the `intersection` class method on the `RedisSet` class, but before calling this method, we do return early if any of the given keys does not exist. This is an important shortcut, when computing the intersection of sets, if any of the sets is empty, we know that the final result will be an empty sets, and non existing sets are treated as empty sets.

``` ruby
module BYORedis
  class RedisSet

    # ...

    def self.intersection(sets)
      # Sort the sets smallest to largest
      sets.sort_by!(&:cardinality)

      intersection_set = RedisSet.new
      # Iterate over the first set, if we find a set that does not contain it, discard

      sets[0].each do |member|
        present_in_all_other_sets = true
        sets[1..-1].each do |set|
          unless set.member?(member)
            present_in_all_other_sets = false
            break
          end
        end
        # Otherwise, keep
        intersection_set.add(member) if present_in_all_other_sets
      end

      intersection_set
    end

    # ...

  end
end
```
_listing 9.19 The `RedisSet.intersection` class method_

Set intersection is not as straightforward as set union but is not at complicated as set difference. We start by sorting all the sets from smallest to largest. Doing the sorting is a small optimization because we have to iterate over all the elements of at least one set, so we might as well pick the smaller one for that.

Once the sets are sorted, we pick the first one, the smallest one, and iterate over all its members, for each member we check its presence in all other sets, as soon as this check is false, we continue to the next member. This is because the result set of the intersection only contains elements present in all the sets.

If we make it through all the sets, then the member is indeed present in all sets and we add it to the result set.

The last set operation command on our list is `SINTERSTORE`, we've seen this pattern two times already by now. We use the `SetUtils.generic_set_store_operation` method the same way we did previously:

``` ruby
module BYORedis

  # ...

  class SInterStoreCommand < BaseCommand
    def call
      SetUtils.generic_set_store_operation(@db, @args) do
        SetUtils.generic_sinter(@db, @args)
      end
    end

    def self.describe
      Describe.new('sinterstore', -3, [ 'write', 'denyoom' ], 1, -1, 1,
                   [ '@write', '@set', '@slow' ])
    end
  end
end
```
_listing 9.20 The `SInterStoreCommand` class_

With `SInterStoreCommand` completed, we've now implemented all the set operation commands, `SUNION`, `SINTER` & `SDIFF`, and their `*STORE` variants.

### Membership related operations

**SMEMBERS**

The `SMEMBERS` command returns all the members of a set. Sets do not guarantee ordering, so we can just return members without having to worry about ordering. Let's create the `SMembersCommand`:

``` ruby
module BYORedis

  # ...

  class SMembersCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)
      set = @db.lookup_set(@args[0])

      RESPArray.new(set.members)
    end

    def self.describe
      Describe.new('smembers', 2, [ 'readonly', 'sort_for_script' ], 1, 1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end
end
```
_listing 9.21 The `SMembersCommand` class_

We need to add the `RedisSet#members` command:

``` ruby
module BYORedis
  class RedisSet

    # ...

    def members
      case @underlying
      when IntSet then @underlying.members.map { |i| Utils.integer_to_string(i) }
      when Dict then @underlying.keys
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    # ...

  end
end
```
_listing 9.22 The `RedisSet#members` method_

In the `IntSet` case we call the `IntSet#members` methods, and convert all in `Integer` instances to `String` with the `Utils.integer_to_string` method. In the `Dict` case we can directly return from the already existing `Dict#keys` method.

The conversion to strings in the `IntSet` case is to follow the behavior of the Redis command, as the following example shows, even if the set is an `intset`, Redis converts the value to RESP strings before returning them:

``` bash
127.0.0.1:6379> SADD s 1 2 3
(integer) 3
127.0.0.1:6379> DEBUG OBJECT s
Value at:0x7f88e7004130 refcount:1 encoding:intset serializedlength:15 lru:11187844 lru_seconds_idle:3
127.0.0.1:6379> SMEMBERS s
1) "1"
2) "2"
3) "3"
```

The quotes around the numbers show us that the array members are indeed strings, and `redis-cli` would prefix them with `(integer)` otherwise, but we can use `nc` to confirm the type of the elements of the array:

``` bash
> echo "SMEMBERS s" | nc -c localhost 6379
*3
$1
1
$1
2
$1
3
```

**SISMEMBER**

`SISMEMBER` returns an integer acting as a boolean, `1` for `true` and `0` for `false`, depending on the presence of the given member in the set:

``` ruby
module BYORedis

  # ...

  class SIsMemberCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      set = @db.lookup_set(@args[0])
      if set
        presence = set.member?(@args[1]) ? 1 : 0
        RESPInteger.new(presence)
      else
        RESPInteger.new(0)
      end
    end

    def self.describe
      Describe.new('sismember', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@set', '@fast' ])
    end
  end
end
```
_listing 9.23 The `SIsMemberCommand` class_

We delegate the actual work of checking if the set contains the given member to the `RedisSet#member?` method which we added earlier when adding the `SDIFF` command.

**SMISMEMBER** _(New in 6.2.0)_

``` ruby
module BYORedis

  # ...

  class SMIsMemberCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      set = @db.lookup_set(@args.shift)
      members = @args

      if set.nil?
        result = Array.new(members.size, 0)
      else
        result = members.map do |member|
          set.member?(member) ? 1 : 0
        end
      end

      RESPArray.new(result)
    end

    def self.describe
      Describe.new('smismember', -3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@set', '@fast' ])
    end
  end
end
```
_listing 9.24 The `SMIsMemberCommand` class_

This command uses the same method from `RediSet` we used in `SIsMemberCommand`, but inside the `map` method, which we use to return an array of integers acting as booleans, `1` if the member is in the set, `0` if it isn't.

**SCARD**

``` ruby
module BYORedis

  # ...

  class SCardCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)
      set = @db.lookup_set(@args[0])

      cardinality = set.nil? ? 0 : set.cardinality
      RESPInteger.new(cardinality)
    end

    def self.describe
      Describe.new('scard', 2, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@set', '@fast' ])
    end
  end
end
```
_listing 9.25 The `SCardCommand` class_

The command delegates to the `RedisSet#cardinality` method:

``` ruby
module BYORedis
  class RedisSet

    # ...

    def cardinality
      case @underlying
      when IntSet then @underlying.cardinality
      when Dict then @underlying.used
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    # ...

  end
end
```
_listing 9.26 The `RedisSet#cardinality` method_

Both `IntSet` and `Dict` already have methods that return what we need here, so we call them and return their results directly.

**SRANDMEMBER**

``` ruby
module BYORedis

  # ...

  class SRandMemberCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      raise RESPSyntaxError if @args.length > 2

      count = Utils.validate_integer(@args[1]) if @args[1]
      set = @db.lookup_set(@args[0])

      if set
        if count.nil?
          random_members = set.random_member
        else
          random_members = set.random_members_with_count(count)
        end

        RESPSerializer.serialize(random_members)
      elsif count.nil?
        NullBulkStringInstance
      else
        EmptyArrayInstance
      end
    end

    def self.describe
      Describe.new('srandmember', -2, [ 'readonly', 'random' ], 1, 1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end
end
```
_listing 9.27 The `SRandMemberCommand` class_

In this command we extract the `count` option, if present, and decide which method to call on `RediSet` depending on whether or not it was passed. We perform this check here because the logic behind getting a single random element, which is what `RediSet#random_member` does, is significantly simpler than getting multiple random elements, which is what `RedisSet#random_members_with_count` does. On top of that, the return type of the command is different, it is either `nil` or a single element in the first case and is an array, empty or not, otherwise

We could check for the type of the `random_members` variable in the `call` method, and call `RESPBulkString.new` on it, or `RESPArray.new`, depending on its type. There is a slightly better approach, as in, easier to reuse in different places, which is what the `JSON.generate` method from the standard library does:

``` ruby
irb(main):005:0> JSON.generate(1)
=> "1"
irb(main):006:0> JSON.generate({a: 1, b: 2})
=> "{\"a\":1,\"b\":2}"
irb(main):007:0> JSON.generate([1,2,3])
=> "[1,2,3]"
irb(main):008:0> JSON.generate('a')
=> "\"a\""
```

With `generate`, you can pass anything that can be serialized, and it'll return the serialized value for you. And while, yes, Ruby also provides `to_json` methods of most types, and it returns the same result:

``` ruby
irb(main):010:0> {a: 1, b: 2}.to_json
=> "{\"a\":1,\"b\":2}"
```

I personally prefer the first approach as it avoids crowding objects with many methods, and instead separate functionalities across different classes/objects. Both options work, I happen to prefer the first one, so let's mimic it with `RESPSerializer`:

``` ruby
module BYORedis

  # ...

  class RESPSerializer
    def self.serialize(object)
      case object
      when Array then RESPArray.new(object)
      when RedisSet then RESPArray.new(object.members)
      when List then ListSerializer.new(object)
      when Integer then RESPInteger.new(object)
      when String then RESPBulkString.new(object)
      when Dict
        pairs = []
        object.each { |k, v| pairs.push(k, v) }
        RESPArray.new(pairs)
      when nil then NullBulkStringInstance
      else
        raise "Unknown object for RESP serialization #{ object }"
      end
    end
  end
end
```

With this class we can now call `RESPSerializer.new(object)` with any objects that can be serialized to RESP, without having to know its exact type. If we happen to know the type of the object we're dealing, we might as well call its specific RESP serializer, which is what we'll keep doing through the rest of the book.

Getting a random element from an `IntSet` does not require as many steps as it does for a `Dict`. Given that the structure behind an `IntSet` is an array, we can pick a random index, between `0` and `size - 1`, and return the element at that index. As long as the random function we use is "random enough", the element returned will be random. We're using quotes here because the topic of randomness if fairly complicated, luckily Ruby does a lot of the heavy lifting for us, with the `Kernel.rand` method and the `SecureRandom` module. The difference between the two different approaches is a little bit out of scope for now, and we'll be using `Kernel.rand` since it is sufficient for our needs.

On the other hand, getting a random entry for a set using a `Dict` is way more complicated. It might seem similar at first glance, the data structure powering a hash table is also an array, but some buckets might be empty, so we cannot "just" pick a random index and return the value stored there, given that it could be empty. This problem is not insurmountable, we can add some form of retry logic, until a non empty bucket is found. We also need to take into account that the `Dict` might be in the middle of the rehashing process, in which case the buckets will be split across two different hash tables. Finally, even if we dealt with these issues, there is still a major issue, buckets contain zero or more entries, and given the nature of the SipHash algorithm, there is no way to expect the distribution of entries across buckets.

In practical terms, this means for a hash of size 8, containing 6 entries, it is entirely possible that five buckets are empty, two contain one element each and that the last bucket contain the other four elements. This means that even after finding a non empty bucket, we now need to select a random entry within that bucket if it contains more than one. Doing so works in the sense of being able to potentially return any of the values contained in the hash, but it completely obliterates the distribution of the result.

Ideally, as we call `SRANDMEMBER` multiple times, the distribution of the returned element should trend towards the perfect proportion. Using the example from above, if a set contains six elements, each element should have a probability of `1/6` to be returned, but the approach described above might be completely off, let's look at an example.

Note that these examples assume that the `Kernel.rand` has a perfect distribution, which is not the case, computers cannot be perfectly random, but it is pretty close as the following example shows:

``` ruby
distribution = Hash.new { |h, k| h[k] = 0 }
times = 100_000

times.times do
  distribution[rand(1..6)] += 1
end

p Hash[distribution.map { |k, v| [k, v / times.to_f] }]
```

The previous script returned the following on my machine, the result should be different but pretty close on another machine, or as you run it multiple times:

``` ruby
{1=>0.17011, 2=>0.16645, 6=>0.16604, 5=>0.16696, 4=>0.1657, 3=>0.16474}
```

The perfect distribution would be `0.166666667`, so it's not _that_ far. For example, running the same example with `times = 1_000_000` instead returned closer results as expected, the more we run it, the more the results will get closer to the perfect distribution:

``` ruby
{3=>0.166954, 4=>0.16658, 5=>0.16693, 2=>0.166722, 1=>0.166075, 6=>0.166739}
```

Back to the distribution problem in a hash table, the previous example we mentioned looked like the following, five empty buckets, two with one member each and one with four.

```
s = { nil, nil, nil, nil, nil, < 1 >, < 2 >, < 3, 4, 5, 6 > }
```

Each of the bucket has about `1/6` chance of getting picked, but we'll retry as long we pick an empty bucket, so given that there are three non-empty buckets, each of these has about 1/3 change of getting picket. The problem is if the last one get picked, then we need to roll the die one more time, and this time each item will have about 1/4 chance of getting picked, bringing the probabilities of each elements to the following:

```
1 => 1/3 ~= 0.333333333
2 => 1/3 ~= 0.333333333
3 => 1/3 * 1/4 = 1/24 ~= 0.083333333
4 => 1/3 * 1/4 = 1/24 ~= 0.083333333
5 => 1/3 * 1/4 = 1/24 ~= 0.083333333
6 => 1/3 * 1/4 = 1/24 ~= 0.083333333
```

To attempt addressing this problem Redis uses two functions, `dictGetSomeKeys` and `dictGetFairRandomKey`. It also uses a third function, `dictGetRandomKey`, which implements the logic we described previously, only as a backup, in case `dictGetSomeKeys` fails to return any keys.

This approach alleviates some of the issues we described earlier by first randomly selecting keys through the dictionary, putting them in a flat array, and only then picking one random index in this array.

Let's add these methods to our `Dict` class

``` ruby
module BYORedis
  class Dict

    # ...

    GETFAIR_NUM_ENTRIES = 15
    def fair_random_entry
      entries = get_some_entries(GETFAIR_NUM_ENTRIES)

      if entries.empty?
        random_entry
      else
        entries[rand(0...entries.size)]
      end
    end

    private

    def get_some_entries(count)
      entries = []
      stored = 0
      count = used if count > used
      maxsteps = count * 10

      count.times { rehash_step } if rehashing?

      tables = rehashing? ? 2 : 1
      maxsizemask = main_table.sizemask
      if tables > 1 && rehashing_table.sizemask > maxsizemask
        maxsizemask = rehashing_table.sizemask
      end

      i = rand(0..maxsizemask)
      empty_len = 0
      while stored < count && maxsteps
        iterate_through_hash_tables_unless_rehashing do |hash_table|
          # If we're in the process of rehashing, up to the indexes already visited in the main
          # table during the rehashing, there are no populated buckets so we can skip in the
          # main table, all the indexes between 0 and @rehashidx - 1
          if rehashing? && hash_table == main_table && i < @rehashidx
            if i >= rehashing_table.size
              i = @rehashidx
            else
              next
            end
          end

          next if i >= hash_table.size # Out of range for this table

          hash_entry = hash_table.table[i]

          # Count contiguous empty bucket and jump to other locations if they reach 'count'
          # with a minimum of 5
          if hash_entry.nil?
            empty_len += 1
            if empty_len >= 5 && empty_len > count
              i = rand(0..maxsizemask)
              empty_len = 0
            end
          else
            empty_len = 0
            while hash_entry
              entries << hash_entry
              hash_entry = hash_entry.next
              stored += 1
              return entries if stored == count
            end
          end
        end

        i = (i + 1) & maxsizemask # increment and wraparound if needed
        maxsteps -= 1
      end

      entries
    end

    def random_entry
      return if used == 0

      rehash_step if rehashing?

      hash_entry = nil

      if rehashing?
        # There are no elements indexes from 0 to rehashidx-1 so we know the only places we can
        # find an element are in main_table[rehashidx..-1] and anywhere in the rehashing table
        # We generate the random_index between the total number of slots (the two sizes), minus
        # the rehashing index. An example, we're growing from 8 to 16 buckets, that's 24 total
        # slots, now let's imagine that @rehashidx is 4, we generate an index between 0 and 20
        # (excluded), and we add 4 to it, that means that we _never_ have a value under 4.
        # If the random index is 8 or more, we need to look in the rehashing table, but we need
        # adjust it by removing 8, the size of the main table to it, so say it was initially 19,
        # plus four, that' 23, minus 8, that's 15, the last bucket in the rehashing table.
        # If the random index is between 4 and 7, then we look directly in the main table
        while hash_entry.nil?
          max = slots - @rehashidx
          random_index = @rehashidx + SecureRandom.rand(max)
          hash_entry =
            if random_index >= main_table.size
              rehashing_table.table[random_index - main_table.size]
            else
              main_table.table[random_index]
            end
        end
      else
        while hash_entry.nil?
          random_index = SecureRandom.rand(main_table.size)
          hash_entry = main_table.table[random_index]
        end
      end

      # Now that we found a non empty bucket, we need to pick a random element from it, but if
      # there's only one item, we can save some time and return right away
      return hash_entry if hash_entry.next.nil?

      list_length = 0
      original_hash_entry = hash_entry
      while hash_entry
        list_length += 1
        hash_entry = hash_entry.next
      end
      random_list_index = SecureRandom.rand(list_length)
      hash_entry = original_hash_entry
      random_list_index.times do
        hash_entry = hash_entry.next
      end

      hash_entry
    end
  end
end
```
_listing 9.28 The new random methods in the `Dict` class_

The three methods we just added to the `Dict` class implement this better approach to retrieving a random key/value pair. `fair_random_entry` is the only public one and attempts to use the result from `get_some_entries`, but due to the nature of the hash table, it is possible that `get_some_keys` fails to return any keys, in which case we fall back to `random_entry`, which suffers from the distribution above outlined above, but still returns _a_ random element, just not _that_ random.

Now that `Dict` is updated, we can implement `random_member` and `random_members_with_count` in the `RedisSet` class:

``` ruby
module BYORedis
  class RedisSet

    # How many times bigger should be the set compared to the requested size
    # for us to don't use the "remove elements" strategy? Read later in the
    # implementation for more info.
    # See: https://github.com/antirez/redis/blob/6.0.0/src/t_set.c#L609-L612
    SRANDMEMBER_SUB_STRATEGY_MUL = 3

    # ...

    def random_members_with_count(count)
      return [] if count.nil? || count == 0

      # Case 1: Count is negative, we return that many elements, ignoring duplicates
      if count < 0
        members = []
        (-count).times do
          members << random_member
        end

        return members
      end

      # Case 2: Count is positive and greater than the size, we return the whole thing
      return self if count >= cardinality

      # For both case 3 & 4 we need a new set
      new_set_content = Dict.new
      # Case 3: Number of elements in the set is too small to grab n random distinct members
      # from it so we instead pick random elements to remove from it
      # Start by creating a new set identical to self and then remove elements from it
      if count * SRANDMEMBER_SUB_STRATEGY_MUL > cardinality
        size = cardinality
        each { |member| new_set_content.add(member, nil) }
        while size > count
          random_entry = new_set_content.fair_random_entry
          new_set_content.delete(random_entry.key)
          size -= 1
        end
        return new_set_content.keys
      end

      # Case 4: The number of elements in the set is big enough in comparison to count so we
      # do the "classic" approach of picking count distinct elements
      added = 0
      while added < count
        member = random_member
        added += 1 if new_set_content.add(member, nil)
      end

      new_set_content.keys
    end

    def random_member
      case @underlying
      when IntSet then Utils.integer_to_string(@underlying.random_member)
      when Dict then @underlying.fair_random_entry.key
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    # ...

  end
end
```
_listing 9.29 The `random_member` and `random_member_with_count` methods in the `RedisSet` class_

The `random_members_with_count` method breaks down the possible scenarios in four different possibilities. "Case 1" is for a negative `count` value, in which case duplicates are allowed, so we can iterate as many times as needed and select a random member at each step, without having to worry about anything else.

"Case 2" is if `count` is greater than the size of the set, in which case we don't actually need to select any random members and we can return the whole set.

We differentiate between "Case 3" and "Case 4" depending on how close `count` is to the size of the set. The idea being that because we can't return duplicates, it might take a while to pick `n` random members if `n` is really close to the size of the set. Let's look at an example with a set of size `10`, the numbers `1` to `10`. If we call `SRANDMEMBER set 9`, we want to return nine random members, but doing so would require many tries.

Each of the member has a `1/10` change of getting picked, so the first pick will get a random member, but on the second try we have a `1/10` chance of picking the same element, in which case we'd need to try again. As we fill up the result dict, the odds of picking only one of the few non selected members will be really small.

This case if avoided with the `if count * SRANDMEMBER_SUB_STRATEGY_MUL > cardinality` condition. The constant is set to `15`, so with a `count` value of `10` and a set containing `15` elements, the condition will be true, `10 * 15 > 15`.

The condition would only be `false` with a large enough set and a small enough `count` value, such as `100` and `2`, `2 * 15 > 100 == false`.

So, in "Case 3", we instead create a new dict with all the elements from the set, and remove random members until its size reaches `count`.

"Case 4" is the most straightforward approach, we keep looping and picking random members, until we've picked `count` unique members.

### Removing elements

**SPOP**

``` ruby
module BYORedis

  # ...

  class SPopCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      raise RESPSyntaxError if @args.length > 2

      if @args[1]
        count = Utils.validate_integer(@args[1])
        return RESPError.new('ERR index out of range') if count < 0
      end
      key = @args[0]
      set = @db.lookup_set(key)

      if set
        popped_members = @db.generic_pop(key, set) do
          if count.nil?
            set.pop
          else
            set.pop_with_count(count)
          end
        end

        RESPSerializer.serialize(popped_members)
      elsif count.nil?
        NullBulkStringInstance
      else
        EmptyArrayInstance
      end
    end

    def self.describe
      Describe.new('spop', -2, [ 'write', 'random', 'fast' ], 1, 1, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end
end
```
_listing 9.30 The `SPopCommand` class_

Similarly to `SRANDMEMBER`, `SPOP` accepts a `count` argument, describing how many items can be returned, contrary to `SRANDMEMBER`, `SPOP` does not accept negative `count` values, and a value of `0` is effectively a no-op, no members are popped.

We renamed the `DB#generic_pop_wrapper` that used to only work with `List` instances to `generic_pop` and we use it with `RedisSet` instances now.

We now need to add `RedisSet#pop` and `RedisSet#pop_with_count`:

``` ruby
module BYORedis
  class RedisSet

    # ...

    # How many times bigger should be the set compared to the remaining size
    # for us to use the "create new set" strategy? Read later in the
    # implementation for more info.
    # See: https://github.com/antirez/redis/blob/6.0.0/src/t_set.c#L413-416
    SPOP_MOVE_STRATEGY_MUL = 5

    # ...

    def pop
      case @underlying
      when IntSet then @underlying.pop.to_s
      when Dict then
        random_entry = @underlying.fair_random_entry
        @underlying.delete(random_entry.key)
        random_entry.key
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    def pop_with_count(count)
      return [] if count.nil? || count == 0

      # Case 1: count is greater or equal to the size of the set, we return the whole thing
      if count >= cardinality
        all_members = members
        clear
        return all_members
      end

      remaining = cardinality - count
      if remaining * SPOP_MOVE_STRATEGY_MUL > count
        # Case 2: Count is small compared to the size of the set, we "just" pop random elements

        count.times.map { pop }
      else
        # Case 3: count is big and close to the size of the set, and remaining is small, we do
        # the reverse, we pick remaining elements, and they become the new set
        new_set = RedisSet.new
        remaining.times { new_set.add(pop) }
        # We have removed all the elements that will be left in the set, so before swapping
        # them, we store all the elements left in the set, which are the ones that will end up
        # popped
        result = members

        # Now that we have saved all the members left, we clear the content of the set and copy
        # all the items from new_set, which are the ones left
        clear
        new_set.each { |member| add(member) }

        result
      end
    end

    # ...

  end
end
```
_listing 9.31 The `pop` & `pop_with_count` methods in `RedisSet`_

`pop` is straightforward in the `IntSet` case given that we already implemented the `IntSet#pop` method, we can call it and directly return from it.

On the other hand, the `Dict` case is trickier and we now use the newly created `Dict#fair_random_entry` method, to find which entry to delete from the set.

`pop_with_count` is optimized to handle a few different edge cases elegantly. We label "Case 1" the case where the `count` value is greater than or equal to the cardinality of the set, and can return all the members, and clear the set.

In the case where count is anywhere between `1` and `cardinality - 1`, we do need to find random elements to remove from the set and return them. While it may seem like a simple problem at first, just iterate `count` times and pop a random element at each iteration, the reality is a little bit more complicated. The problem is that the process of popping random element can get expensive if the `count` number is very large, so instead we use an arbitrary threshold and if count is big enough that it is too close to the cardinality of the set, we reverse the process. We only pop the number of elements that should be left in the set, set them aside, extract all the remaining elements from the set, return them and add back the small set of remaining elements in the set.

Looking at a set of 10 elements as an example, if `count` is `9`, then `remaining * SPOP_MOVE_STRATEGY_MUL` is `5`, which is not greater than `9`, so we fall in "Case 3". In the case where `count` is `8`, then `2 * 5 = 10`, which is greater than count, so we pop eight times from the set, that's "Case 2".

**SREM**

``` ruby
module BYORedis

  # ...

  class SRemCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      set = @db.lookup_set(@args.shift)
      remove_count = 0

      if set
        @args.each do |member|
          remove_count += 1 if @db.remove_from_set(key, set, member)
        end
      end

      RESPInteger.new(remove_count)
    end

    def self.describe
      Describe.new('srem', -3, [ 'write', 'fast' ], 1, 1, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end
end
```
_listing 9.32 The `SRemCommand` class_

`SREM` relies on `DB#remove_from_set`, let's add it:

``` ruby
module BYORedis
  class DB

    # ...

    def remove_from_set(key, set, member)
      removed = set.remove(member)
      @data_store.delete(key) if set.empty?

      removed
    end

    # ...

  end
end
```

Let's now add `RedisSet#remove` to complete the `SREM` implementation:

``` ruby
module BYORedis
  class RedisSet

    # ...

    def remove(member)
      case @underlying
      when IntSet
        member_as_integer = Utils.string_to_integer_or_nil(member)
        if member_as_integer
          @underlying.remove(member_as_integer)
        else
          false
        end
      when Dict then !@underlying.delete_entry(member).nil?
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    # ...

  end
end
```
_listing 9.33 The `RedisSet#remove` method_

Up until now the `Dict#remove` method used to return the value of the deleted pair, or `nil` if the dict did not contain the given key. The problem with this approach is that if the value stored in the `Dict` was `nil`, the caller would not be able to differentiate the successful deletion of a pair, or a "miss" if the dictionary did not contain the key.

Similarly to how we initially implemented the `get` method by using the lower level method `get_entry`, we are adding the `delete_entry` method, which returns the full `DictEntry` instance for a successful deletion. With this change, callers can now know that a `nil` value is indeed a miss whereas a `DictEntry` instance, regardless of what the value of the `value` field is, is a successful deletion.

``` ruby
module BYORedis
  class Dict

    # ...

    def delete_entry(key)
      return if main_table.used == 0 && rehashing_table.used == 0

      rehash_step if rehashing?

      hash_key = SipHash.digest(RANDOM_BYTES, key)
      iterate_through_hash_tables_unless_rehashing do |hash_table|
        index = hash_key & hash_table.sizemask
        entry = hash_table.table[index]
        previous_entry = nil

        while entry
          if entry.key == key
            if previous_entry
              previous_entry.next = entry.next
            else
              hash_table.table[index] = entry.next
            end
            hash_table.used -= 1
            return entry
          end
          previous_entry = entry
          entry = entry.next
        end
      end

      nil
    end

    def delete(key)
      delete_entry(key)&.value
    end

    # ...

  end
end
```
_listing 9.34 The `delete` & `delete_entry` methods in the `Dict` class_

The `delete` method is now written in terms of `delete_entry`. The `&.` operator allows us to concisely return `nil` from `delete` if `delete_entry` returned `nil` itself, in the case where the `Dict` does not contain `key`. If the `value` field of the `DictEntry` is `nil`, then callers of `delete` have no way of differentiating a miss, when the `Dict` does not contain `key`, and a successful deletion. If this is important to the callers, which in the case in `SRemCommand`, then callers can use `delete_entry` instead.

**SMOVE**

`SMOVE` is used to remove a member from a set and add it to another

``` ruby
module BYORedis

  # ...

  class SMoveCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      source_key = @args[0]
      source = @db.lookup_set(source_key)
      member = @args[2]
      destination = @db.lookup_set_for_write(@args[1])

      if source.nil?
        result = 0
      else
        removed = @db.remove_from_set(source_key, source, member)
        if removed
          destination.add(member)
          result = 1
        else
          result = 0
        end
      end

      RESPInteger.new(result)
    end

    def self.describe
      Describe.new('smove', 4, [ 'write', 'fast' ], 1, 2, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end
end
```
_listing 9.35 The `SMoveCommand` class_

If the `source` set does not exist, there is nothing to do since we have no set to remove `member` from. If it does exist, then we call `DB#remove_from_set`, which we added earlier for the `SREM` command, if it returns `true` then we add the `member` in `destination`. Note that we return `1` as long as `member` was found in `source`, even if it was already in `destination`.

This wraps up the last set command!

## Conclusion

You can find the code [on GitHub][github-code].

With set commands implemented, we now have one last native data type left, sorted sets, and this is what [Chapter 10][chapter-10] will cover.

## Appendix A: A more idiomatic `IntSet`

The `IntSet` class we created earlier in the Chapter was trying to replicate as much as possible the logic used in `intset.c` in Redis, at the cost of not being really idiomatic Ruby. If we were to set aside the encoding, and use the Ruby `Integer` class as provided, we can end up with a simpler implementation:

``` ruby
module BYORedis
  class IntSet

    def initialize
      @underlying_array = []
    end

    def add(member)
      raise "Member is not an int: #{ member }" unless member.is_a?(Integer)

      index = @underlying_array.bsearch_index { |x| x >= member }

      if index.nil?
        @underlying_array.append(member)

        true
      elsif @underlying_array[index] == member
        false
      else
        @underlying_array.insert(index, member)

        true
      end
    end

    # ...
  end
end
```
_listing 9.36 The `initialize` & `add` methods in the `IntSet` class_

The first thing we do in `add` is check that the argument is indeed an `Integer`, and abort if it isn't. It is up to callers of these methods to do the due diligence of calling this method with the correct argument type.

The next step is to find where in the array should the new element be added, and we use the [`bsearch_index`][ruby-doc-bsearch-index] method for that. By giving the block `{ |x| x >= member }` block to the method, it will return the smallest index of an element that is greater than or equal to `member`. If no elements are greater than or equal to `member`, then it returns `nil`.

Based on these cases, there are three cases we need to consider, the first one, if `index` is `nil`, means that no elements in the array are greater than or equal to `member`, in other words, `member` is now the member with the largest value, and we should add it at the end of the array. This is what we do with the `Array#append` method.

Next is the case where `index` is not `nil`, and in this case there are two options, the value at `index` is either equal to `member`, or greater than `member`. If the value is equal to `member`, it means that there is already an `Integer` in the array with the same value as `member`, and it means that we have nothing to do, the member is already present.

The last case is if the value at `index` is greater than `member`. `bsearch_index` guarantees that if returned the smallest index of all the values greater than `member`, so we need to add `member` right before this element, and this is what [`Array#insert`][ruby-doc-insert] does, it inserts the given value before the element with the given index.

The next main method is the ability to check for the presence of a member in the set, this is what the `include?` method does:

``` ruby
def include?(member)
  return false if member.nil?

  !@underlying_array.bsearch { |x| member <=> x }.nil?
end
alias member? include?
```
_listing 9.37 The `IntSet#include?` method_

We're following Ruby's `Set` class naming convention here, naming the method `include?`, also accessible through the `member?` alias. We'll use the `member?` method throughout this chapter to use the same language used by the Redis commands such as `SISMEMBER`.

The `include?` methods relies almost exclusively on the [`bsearch`][ruby-doc-bsearch] method, which we've indirectly explored previously through its close sibling [`bsearch_index`][ruby-doc-bsearch-index]. Both methods use a similar API, the difference being that `bsearch` returns an element from the array whereas `bsearch_index` returns the index of an element. Both could have been used here given that the returned value itself is not that important, we already really care whether or not the result is `nil`.

These `bsearch` methods have two modes of operation, which can be a bit confusing these the mode is decided implicitly depending on the return value of the block passed to the method. The two modes are `find-minimum` & `find-any`. We've only used the `find-minimum` mode so far, by passing the block `{ |x| member <=> x }` to the method, we end up using the `find-any` mode, because the `Integer#<=>` method returns an integer, `-1`, `0` or `1`. The Ruby documentation of the method is surprisingly not clear at describing the behavior of this mode, on the other hand the man page of `bsearch(3)`, accessible with `man 3 bsearch`, which is what the `find-any` mode is based after is a little bit more helpful:

> The contents of the array should be in ascending sorted order according to the comparison function referenced by compar.
>
> The compar routine is expected to have two arguments which point to the key object and to an array member, in that order.  It should return an integer which is less than, equal to, or greater than zero if the key object is found, respectively, to be less than, to match, or be greater than the array member.

We need to "translate" this description given that it describes the `C` function, not the Ruby one, but the `compar` function is essentially very similar to the block argument, and the `key` object is the argument to the method, `member` in the `include?` method.

With that said, what the documentation tells us is that the block should return `0` if both values are equal, a negative value if `member` is less than `x`, and a positive value if `member` is greater than `x`. The `<=>`, often called "spaceship operator", does exactly that!

Using this block, `bsearch` will return the value if it finds it, or `nil` if it can't find it.


Let's now add the ability to remove an element from a set with the `remove` method:

``` ruby
def remove(member)
  index = @underlying_array.bsearch_index { |x| member <=> x }
  if index
    @underlying_array.delete_at(index)
    true
  else
    false
  end
end
```
_listing 9.38 The `IntSet#remove` method_

The `method` method uses the `bsearch_index` method in a way almost identical to how the `include?` method uses the `bsearch` method. By using the `Interger#<=>` method, we will either receive the index of `member` in the array, or `nil`. If `index` is `nil`, there's nothing to remove, so we can return `false` and call it a day. On the other hand, if `index` is not `nil`, we use the `Array#delete_at` method, which deletes the element at the given index.

Let's now add the `pop` and `random_member` methods, which both behave very similarly, with the exception that `random_member` does not remove any elements from the array:

``` ruby
# ...
def pop
  rand_index = rand(@underlying_array.size)
  @underlying_array.delete_at(rand_index)
end

def random_member
  rand_index = rand(@underlying_array.size)
  @underlying_array[rand_index]
end
```
_listing 9.39 The `pop`, & `random_member` methods in the `IntSet` class_

Finally, we need a few more methods to provide an easy to use API for the `IntSet` class, namely, the methods `empty?` to check if the sets is empty or not, `members`, to return all the members in the set, `cardinality`, to return the size of the set, and `each`, to provide a way to iterate over all the members in the set. Ruby gives us tools that allow us to provide these methods without having to explicitly define them. We're using the [`Forwardable` module][ruby-doc-forwardable] to delegate some methods directly to the `Array` instance, `@underlying_array`. We're also using the `alias` keyword to provide some of these methods through the same naming conventions used in Redis. We also use the `attr_reader` approach to create an accessor for the `@underlying_array` instance variable, and alias it to `members` to provide a more explicit method name:

``` ruby
require 'forwardable'

module BYORedis
  class IntSet
    extend Forwardable

    attr_reader :underlying_array
    def_delegators :@underlying_array, :empty?, :each, :size

    alias cardinality size
    alias card cardinality
    alias members underlying_array

    def initialize
      @underlying_array = []
    end

    # ...

  end
end
```
_listing 9.40 The `members`, `empty?`, `each` & `cardinality` methods in the `IntSet` class_

And with these aliases, we now have completed the `IntSet` class, let's now use it, in combination with the `Dict` class, to implement the Set commands, starting with the one allowing us to create a new set.

[redis-set-commands]:https://redis.io/commands#set
[redis-src-tset]:https://github.com/redis/redis/blob/6.0.0/src/t_set.c
[redis-config-max-intset-entries]:https://github.com/redis/redis/blob/6.0.0/redis.conf#L1520
[redis-src-intset]:https://github.com/redis/redis/blob/6.0.0/src/intset.c
[wikipedia-finite-sets]:https://en.wikipedia.org/wiki/Finite_set
[wikipedia-set-type]:https://en.wikipedia.org/wiki/Set_(abstract_data_type)
[chapter-8]:/post/chapter-8-adding-hash-commands/
[redis-src-dictadd]:https://github.com/redis/redis/blob/6.0.0/src/t_set.c#L79
[ruby-doc-bsearch]:https://ruby-doc.org/core-2.7.1/Array.html#bsearch-method
[ruby-doc-forwardable]:https://ruby-doc.org/stdlib-2.7.1/libdoc/forwardable/rdoc/Forwardable.html
[ruby-doc-bsearch]:https://ruby-doc.org/core-2.7.1/Array.html#bsearch-method
[ruby-doc-bsearch-index]:https://ruby-doc.org/core-2.7.1/Array.html#bsearch_index-method
[ruby-doc-insert]:https://ruby-doc.org/core-2.7.1/Array.html#insert-method
[wikipedia-set-relative-complement]:https://en.wikipedia.org/wiki/Complement_(set_theory)#Relative_complement
[wikipedia-set-union]:https://en.wikipedia.org/wiki/Union_(set_theory)
[wikipedia-set-intersection]:https://en.wikipedia.org/wiki/Intersection_(set_theory)
[github-code]:https://github.com/pjambet/redis-in-ruby/tree/master/code/chapter-9
[chapter-10]:/post/chapter-10-adding-sorted-set-commands/
[chapter-6]:/post/chapter-6-building-a-hash-table/
[ruby-set-class]:https://ruby-doc.org/stdlib-2.7.1/libdoc/set/rdoc/Set.html
[chapter-7]:/post/chapter-7-adding-list-commands/
[ruby-doc-array-pack]:https://ruby-doc.org/core-2.7.1/Array.html#pack-method
