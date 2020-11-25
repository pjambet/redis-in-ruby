---
title: "Chapter 11 Adding Bitmaps Commands"
date: 2020-10-17T18:17:39-04:00
lastmod: 2020-10-17T18:17:39-04:00
draft: true
comment: false
keywords: []
summary: ""
---

open q: why >> 3 in redis? add println and debug

## What we'll cover

bla bla bla

### Use case examples

I would personally categorize the bitmap operations as less common that any other data types, I think that where using other data types might be a good default, "let's use a string here" or "this should probably be a hash", bitmaps should be a very conscious decision based on how "optimized" of a data type it is.

Put differently, I would only recommend it if you've weighted other options and you've come to the conclusion that the loss in 

## GETBIT

Layout of the string `'abcd'`

``` ruby
irb(main):005:0> 'abcd'.chars.map(&:ord).map { |byte| '%08b' % byte }
=> ["01100001", "01100010", "01100011", "01100100"]
```

```
|-----------------|----------------------|------------------------|
| 7 6 5 4 3 2 1 0 | 7 6 5  4  3  2  1  0 | 7  6  5  4  3  2  1  0 | (index per byte)
|-----------------|----------------------|------------------------|
| 0 1 1 0 0 0 0 1 | 0 1 1  0  0  0  1  0 | 0  1  1  0  0  0  1  1 |
|-----------------|----------------------|------------------------|
| 0 1 2 3 4 5 6 7   8 9 10 11 12 13 14 15  16 17 18 19 20 21 22 23| (index)
```

`GETBIT s 0`

`GETBIT s 12`

``` ruby
irb(main):001:0> string_index = 12 / 8 # => 1
irb(main):002:0> byte_offset = 12 & 7 # => 4 / Equivalent to 12 & 8
irb(main):003:0> s[string_index]
=> "b"
irb(main):004:0> (s[string_index].ord >> (7 - byte_offset)) & 1
=> 0
```

`GETBIT s 22`

``` ruby
irb(main):001:0> string_index = 22 / 8 # => 2
irb(main):002:0> byte_offset = 22 & 7 # => 6 / Equivalent to 12 & 8
irb(main):003:0> s[string_index]
=> "c"
irb(main):004:0> (s[string_index].ord >> (7 - byte_offset)) & 1
=> 1
```

## SETBIT

Same same same

``` ruby
```

## BITOP

### and

### or

### xor

### not

## BITCOUNT

## BITPOS

## BITFIELD

Haa, finally

asda

## Conclusion


https://redis.io/topics/data-types-intro#bitmaps

BITCOUNT
BITFIELD
BITOP
BITPOS
GETBIT
SETBIT
