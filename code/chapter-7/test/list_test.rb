# coding: utf-8

require_relative './test_helper'

describe 'BYORedis - List commands' do
  describe 'LRANGE' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LRANGE a', '-ERR wrong number of arguments for \'LRANGE\' command' ],
        [ 'LRANGE a b', '-ERR wrong number of arguments for \'LRANGE\' command' ],
        [ 'LRANGE a b c d', '-ERR wrong number of arguments for \'LRANGE\' command' ],
      ]
    end

    it 'fails if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LRANGE not-a-list 0 -1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'fails it start & stop are not integers' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LRANGE a not-a-number not-a-number', '-ERR value is not an integer or out of range' ],
        [ 'LRANGE a 0 not-a-number', '-ERR value is not an integer or out of range' ],
        [ 'LRANGE a not-a-number -1', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns the whole list with 0 -1' do
      assert_command_results [
        [ 'LPUSH a g f e d c b', ':6' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd', 'e', 'f', 'g' ] ],
      ]
    end

    it 'handles negative indexes as starting from the right side' do
      assert_command_results [
        [ 'LPUSH a d c b', ':3' ],
        [ 'LRANGE a -3 2', [ 'b', 'c', 'd' ] ],
        [ 'LRANGE a -2 1', [ 'c' ] ],
        [ 'LRANGE a -2 2', [ 'c', 'd' ] ],
        [ 'LRANGE a -1 2', [ 'd' ] ],
      ]
    end

    it 'works with out of bounds indices' do
      assert_command_results [
        [ 'LPUSH a b c d', ':3' ],
        [ 'LRANGE a 2 22', [ 'b' ] ],
      ]

      assert_command_results [
        [ 'LPUSH a b c d', ':3' ],
        [ 'LRANGE a -6 0', [ 'd' ] ],
      ]
    end

    it 'returns an empty array for out of order boundaries' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LRANGE a 2 1', [] ],
        [ 'LRANGE a -1 -2', [] ],
      ]
    end

    it 'returns sublists' do
      assert_command_results [
        [ 'LPUSH a f e d c b', ':5' ],
        [ 'LRANGE a 1 1', [ 'c' ] ],
        [ 'LRANGE a 1 3', [ 'c', 'd', 'e' ] ],
        [ 'LRANGE a 3 4', [ 'e', 'f' ] ],
        [ 'LRANGE a 3 100', [ 'e', 'f' ] ],
      ]
    end
  end

  describe 'LPUSH' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LPUSH a', '-ERR wrong number of arguments for \'LPUSH\' command' ],
      ]
    end

    it 'fails if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LPUSH not-a-list a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'creates a list for a non existing key' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
      ]
    end

    it 'returns the number of elements in the list after insert' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LPUSH a c', ':2' ],
        [ 'LPUSH a d', ':3' ],
      ]
    end

    it 'handles multiple keys' do
      assert_command_results [
        [ 'LPUSH a b c d e', ':4' ],
        [ 'LPUSH a f g', ':6' ],
      ]
    end
  end

  describe 'LPUSHX' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LPUSHX', '-ERR wrong number of arguments for \'LPUSHX\' command' ],
        [ 'LPUSHX a', '-ERR wrong number of arguments for \'LPUSHX\' command' ],
      ]
    end

    it 'fails if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LPUSHX not-a-list a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'does nothing a non existing key' do
      assert_command_results [
        [ 'LPUSHX a b', ':0' ],
      ]
    end

    it 'returns the number of elements in the list after insert' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LPUSHX a b', ':2' ],
        [ 'LPUSHX a c', ':3' ],
        [ 'LPUSHX a d', ':4' ],
      ]
    end

    it 'handles multiple keys' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LPUSHX a c d e', ':4' ],
        [ 'LPUSHX a f g', ':6' ],
      ]
    end
  end

  describe 'RPUSH' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'RPUSH a', '-ERR wrong number of arguments for \'RPUSH\' command' ],
      ]
    end

    it 'fails if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'RPUSH not-a-list a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'creates a list for a non existing key' do
      assert_command_results [
        [ 'RPUSH a b', ':1' ],
      ]
    end

    it 'returns the number of elements in the list after insert' do
      assert_command_results [
        [ 'RPUSH a b', ':1' ],
        [ 'RPUSH a c', ':2' ],
        [ 'RPUSH a d', ':3' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd' ] ],
      ]
    end

    it 'handles multiple keys' do
      assert_command_results [
        [ 'RPUSH a b c d e', ':4' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd', 'e' ] ],
        [ 'RPUSH a f g', ':6' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd', 'e', 'f', 'g' ] ],
      ]
    end
  end

  describe 'RPUSHX' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'RPUSHX', '-ERR wrong number of arguments for \'RPUSHX\' command' ],
        [ 'RPUSHX a', '-ERR wrong number of arguments for \'RPUSHX\' command' ],
      ]
    end

    it 'fails if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'RPUSHX not-a-list a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'does nothing a non existing key' do
      assert_command_results [
        [ 'RPUSHX a b', ':0' ],
      ]
    end

    it 'returns the number of elements in the list after insert' do
      assert_command_results [
        [ 'RPUSH a b', ':1' ],
        [ 'RPUSHX a b', ':2' ],
        [ 'RPUSHX a c', ':3' ],
        [ 'RPUSHX a d', ':4' ],
        [ 'LRANGE a 0 -1', [ 'b', 'b', 'c', 'd' ] ],
      ]
    end

    it 'handles multiple keys' do
      assert_command_results [
        [ 'RPUSH a b', ':1' ],
        [ 'RPUSHX a c d e', ':4' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd', 'e' ] ],
        [ 'RPUSHX a f g', ':6' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd', 'e', 'f', 'g' ] ],
      ]
    end
  end


  describe 'LLEN' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LLEN', '-ERR wrong number of arguments for \'LLEN\' command' ],
        [ 'LLEN a b', '-ERR wrong number of arguments for \'LLEN\' command' ],
      ]
    end

    it 'returns an error if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LLEN not-a-list', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns 0 for non existing keys' do
      assert_command_results [
        [ 'LLEN not-a-thing', ':0' ],
      ]
    end

    it 'returns the size of the list' do
      assert_command_results [
        [ 'LPUSH a b c d', ':3' ],
        [ 'LLEN a', ':3' ],
      ]
    end
  end

  describe 'LPOP' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LPOP', '-ERR wrong number of arguments for \'LPOP\' command' ],
        [ 'LPOP a b', '-ERR wrong number of arguments for \'LPOP\' command' ],
      ]
    end

    it 'returns an error if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LPOP not-a-list', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns nil for non existing keys' do
      assert_command_results [
        [ 'LPOP not-a-thing', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns the head of list and removes it from the list' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LPOP a', 'b' ],
        [ 'LLEN a', ':0' ],
        [ 'LPUSH a c b', ':2' ],
        [ 'LPOP a', 'b' ],
        [ 'LLEN a', ':1' ],
        [ 'LPUSH a b 1 2 3', ':5' ],
        [ 'LPOP a', '3' ],
        [ 'LRANGE a 0 -1', [ '2', '1', 'b', 'c' ] ],
      ]
    end

    it 'deletes the key/value pair after popping the last element' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'TYPE a', '+list' ],
        [ 'LPOP a', 'b' ],
        [ 'TYPE a', '+none' ],
      ]
    end
  end

  describe 'RPOP' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'RPOP', '-ERR wrong number of arguments for \'RPOP\' command' ],
        [ 'RPOP a b', '-ERR wrong number of arguments for \'RPOP\' command' ],
      ]
    end

    it 'returns an error if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'RPOP not-a-list', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns nil for non existing keys' do
      assert_command_results [
        [ 'RPOP not-a-thing', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns the tail of list and removes it from the list' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'RPOP a', 'b' ],
        [ 'LLEN a', ':0' ],
        [ 'LPUSH a c b', ':2' ],
        [ 'RPOP a', 'c' ],
        [ 'LLEN a', ':1' ],
        [ 'LPUSH a b 1 2 3', ':5' ],
        [ 'RPOP a', 'b' ],
        [ 'LRANGE a 0 -1', [ '3', '2', '1', 'b' ] ],
      ]
    end

    it 'deletes the key/value pair after popping the last element' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'TYPE a', '+list' ],
        [ 'RPOP a', 'b' ],
        [ 'TYPE a', '+none' ],
      ]
    end
  end

  describe 'RPOPLPUSH' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'RPOPLPUSH', '-ERR wrong number of arguments for \'RPOPLPUSH\' command' ],
        [ 'RPOPLPUSH a', '-ERR wrong number of arguments for \'RPOPLPUSH\' command' ],
        [ 'RPOPLPUSH a b c', '-ERR wrong number of arguments for \'RPOPLPUSH\' command' ],
      ]
    end

    it 'returns an error if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'RPOPLPUSH not-a-list not-a-list', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns nil for non existing keys' do
      assert_command_results [
        [ 'RPOPLPUSH not-a-thing not-a-thing', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'works with the same list, it rotates it' do
      assert_command_results [
        [ 'LPUSH source c b a', ':3' ],
        [ 'LRANGE source 0 -1', [ 'a', 'b', 'c' ] ],
        [ 'RPOPLPUSH source source', 'c' ],
        [ 'LRANGE source 0 -1', [ 'c', 'a', 'b' ] ],
      ]
      # Also works with a single element
      assert_command_results [
        [ 'LPUSH source a', ':1' ],
        [ 'LRANGE source 0 -1', [ 'a' ] ],
        [ 'RPOPLPUSH source source', 'a' ],
        [ 'LRANGE source 0 -1', [ 'a' ] ],
      ]
    end

    it 'creates the destination list if it does not exist' do
      assert_command_results [
        [ 'LPUSH source a', ':1' ],
        [ 'RPOPLPUSH source destination', 'a' ],
        [ 'LRANGE source 0 -1', [] ],
        [ 'LRANGE destination 0 -1', [ 'a' ] ],
      ]
    end

    it 'returns the tail of the source and pushes it to the head of the destination' do
      assert_command_results [
        [ 'RPUSH source a b c', ':3' ],
        [ 'RPUSH destination 1 2 3', ':3' ],
        [ 'RPOPLPUSH source destination', 'c' ],
        [ 'LRANGE source 0 -1', [ 'a', 'b' ] ],
        [ 'LRANGE destination 0 -1', [ 'c', '1', '2', '3' ] ],
        [ 'RPOPLPUSH source destination', 'b' ],
        [ 'LRANGE source 0 -1', [ 'a' ] ],
        [ 'LRANGE destination 0 -1', [ 'b', 'c', '1', '2', '3' ] ],
      ]
    end
  end

  describe 'LTRIM' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LTRIM', '-ERR wrong number of arguments for \'LTRIM\' command' ],
        [ 'LTRIM a', '-ERR wrong number of arguments for \'LTRIM\' command' ],
        [ 'LTRIM a b', '-ERR wrong number of arguments for \'LTRIM\' command' ],
        [ 'LTRIM a b c d', '-ERR wrong number of arguments for \'LTRIM\' command' ],
      ]
    end

    it 'fails if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LTRIM not-a-list 0 -1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'fails it start & stop are not integers' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LTRIM a not-a-number not-a-number', '-ERR value is not an integer or out of range' ],
        [ 'LTRIM a 0 not-a-number', '-ERR value is not an integer or out of range' ],
        [ 'LTRIM a not-a-number -1', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'does nothing with 0 -1' do
      assert_command_results [
        [ 'LPUSH a g f e d c b', ':6' ],
        [ 'LTRIM a 0 -1', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd', 'e', 'f', 'g' ] ],
      ]
    end

    it 'handles negative indexes as starting from the right side for start' do
      assert_command_results [
        [ 'LPUSH a d c b', ':3' ],
        [ 'LTRIM a -3 2', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd' ] ],
      ]
    end

    it 'handles negative indexes as starting from the right side for end' do
      assert_command_results [
        [ 'LPUSH a d c b', ':3' ],
        [ 'LTRIM a -1 2', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'd' ] ],
      ]
    end

    it 'works with out of bounds indices' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LTRIM a 2 22', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'd' ] ]
      ]
    end

    it 'deletes the list for out of order boundaries' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LTRIM a 2 1', '+OK' ],
        [ 'TYPE a', '+none' ],
        [ 'LPUSH a b', ':1' ],
        [ 'LTRIM a -1 -2', '+OK' ],
        [ 'TYPE a', '+none' ],
      ]
    end

    it 'only keeps the sublist indicated by the range' do
      assert_command_results [
        [ 'RPUSH a b c d e f', ':5' ],
        [ 'LTRIM a 1 1', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'c' ] ],
      ]
      assert_command_results [
        [ 'RPUSH a b c d e f', ':5' ],
        [ 'LTRIM a 1 3', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'c', 'd', 'e' ] ],
      ]
      assert_command_results [
        [ 'RPUSH a b c d e f', ':5' ],
        [ 'LTRIM a 3 4', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'e', 'f' ] ],
      ]
      assert_command_results [
        [ 'RPUSH a b c d e f', ':5' ],
        [ 'LTRIM a 3 100', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'e', 'f' ] ],
      ]
    end
  end

  describe 'LSET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LSET', '-ERR wrong number of arguments for \'LSET\' command' ],
        [ 'LSET a', '-ERR wrong number of arguments for \'LSET\' command' ],
        [ 'LSET a b', '-ERR wrong number of arguments for \'LSET\' command' ],
        [ 'LSET a b c d', '-ERR wrong number of arguments for \'LSET\' command' ],
      ]
    end

    it 'returns an error if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LSET not-a-list 0 new-value', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns and error if index is not an integer' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LSET a not-a-number new-value', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns an error for non existing keys' do
      assert_command_results [
        [ 'LSET not-a-thing 0 new-value', '-ERR no such key' ],
      ]
    end

    it 'replaces the element at the given index' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LSET a 1 new-value', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'b', 'new-value', 'd' ] ],
      ]
    end

    it 'replaces the element at the given starting from the end with a negative index' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LSET a -1 new-value', '+OK' ],
        [ 'LSET a -2 another-new-value', '+OK' ],
        [ 'LRANGE a 0 -1', [ 'b', 'another-new-value', 'new-value' ] ],
      ]
    end

    it 'returns an error for out of range indexes' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LSET a 3 new-value', '-ERR index out of range' ],
        [ 'LSET a 4 new-value', '-ERR index out of range' ],
        [ 'LSET a -4 new-value', '-ERR index out of range' ],
        [ 'LSET a -5 new-value', '-ERR index out of range' ],
      ]
    end
  end

  describe 'LREM' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LREM', '-ERR wrong number of arguments for \'LREM\' command' ],
        [ 'LREM a', '-ERR wrong number of arguments for \'LREM\' command' ],
        [ 'LREM a b', '-ERR wrong number of arguments for \'LREM\' command' ],
        [ 'LREM a b c d', '-ERR wrong number of arguments for \'LREM\' command' ],
      ]
    end

    it 'returns an error if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LREM not-a-list 0 new-value', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns and error if index is not an integer' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LREM a not-a-number b', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns 0 if no nodes are equal to element' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LREM a 0 e', ':0' ],
      ]
    end

    it 'returns 0 for non existing keys' do
      assert_command_results [
        [ 'LREM not-a-thing 0 new-value', ':0' ],
      ]
    end

    it 'removes the first n nodes equal to element from the end with a positive index' do
      assert_command_results [
        [ 'RPUSH a b c d b e f b g h', ':9' ],
        [ 'LREM a 2 b', ':2' ],
        [ 'LRANGE a 0 -1', [ 'c', 'd', 'e', 'f', 'b', 'g', 'h' ] ],
        [ 'LREM a 2 b', ':1' ],
        [ 'LRANGE a 0 -1', [ 'c', 'd', 'e', 'f', 'g', 'h' ] ],
      ]
    end

    it 'removes the first n nodes equal to element from the end with a negative index' do
      assert_command_results [
        [ 'RPUSH a b c d b e f b g h', ':9' ],
        [ 'LREM a -2 b', ':2' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd', 'e', 'f', 'g', 'h' ] ],
        [ 'LREM a 2 b', ':1' ],
        [ 'LRANGE a 0 -1', [ 'c', 'd', 'e', 'f', 'g', 'h' ] ],
      ]
    end

    it 'removes all elements if count is 0' do
      assert_command_results [
        [ 'RPUSH a b c d b e f b g h', ':9' ],
        [ 'LREM a 0 b', ':3' ],
        [ 'LRANGE a 0 -1', [ 'c', 'd', 'e', 'f', 'g', 'h' ] ],
        [ 'LREM a 0 b', ':0' ],
        [ 'LRANGE a 0 -1', [ 'c', 'd', 'e', 'f', 'g', 'h' ] ],
      ]
    end
  end

  describe 'LPOS' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LPOS', '-ERR wrong number of arguments for \'LPOS\' command' ],
        [ 'LPOS a', '-ERR wrong number of arguments for \'LPOS\' command' ],
        [ 'LPOS a b c', '-ERR syntax error' ],
        [ 'LPOS a b RANK', '-ERR syntax error' ],
        [ 'LPOS a b RANK 1 COUNT', '-ERR syntax error' ],
        [ 'LPOS a b COUNT 1 RANK', '-ERR syntax error' ],
        [ 'LPOS a b MAXLEN 1 COUNT', '-ERR syntax error' ],
        [ 'LPOS a b COUNT 1 MAXLEN', '-ERR syntax error' ],
      ]
    end

    it 'returns nil if the key does not exist' do
      assert_command_results [
        [ 'LPOS not-a-thing 0', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns an error if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LPOS not-a-list 0', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns and error if count is not an integer' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LPOS a b COUNT not-a-number', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns and error if rank is not an integer' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LPOS a b RANK not-a-number', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns and error if maxlen is not an integer' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LPOS a b MAXLEN not-a-number', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns the 0-index position of the element in the list' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LPOS a c', ':1' ],
      ]
    end

    it 'returns nil if the element is not in the list' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LPOS a e', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns the position of the nth element with the RANK option' do
      assert_command_results [
        [ 'RPUSH a b c b d', ':4' ],
        [ 'LPOS a b RANK 1', ':0' ],
        [ 'LPOS a b RANK 2', ':2' ],
        [ 'LPOS a b RANK 3', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns the position of the nth element from the right with a negative RANK option' do
      assert_command_results [
        [ 'RPUSH a b c b d', ':4' ],
        [ 'LPOS a b RANK -1', ':2' ],
        [ 'LPOS a b RANK -2', ':0' ],
        [ 'LPOS a b RANK -3', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns an error with a rank of 0' do
      assert_command_results [
        [ 'RPUSH a b c b d', ':4' ],
        [ 'LPOS a b RANK 0', '-ERR RANK can\'t be zero: use 1 to start from the first match, 2 from the second, ...' ],
      ]
    end

    it 'returns an error with a negative count' do
      assert_command_results [
        [ 'RPUSH a b c b d', ':4' ],
        [ 'LPOS a b COUNT -1', '-ERR COUNT can\'t be negative' ],
      ]
    end

    it 'returns an error with a negative maxlen' do
      assert_command_results [
        [ 'RPUSH a b c b d', ':4' ],
        [ 'LPOS a b MAXLEN -1', '-ERR MAXLEN can\'t be negative' ],
      ]
    end

    it 'returns an array on indexes for the first n matches with the count option' do
      assert_command_results [
        [ 'RPUSH a b c b d', ':4' ],
        [ 'LPOS a b COUNT 1', "*1\r\n:0\r\n" ],
        [ 'LPOS a b COUNT 2', "*2\r\n:0\r\n:2\r\n" ],
        [ 'LPOS a b COUNT 3', "*2\r\n:0\r\n:2\r\n" ],
      ]
    end

    it 'returns an array on indexes for all the matches with 0 as the count option' do
      assert_command_results [
        [ 'RPUSH a b c b d', ':4' ],
        [ 'LPOS a b COUNT 0', "*2\r\n:0\r\n:2\r\n" ],
      ]
    end

    it 'returns the first n elements after the rank with both count and rank' do
      assert_command_results [
        [ 'RPUSH a b c b d b', ':5' ],
        [ 'LPOS a b COUNT 1 RANK 1', "*1\r\n:0\r\n" ],
        [ 'LPOS a b COUNT 2 RANK 1', "*2\r\n:0\r\n:2\r\n" ],
        [ 'LPOS a b COUNT 3 RANK 1', "*3\r\n:0\r\n:2\r\n:4\r\n" ],
        [ 'LPOS a b COUNT 4 RANK 1', "*3\r\n:0\r\n:2\r\n:4\r\n" ],
        [ 'LPOS a b COUNT 1 RANK 2', "*1\r\n:2\r\n" ],
        [ 'LPOS a b COUNT 2 RANK 2', "*2\r\n:2\r\n:4\r\n" ],
        [ 'LPOS a b COUNT 3 RANK 2', "*2\r\n:2\r\n:4\r\n" ],
        [ 'LPOS a b COUNT 1 RANK 3', "*1\r\n:4\r\n" ],
        [ 'LPOS a b COUNT 2 RANK 3', "*1\r\n:4\r\n" ],
        [ 'LPOS a b COUNT 1 RANK 4', "*0\r\n" ],
      ]
    end

    it 'returns an array on indexes for the last n matches with the count option and a negative RANK' do
      assert_command_results [
        [ 'RPUSH a b c b d b', ':5' ],
        [ 'LPOS a b COUNT 1 RANK -1', "*1\r\n:4\r\n" ],
        [ 'LPOS a b COUNT 2 RANK -1', "*2\r\n:4\r\n:2\r\n" ],
        [ 'LPOS a b COUNT 3 RANK -1', "*3\r\n:4\r\n:2\r\n:0\r\n" ],
        [ 'LPOS a b COUNT 4 RANK -1', "*3\r\n:4\r\n:2\r\n:0\r\n" ],
        [ 'LPOS a b COUNT 1 RANK -2', "*1\r\n:2" ],
        [ 'LPOS a b COUNT 2 RANK -2', "*2\r\n:2\r\n:0" ],
        [ 'LPOS a b COUNT 3 RANK -2', "*2\r\n:2\r\n:0" ],
        [ 'LPOS a b COUNT 1 RANK -3', "*1\r\n:0" ],
        [ 'LPOS a b COUNT 2 RANK -3', "*1\r\n:0" ],
        [ 'LPOS a b COUNT 1 RANK -4', "*0\r\n" ],
      ]
    end

    it 'returns an empty array when count is specified and not matches are found' do
      assert_command_results [
        [ 'RPUSH a b c b d', ':4' ],
        [ 'LPOS a e COUNT 0', [] ],
      ]
    end

    it 'only scans n element with the MAXLEN option' do
      assert_command_results [
        [ 'RPUSH a b c b d b', ':5' ],
        [ 'LPOS a b MAXLEN 3 COUNT 3', "*2\r\n:0\r\n:2\r\n" ],
        [ 'LPOS a b MAXLEN 2 COUNT 3', "*1\r\n:0\r\n" ],
      ]
    end

    it 'only scans n element starting from the end with the MAXLEN option and a negative rank' do
      assert_command_results [
        [ 'RPUSH a b c b d b', ':5' ],
        [ 'LPOS a b MAXLEN 5 COUNT 3 RANK -1', "*3\r\n:4\r\n:2\r\n:0\r\n" ],
        [ 'LPOS a b MAXLEN 4 COUNT 3 RANK -1', "*2\r\n:4\r\n:2\r\n" ],
        [ 'LPOS a b MAXLEN 3 COUNT 3 RANK -1', "*2\r\n:4\r\n:2\r\n" ],
        [ 'LPOS a b MAXLEN 2 COUNT 3 RANK -1', "*1\r\n:4\r\n" ],
        [ 'LPOS a b MAXLEN 1 COUNT 3 RANK -1', "*1\r\n:4\r\n" ],
      ]
    end
  end

  describe 'LINSERT' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LINSERT', '-ERR wrong number of arguments for \'LINSERT\' command' ],
        [ 'LINSERT a', '-ERR wrong number of arguments for \'LINSERT\' command' ],
        [ 'LINSERT a b', '-ERR wrong number of arguments for \'LINSERT\' command' ],
        [ 'LINSERT a b c', '-ERR wrong number of arguments for \'LINSERT\' command' ],
        [ 'LINSERT a b c d', '-ERR syntax error' ],
        [ 'LINSERT a b c d e', '-ERR wrong number of arguments for \'LINSERT\' command' ],
        [ 'LINSERT a before c d e', '-ERR wrong number of arguments for \'LINSERT\' command' ],
        [ 'LINSERT a after c d e', '-ERR wrong number of arguments for \'LINSERT\' command' ],
      ]
    end

    it 'returns 0 if the key does not exist' do
      assert_command_results [
        [ 'LINSERT not-a-thing BEFORE 0 a', ':0' ],
        [ 'LINSERT not-a-thing AFTER 0 a', ':0' ],
      ]
    end

    it 'returns an error if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LINSERT not-a-list BEFORE a a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'LINSERT not-a-list AFTER a a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'inserts the new element before the pivot with BEFORE and returns the new size' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LINSERT a BEFORE c new-element', ':4' ],
        [ 'LRANGE a 0 -1', [ 'b', 'new-element', 'c', 'd' ] ],
        [ 'LINSERT a BEFORE b new-head', ':5' ],
        [ 'LRANGE a 0 -1', [ 'new-head', 'b', 'new-element', 'c', 'd' ] ],
      ]
    end

    it 'inserts the new element after the pivot with AFTER and returns the new size' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LINSERT a AFTER c new-element', ':4' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'new-element', 'd' ] ],
        [ 'LINSERT a AFTER d new-tail', ':5' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'new-element', 'd', 'new-tail' ] ],
      ]
    end

    it 'returns -1 if the pivot is not found' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LINSERT a BEFORE e new-element', ':-1' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd' ] ],
        [ 'LINSERT a AFTER e new-element', ':-1' ],
        [ 'LRANGE a 0 -1', [ 'b', 'c', 'd' ] ],
      ]
    end
  end

  describe 'LINDEX' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'LINDEX', '-ERR wrong number of arguments for \'LINDEX\' command' ],
        [ 'LINDEX a', '-ERR wrong number of arguments for \'LINDEX\' command' ],
        [ 'LINDEX a b c', '-ERR wrong number of arguments for \'LINDEX\' command' ],
      ]
    end

    it 'returns nil if the key does not exist' do
      assert_command_results [
        [ 'LINDEX not-a-thing 0', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns an error if index is not an integer' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LINDEX a b', '-ERR value is not an integer or out of range' ],
        [ 'LINDEX a 1.0', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns nil if the index is out of range' do
      assert_command_results [
        [ 'LPUSH a b', ':1' ],
        [ 'LINDEX a 1', BYORedis::NULL_BULK_STRING ],
        [ 'LINDEX a -2', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns an error if the key is not a list' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'LINDEX not-a-list 0', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the value at the given index' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LINDEX a 0', 'b' ],
        [ 'LINDEX a 1', 'c' ],
        [ 'LINDEX a 2', 'd' ],
      ]
    end

    it 'handles negative indexes' do
      assert_command_results [
        [ 'RPUSH a b c d', ':3' ],
        [ 'LINDEX a -1', 'd' ],
        [ 'LINDEX a -2', 'c' ],
        [ 'LINDEX a -3', 'b' ],
      ]
    end
  end

  describe 'BLPOP' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'BLPOP', '-ERR wrong number of arguments for \'BLPOP\' command' ],
        [ 'BLPOP a', '-ERR wrong number of arguments for \'BLPOP\' command' ],
      ]
    end

    it 'returns an error if the last argument is not an integer or a float' do
      assert_command_results [
        [ 'BLPOP a b', '-ERR timeout is not a float or out of range' ],
        [ 'BLPOP a 1.b', '-ERR timeout is not a float or out of range' ],
      ]
    end

    it 'returns an error if the first non nil value is not a key' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'BLPOP a b not-a-list 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the popped element of the first non empty list and its name' do
      assert_command_results [
        [ 'RPUSH a a1 a2 a3', ':3' ],
        [ 'BLPOP a 1', [ 'a', 'a1' ] ],
        [ 'LRANGE a 0 -1', [ 'a2', 'a3' ] ],
        [ 'RPUSH b b1 b2 b3', ':3' ],
        [ 'BLPOP b a 1', [ 'b', 'b1' ] ],
        [ 'BLPOP a b 1', [ 'a', 'a2' ] ],
        [ 'LRANGE a 0 -1', [ 'a3' ] ],
        [ 'LRANGE b 0 -1', [ 'b2', 'b3' ] ],
      ]
    end

    it 'handles the case where the blocked client disconnects before the end of the timeout' do
      with_server do |socket|
        socket.write to_query('BLPOP', 'a', '0.2')
        socket.close
        sleep 0.2

        # Check that the server is still here!
        socket = TCPSocket.new 'localhost', 2000
        socket.write to_query('GET', 'a')
        response = read_response(socket, read_timeout: 0.2)

        assert_equal("$-1\r\n", response)
      end
    end

    it 'does nothing when the blocked client disconnects before one of lists it was blocked receives an element' do
      with_server do |socket|
        socket.write to_query('BLPOP', 'a', '0.2')
        socket.close
        sleep 0.01 # Sleep long enough to give time to the server to handle the disconnect

        socket2 = TCPSocket.new 'localhost', 2000
        socket2.write to_query('RPUSH', 'a', 'a1', 'a2')
        response = read_response(socket2, read_timeout: 0.2)
        assert_equal(":2\r\n", response)

        socket2.write to_query('LPOP', 'a')
        response = read_response(socket2, read_timeout: 0.2)
        # Checking that a1 was not popped since the client that asked for it disconnected
        assert_equal("$2\r\na1\r\n", response)
      end
    end

    it 'blocks up to timeout seconds and returns nil if all list are empty' do
      with_server do |server_socket|
        start_time = Time.now
        server_socket.write to_query('BLPOP', 'a', '0.1')

        response = read_response(server_socket, read_timeout: 0.2)
        duration = Time.now - start_time

        assert_equal("*-1\r\n", response)
        assert_operator(duration, :>=, 0.1)
      end
    end

    it 'clears the blocked states after a timeout' do
      with_server do |server_socket|
        start_time = Time.now
        server_socket.write to_query('BLPOP', 'a', '0.1')

        response = read_response(server_socket, read_timeout: 0.2)
        duration = Time.now - start_time

        assert_equal("*-1\r\n", response)

        server_socket.write to_query('LPUSH', 'a', 'a1')
        response2 = read_response(server_socket)

        assert_equal(":1\r\n", response2)

        assert_operator(duration, :>=, 0.1)
      end
    end

    it 'blocks and returns if the first list receives elements during the timeout' do
      assert_blocking_behavior(blocking_command: 'BLPOP',
                               list_names: [ 'a', 'b' ],
                               timeout: 0.5,
                               push_commands: [ 'RPUSH a a1' ],
                               expected_response: "*2\r\n$1\r\na\r\n$2\r\na1\r\n",
                               pushed_to_list_name: 'a')
    end

    it 'blocks and returns if the second list receives elements during the timeout' do
      assert_blocking_behavior(blocking_command: 'BLPOP',
                               list_names: [ 'a', 'b' ],
                               timeout: 0.5,
                               push_commands: [ 'RPUSH b b1' ],
                               expected_response: "*2\r\n$1\r\nb\r\n$2\r\nb1\r\n",
                               pushed_to_list_name: 'b')
    end

    it 'accumulates commands while blocked' do
      assert_command_accumulation(blocking_command: 'BLPOP a b 0.6',
                                  expected_response: "*-1\r\n+OK\r\n$1\r\nb\r\n")
    end

    it 'handles back to back blocking commands with buffering' do
      assert_back_to_back_blocking_commands_before_timeout(
        blocking_command: 'BLPOP',
        expected_response1: "*2\r\n$1\r\na\r\n$2\r\na1\r\n",
        expected_response2: "*2\r\n$1\r\na\r\n$2\r\na2\r\n"
      )
    end

    it 'handles back to back blocking commands after a timeout' do
      assert_back_to_back_blocking_commands_after_timeout(
        blocking_command: 'BLPOP'
      )
    end

    it 'processes buffered commands after being unblocked' do
      # Three responses: [ a, a1 ], +OK & 1
      assert_buffer_commands_processing(
        blocking_command: 'BLPOP',
        expected_response: "*2\r\n$1\r\na\r\n$2\r\na1\r\n+OK\r\n$1\r\n1\r\n"
      )
    end

    it 'can unblock as many clients as possible' do
      assert_multiple_clients_unblocked(blocking_command: 'BLPOP', expected_results: [
                                          "*2\r\n$1\r\na\r\n$2\r\na1\r\n",
                                          "*2\r\n$1\r\na\r\n$2\r\na2\r\n",
                                          "*2\r\n$1\r\na\r\n$2\r\na3\r\n",
                                        ])
    end
  end

  describe 'BRPOP' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'BRPOP', '-ERR wrong number of arguments for \'BRPOP\' command' ],
        [ 'BRPOP a', '-ERR wrong number of arguments for \'BRPOP\' command' ],
      ]
    end

    it 'returns an error if the last argument is not an integer or a float' do
      assert_command_results [
        [ 'BRPOP a b', '-ERR timeout is not a float or out of range' ],
        [ 'BRPOP a 1.b', '-ERR timeout is not a float or out of range' ],
      ]
    end

    it 'returns an error if the first non nil value is not a key' do
      assert_command_results [
        [ 'SET not-a-list 1', '+OK' ],
        [ 'BRPOP a b not-a-list 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the popped element of the first non empty list and its name' do
      assert_command_results [
        [ 'RPUSH a a1 a2 a3', ':3' ],
        [ 'BRPOP a 1', [ 'a', 'a3' ] ],
        [ 'LRANGE a 0 -1', [ 'a1', 'a2' ] ],
        [ 'RPUSH b b1 b2 b3', ':3' ],
        [ 'BRPOP b a 1', [ 'b', 'b3' ] ],
        [ 'BRPOP a b 1', [ 'a', 'a2' ] ],
        [ 'LRANGE a 0 -1', [ 'a1' ] ],
        [ 'LRANGE b 0 -1', [ 'b1', 'b2' ] ],
      ]
    end

    it 'does nothing when the blocked client disconnects before one of lists it was blocked receives an element' do
      with_server do |socket|
        socket.write to_query('BRPOP', 'a', '0.2')
        socket.close
        sleep 0.01 # Sleep long enough to give time to the server to handle the disconnect

        socket2 = TCPSocket.new 'localhost', 2000
        socket2.write to_query('RPUSH', 'a', 'a1', 'a2')
        response = read_response(socket2, read_timeout: 0.2)
        assert_equal(":2\r\n", response)

        socket2.write to_query('RPOP', 'a')
        response = read_response(socket2, read_timeout: 0.2)
        # Checking that a1 was not popped since the client that asked for it disconnected
        assert_equal("$2\r\na2\r\n", response)
      end
    end

    it 'blocks up to timeout seconds and returns nil if all list are empty' do
      with_server do |server_socket|
        start_time = Time.now
        server_socket.write to_query('BRPOP', 'a', '0.1')

        response = read_response(server_socket, read_timeout: 0.2)
        duration = Time.now - start_time

        assert_equal("*-1\r\n", response)
        assert_operator(duration, :>=, 0.1)
      end
    end

    it 'blocks and returns if the first list receives elements during the timeout' do
      assert_blocking_behavior(blocking_command: 'BRPOP',
                               list_names: [ 'a', 'b' ],
                               timeout: 0.5,
                               push_commands: [ 'RPUSH a a1' ],
                               expected_response: "*2\r\n$1\r\na\r\n$2\r\na1\r\n",
                               pushed_to_list_name: 'a')
    end

    it 'blocks and returns if the second list receives elements during the timeout' do
      assert_blocking_behavior(blocking_command: 'BRPOP',
                               list_names: [ 'a', 'b' ],
                               timeout: 0.5,
                               push_commands: [ 'RPUSH b b1' ],
                               expected_response: "*2\r\n$1\r\nb\r\n$2\r\nb1\r\n",
                               pushed_to_list_name: 'b')
    end

    it 'accumulates commands while blocked' do
      assert_command_accumulation(blocking_command: 'BRPOP a b 0.6',
                                  expected_response: "*-1\r\n+OK\r\n$1\r\nb\r\n")
    end

    it 'handles back to back blocking commands with buffering' do
      assert_back_to_back_blocking_commands_before_timeout(
        blocking_command: 'BRPOP',
        expected_response1: "*2\r\n$1\r\na\r\n$2\r\na1\r\n",
        expected_response2: "*2\r\n$1\r\na\r\n$2\r\na2\r\n"
      )
    end

    it 'handles back to back blocking commands after a timeout' do
      assert_back_to_back_blocking_commands_after_timeout(
        blocking_command: 'BRPOP'
      )
    end

    it 'can unblock as many clients as possible' do
      assert_multiple_clients_unblocked(blocking_command: 'BRPOP', expected_results: [
                                          "*2\r\n$1\r\na\r\n$2\r\na3\r\n",
                                          "*2\r\n$1\r\na\r\n$2\r\na2\r\n",
                                          "*2\r\n$1\r\na\r\n$2\r\na1\r\n",
                                        ])
    end

    it 'processes buffered commands after being unblocked' do
      # Three responses: [ a, a1 ], +OK & 1
      assert_buffer_commands_processing(
        blocking_command: 'BRPOP',
        expected_response: "*2\r\n$1\r\na\r\n$2\r\na2\r\n+OK\r\n$1\r\n1\r\n"
      )
    end
  end

  describe 'BRPOPLPUSH' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'BRPOPLPUSH', '-ERR wrong number of arguments for \'BRPOPLPUSH\' command' ],
        [ 'BRPOPLPUSH a b', '-ERR wrong number of arguments for \'BRPOPLPUSH\' command' ],
        [ 'BRPOPLPUSH a b c d', '-ERR wrong number of arguments for \'BRPOPLPUSH\' command' ],
      ]
    end

    it 'returns an error if the last argument is not an integer or a float' do
      assert_command_results [
        [ 'BRPOPLPUSH a b foo', '-ERR timeout is not a float or out of range' ],
        [ 'BRPOPLPUSH a b 1.b', '-ERR timeout is not a float or out of range' ],
      ]
    end

    it 'blocks up to timeout seconds and returns nil if all list are empty' do
      with_server do |server_socket|
        start_time = Time.now
        server_socket.write to_query('BRPOPLPUSH', 'source', 'destination', '0.1')

        response = read_response(server_socket, read_timeout: 0.2)
        duration = Time.now - start_time

        assert_equal("*-1\r\n", response)
        assert_operator(duration, :>=, 0.1)
      end
    end

    it 'blocks and pops/pushes when source receives a push' do
      with_server do |socket|
        socket2 = TCPSocket.new 'localhost', 2000
        Thread.new do
          # Wait enough for the first blocking command to be received
          sleep 0.02
          socket2.write to_query('RPUSH', 'source', 'something', 'else')
        end
        socket.write to_query('BRPOPLPUSH', 'source', 'destination', '0.5')
        sleep 0.01 # Wait enough so that the server receives and processes the first command

        response = read_response(socket)
        assert_equal("$4\r\nelse\r\n", response)

        socket.write to_query('LRANGE', 'source', '0', '-1')
        response = read_response(socket)
        assert_equal("*1\r\n$9\r\nsomething\r\n", response)

        socket.write to_query('LRANGE', 'destination', '0', '-1')
        response = read_response(socket)
        assert_equal("*1\r\n$4\r\nelse\r\n", response)
      end
    end

    it 'blocks and pops/pushes when source receives a push' do
      with_server do |socket|
        socket2 = TCPSocket.new 'localhost', 2000
        Thread.new do
          # Wait enough for the first blocking command to be received
          sleep 0.02
          socket2.write to_query('RPUSH', 'a', 'a1')
        end
        socket.write to_query('BRPOPLPUSH', 'a', 'a', '0.5')
        sleep 0.01 # Wait enough so that the server receives and processes the first command

        response = read_response(socket)
        assert_equal("$2\r\na1\r\n", response)

        socket.write to_query('LRANGE', 'a', '0', '-1')
        response = read_response(socket)
        assert_equal("*1\r\n$2\r\na1\r\n", response)
      end
    end

    it 'blocks and returns if the source receives elements during the timeout' do
      assert_blocking_behavior(blocking_command: 'BRPOPLPUSH',
                               list_names: [ 'source', 'destination' ],
                               timeout: 0.5,
                               push_commands: [ 'LPUSH source a1' ],
                               expected_response: "$2\r\na1\r\n",
                               pushed_to_list_name: 'source')

      assert_blocking_behavior(blocking_command: 'BRPOPLPUSH',
                               list_names: [ 'source', 'destination' ],
                               timeout: 0.5,
                               push_commands: [ 'LPUSH another-list a1', 'RPOPLPUSH another-list source' ],
                               expected_response: "$2\r\na1\r\n",
                               pushed_to_list_name: 'source')

      assert_blocking_behavior(blocking_command: 'BRPOPLPUSH',
                               list_names: [ 'source', 'destination' ],
                               timeout: 0.5,
                               push_commands: [ 'LPUSH another-list a1', 'BRPOPLPUSH another-list source 1' ],
                               expected_response: "$2\r\na1\r\n",
                               pushed_to_list_name: 'source')
    end

    it 'handles back to back blocking commands with buffering' do
      assert_back_to_back_blocking_commands_before_timeout(
        blocking_command: 'BRPOPLPUSH',
        expected_response1: "$2\r\na1\r\n",
        expected_response2: "$2\r\na2\r\n"
      )
    end

    it 'handles back to back blocking commands after a timeout' do
      assert_back_to_back_blocking_commands_after_timeout(
        blocking_command: 'BRPOPLPUSH'
      )
    end

    it 'can unblock as many clients as possible' do
      assert_multiple_clients_unblocked(blocking_command: 'BRPOPLPUSH', expected_results: [
                                          "$2\r\na3\r\n",
                                          "$2\r\na2\r\n",
                                          "$2\r\na1\r\n",
                                        ])
    end

    it 'processes buffered commands after being unblocked' do
      # Three responses: a2, +OK & 1
      assert_buffer_commands_processing(
        blocking_command: 'BRPOPLPUSH',
        expected_response: "$2\r\na2\r\n+OK\r\n$1\r\n1\r\n"
      )
    end

    # TODO: These tests are the same RPOPLPUSH, use a shared helper
    it 'returns the tail of the source and pushes it to the head of the destination' do
      assert_command_results [
        [ 'RPUSH source a b c', ':3' ],
        [ 'RPUSH destination 1 2 3', ':3' ],
        [ 'BRPOPLPUSH source destination 1', 'c' ],
        [ 'LRANGE source 0 -1', [ 'a', 'b' ] ],
        [ 'LRANGE destination 0 -1', [ 'c', '1', '2', '3' ] ],
        [ 'BRPOPLPUSH source destination 1', 'b' ],
        [ 'LRANGE source 0 -1', [ 'a' ] ],
        [ 'LRANGE destination 0 -1', [ 'b', 'c', '1', '2', '3' ] ],
      ]
    end

    it 'works with the same list, it rotates it' do
      assert_command_results [
        [ 'LPUSH source c b a', ':3' ],
        [ 'LRANGE source 0 -1', [ 'a', 'b', 'c' ] ],
        [ 'BRPOPLPUSH source source 1', 'c' ],
        [ 'LRANGE source 0 -1', [ 'c', 'a', 'b' ] ],
      ]
      # Also works with a single element
      assert_command_results [
        [ 'LPUSH source a', ':1' ],
        [ 'LRANGE source 0 -1', [ 'a' ] ],
        [ 'BRPOPLPUSH source source 1', 'a' ],
        [ 'LRANGE source 0 -1', [ 'a' ] ],
      ]
    end

    it 'creates the destination list if it does not exist' do
      assert_command_results [
        [ 'LPUSH source a', ':1' ],
        [ 'BRPOPLPUSH source destination 1', 'a' ],
        [ 'LRANGE source 0 -1', [] ],
        [ 'LRANGE destination 0 -1', [ 'a' ] ],
      ]
    end
  end

  def assert_blocking_behavior(blocking_command:, list_names:, timeout:, push_commands:,
                               expected_response:, pushed_to_list_name:)
    with_server do |socket1|
      socket2 = TCPSocket.new 'localhost', 2000

      # Create a thread that will sleep for 200ms and push an element to the second list
      # the first client is blocked on, b
      Thread.new do
        sleep timeout / 2.5 # 200ms for a 500ms timeout
        push_commands.each do |push_command|
          socket2.write to_query(*push_command.split)
        end
      end

      start_time = Time.now
      full_blocking_command = [ blocking_command ] + list_names + [ timeout.to_s ]
      socket1.write to_query(*full_blocking_command)
      sleep timeout / 5 # 100ms for a 500ms timeout

      # First assert that we're still blocked after 100ms, there's nothing to read
      response1 = socket1.read_nonblock(1024, exception: false)
      assert_equal(:wait_readable, response1)

      response = read_response(socket1, read_timeout: timeout * 2)
      duration = Time.now - start_time

      assert_equal(expected_response, response)

      # Finally, we confirm that it took less than the BLPOP timeout to complete
      assert_operator(duration, :<=, timeout)
      # And a sanity check to kind of make sure that it lasted long enough to do _something_
      assert_operator(duration, :>=, 0.1)

      # Once we checked everything, let's just check that the state of the server is fine
      # There were bugs where the server was not cleaning up empty lists and subsequent pops
      # would fail
      socket1.write to_query('BLPOP', pushed_to_list_name, '0.05')
      response2 = read_response(socket1, read_timeout: 0.1)

      assert_equal(BYORedis::NULL_ARRAY, response2)
    end
  end

  def assert_command_accumulation(blocking_command:, expected_response:)
    with_server do |socket|
      start_time = Time.now
      socket.write to_query(*blocking_command.split)

      # Nothing to read right after the write
      response1 = socket.read_nonblock(1024, exception: false)
      assert_equal(:wait_readable, response1)

      sleep 0.1
      # Still nothing to read right after 100ms
      response2 = socket.read_nonblock(1024, exception: false)
      assert_equal(:wait_readable, response2)

      socket.write to_query('SET', 'a', 'b')
      # Nothing to read right after the write
      response3 = socket.read_nonblock(1024, exception: false)
      assert_equal(:wait_readable, response3)

      sleep 0.1
      # Still nothing to read after 100ms
      response4 = socket.read_nonblock(1024, exception: false)
      assert_equal(:wait_readable, response4)

      socket.write to_query('GET', 'a')
      # Nothing to read right after the write
      response5 = socket.read_nonblock(1024, exception: false)
      assert_equal(:wait_readable, response5)

      sleep 0.1
      # Still nothing to right after 100ms
      response6 = socket.read_nonblock(1024, exception: false)
      assert_equal(:wait_readable, response6)

      # Sleep 500ms to let the BLPOP timeout expire and give time to the server to serve
      # the pending commands
      sleep 0.5
      final_response = socket.read_nonblock(1024, exception: false)
      duration = Time.now - start_time
      # Three responses:
      # - empty array, for BLPOP
      # - +OK for the SET a b command
      # - The string 'b' for the GET a command
      assert_equal(expected_response, final_response)
      # Sanity check on the time, greater than 800s since BLPOP waits for 600 and we give a
      # large buffer of 200ms for the server to server the other 2
      assert_operator(duration, :<=, 0.9)
      assert_operator(duration, :>=, 0.8)
    end
  end

  def assert_back_to_back_blocking_commands_before_timeout(blocking_command:,
                                                           expected_response1:,
                                                           expected_response2:)
    with_server do |socket|
      socket2 = TCPSocket.new 'localhost', 2000
      Thread.new do
        # Wait enough for the first blocking command to be received
        sleep 0.01
        socket2.write to_query('RPUSH', 'a', 'a1')
        # Wait enough for the result of the first command to be received and read
        sleep 0.05
        socket2.write to_query('RPUSH', 'a', 'a2')
      end
      socket.write to_query(blocking_command, 'a', 'b', '0.5')
      sleep 0.01 # Wait enough so that the server receives and processes the first command
      # before receiving the second one
      socket.write to_query(blocking_command, 'a', 'b', '0.5')

      sleep 0.01
      response = read_response(socket)
      assert_equal(expected_response1, response)

      # Wait enough for the second RPUSH to be received
      sleep 0.05
      response2 = read_response(socket)
      assert_equal(expected_response2, response2)
    end
  end

  def assert_back_to_back_blocking_commands_after_timeout(blocking_command:)
    with_server do |socket|
      socket.write to_query(blocking_command, 'a', 'b', '0.1')
      socket.write to_query(blocking_command, 'a', 'b', '0.1')

      sleep 0.15
      response = read_response(socket)
      assert_equal("*-1\r\n", response)

      sleep 0.15
      response2 = read_response(socket)
      assert_equal("*-1\r\n", response2)
    end
  end

  def assert_multiple_clients_unblocked(blocking_command:, expected_results:)
    with_server do |socket1|
      socket2 = TCPSocket.new 'localhost', 2000
      socket3 = TCPSocket.new 'localhost', 2000
      socket4 = TCPSocket.new 'localhost', 2000

      socket1.write to_query(blocking_command, 'a', 'b', '0.1')
      sleep 0.01
      socket2.write to_query(blocking_command, 'a', 'b', '0.1')
      sleep 0.01
      socket3.write to_query(blocking_command, 'a', 'b', '0.1')

      socket4.write to_query('RPUSH', 'a', 'a1', 'a2', 'a3')

      response1 = read_response(socket1)
      assert_equal(expected_results[0], response1)

      response2 = read_response(socket2)
      assert_equal(expected_results[1], response2)

      response3 = read_response(socket3)
      assert_equal(expected_results[2], response3)
    end
  end

  def assert_buffer_commands_processing(blocking_command:, expected_response:)
    with_server do |socket1|
      socket2 = TCPSocket.new 'localhost', 2000

      socket1.write to_query(blocking_command, 'a', 'b', '1')
      socket1.write to_query('SET', 'a', '1')
      socket1.write to_query('GET', 'a')

      socket2.write to_query('RPUSH', 'a', 'a1', 'a2')

      # Leave enough time to the server to write back all three responses
      sleep 0.1
      response1 = socket1.read_nonblock(1024, exception: false)
      assert_equal(expected_response, response1)
    end
  end
end
