# coding: utf-8

require_relative './test_helper'

describe 'BYORedis - Hash commands' do
  describe 'HSET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HSET h f1', '-ERR wrong number of arguments for \'HSET\' command' ],
        [ 'HSET h f1 v1 f2', '-ERR wrong number of arguments for \'HSET\' command' ],
        [ 'HSET h f1 v1 f2 v2 f3', '-ERR wrong number of arguments for \'HSET\' command' ],
      ]
    end

    it 'returns the number of fields that were added'

    # ...
  end

  describe 'HGETALL' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HGETALL', '-ERR wrong number of arguments for \'HGETALL\' command' ],
        [ 'HGETALL h a', '-ERR wrong number of arguments for \'HGETALL\' command' ],
      ]
    end

    it 'returns an empty array if the hash does not exist'

    it 'returns all the field/value pairs in a flat array'
  end

  describe 'HGET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HGET h', '-ERR wrong number of arguments for \'HGET\' command' ],
        [ 'HGET h f a', '-ERR wrong number of arguments for \'HGET\' command' ],
      ]
    end

    it 'returns a nil string if the hash does not exist'

    it 'returns a nil string if the field does not exist in the hash'

    it 'returns the value for the given hash/field'
  end

  describe 'HDEL' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HDEL h', '-ERR wrong number of arguments for \'HDEL\' command' ],
      ]
    end

    it 'returns the number of fields that were deleted from the hash'

    it 'removes the hash from the keyspace if it is empty'

    it 'returns 0 if the hash does not exist'
  end

  describe 'HEXISTS' do
    it 'rejects an invalid number of arguments'

    it 'returns 0 if the hash does not exist'

    it 'returns 0 if the field does not exist in the hash'

    it 'returns 1 if the field exists in the hash'
  end

  describe 'HINCRBY' do
    it 'rejects an invalid number of arguments'

    it 'returns the new value, as a RESP integer of the value for the field, after the incr'

    it 'returns an error if the value for the field is not an integer' # ERR hash value is not an integer, test with a float too

    it 'creates a new field/value pair with a value of 0 if the field does not exist'

    it 'creates a new hash and a new field/value pair with a value of 0 if the hash does not exist'
  end

  describe 'HINCRBYFLOAT' do
    it 'rejects an invalid number of arguments'

    it 'returns the new value, as a RESP integer of the value for the field, after the incr'

    it 'returns an error if the value for the field is not a number' # ERR hash value is not a float

    it 'creates a new field/value pair with a value of 0 if the field does not exist'

    it 'creates a new hash and a new field/value pair with a value of 0 if the hash does not exist'
  end

  describe 'KEYS' do
    it 'rejects an invalid number of arguments'

    it 'returns a nil array if the hash does not exist'

    it 'returns an array of all the fields in the hash'
  end

  describe 'HLEN' do
    it 'rejects an invalid number of arguments'

    it 'returns 0 if the hash does not exist'

    it 'returns the number of field/value pairs in the hash'
  end

  describe 'HMGET' do
    it 'rejects an invalid number of arguments'

    it 'returns a nil array if the hash does not exist'

    it 'returns an array of all the values for the given fields in the hash'

    it 'returns an array including nil values for non existing fields'
  end

  describe 'HSETNX' do
    it 'rejects an invalid number of arguments'

    it 'does nothing if the field already exists'

    it 'returns 1 if the hash does not exist'

    it 'returns 1 if the field does not exist'

    it 'returns 0 if the field already exists'
  end

  describe 'HSTRLEN' do
    it 'rejects an invalid number of arguments'

    it 'returns 0 if the field does not exist'
    it 'returns 0 if the hash does not exist'
    it 'returns the length of the string stored for the given field'
  end

  describe 'HVALS' do
    it 'rejects an invalid number of arguments'
    it 'returns an empty array if the hash does not exist'
    it 'returns an array of all the values in the hash'
  end
end
