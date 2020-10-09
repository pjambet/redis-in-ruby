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

    it 'creates a hash if necessary' do
      assert_command_results [
        [ 'TYPE h', '+none' ],
        [ 'HSET h f1 k1', ':1' ],
        [ 'TYPE h', '+hash' ],
      ]
    end

    it 'returns the number of fields that were added' do
      hset_tests_as_list
      hset_tests_as_dict
    end
  end

  describe 'HGETALL' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HGETALL', '-ERR wrong number of arguments for \'HGETALL\' command' ],
        [ 'HGETALL h a', '-ERR wrong number of arguments for \'HGETALL\' command' ],
      ]
    end

    it 'returns an empty array if the hash does not exist' do
      assert_command_results [
        [ 'HGETALL h', '*0' ],
      ]
    end

    it 'returns all the field/value pairs in a flat array' do
      hgetall_tests_as_list
      hgetall_tests_as_dict
    end
  end

  describe 'HGET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HGET h', '-ERR wrong number of arguments for \'HGET\' command' ],
        [ 'HGET h f a', '-ERR wrong number of arguments for \'HGET\' command' ],
      ]
    end

    it 'returns a nil string if the hash does not exist' do
      assert_command_results [
        [ 'HGET h f1', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns a nil string if the field does not exist in the hash' do
      assert_command_results [
        [ 'HSET h f1 k1', ':1' ],
        [ 'HGET h f2', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns the value for the given hash/field' do
      hget_tests_as_list
      hget_tests_as_dict
    end
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

    it 'returns an error if the increment is not an integer'

    it 'creates a new field/value pair with a value of 0 if the field does not exist'

    it 'creates a new hash and a new field/value pair with a value of 0 if the hash does not exist'
  end

  describe 'HINCRBYFLOAT' do
    it 'rejects an invalid number of arguments'

    it 'returns the new value, as a RESP integer of the value for the field, after the incr'

    it 'returns an error if the value for the field is not a number' # ERR hash value is not a float
    it 'returns an error if the increment is not a number (float or int)'

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

  def hset_tests_as_dict
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = '1'
    hset_tests_as_list
  end

  def hset_tests_as_list
    assert_command_results [
      [ 'HSET h f1 k1 f2 k2', ':2' ],
      [ 'HSET h f2 k2', ':0' ],
      [ 'HSET h f2 k2-a', ':0' ],
      [ 'HSET h f2 k2-b f3 k3', ':1' ],
    ]
  end

  def hgetall_tests_as_dict
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = '1'
    hgetall_tests_as_list
  end

  def hgetall_tests_as_list
    with_server do |socket|
      socket.write to_query('HSET', 'h', 'f1', 'k1', 'f2', 'k2')
      IO.select([ socket ], [], [], 0.1)
      assert_response(':2', read_response(socket))

      socket.write to_query('HGETALL', 'h')
      IO.select([ socket ], [], [], 0.1)
      response = read_response(socket)

      # The hash representation does not maintain ordering, so we need to sort the elements to
      # perform a deterministic comparison
      resp_array_elements = response.split.then do |r|
        r.shift # Remove the number of element *4 in this example
        r.each_slice(2).sort
      end

      assert_equal([ [ '$2', 'f1' ], [ '$2', 'f2' ], [ '$2', 'k1' ], [ '$2', 'k2' ] ],
                   resp_array_elements)
    end
  end

  def hget_tests_as_list
    assert_command_results [
      [ 'HSET h f1 k1 f2 k2', ':2' ],
      [ 'HGET h f1', 'k1' ],
      [ 'HGET h f2', 'k2' ],
      [ 'HGET h f3', BYORedis::NULL_BULK_STRING ],
    ]
  end

  def hget_tests_as_dict
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = '1'
    hget_tests_as_list
  end
end
