# coding: utf-8

require_relative './test_helper'

describe 'BYORedis - Hash commands' do
  describe 'HSET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HSET', '-ERR wrong number of arguments for \'HSET\' command' ],
        [ 'HSET h', '-ERR wrong number of arguments for \'HSET\' command' ],
        [ 'HSET h f1', '-ERR wrong number of arguments for \'HSET\' command' ],
        [ 'HSET h f1 v1 f2', '-ERR wrong number of arguments for \'HSET\' command' ],
        [ 'HSET h f1 v1 f2 v2 f3', '-ERR wrong number of arguments for \'HSET\' command' ],
      ]
    end

    it 'creates a hash if necessary' do
      assert_command_results [
        [ 'TYPE h', '+none' ],
        [ 'HSET h f1 v1', ':1' ],
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
        [ 'HGET', '-ERR wrong number of arguments for \'HGET\' command' ],
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
        [ 'HSET h f1 v1', ':1' ],
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
        [ 'HDEL', '-ERR wrong number of arguments for \'HDEL\' command' ],
        [ 'HDEL h', '-ERR wrong number of arguments for \'HDEL\' command' ],
      ]
    end

    it 'returns the number of fields that were deleted from the hash' do
      hdel_tests_as_list
      hdel_tests_as_dict
    end

    it 'removes the hash from the keyspace if it is empty' do
      hdel_tests_keyspace_clear_as_list
      hdel_tests_keyspace_clear_as_dict
    end

    it 'returns 0 if the hash does not exist' do
      assert_command_results [
        [ 'HDEL h f1', ':0' ],
      ]
    end
  end

  describe 'HEXISTS' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HEXISTS', '-ERR wrong number of arguments for \'HEXISTS\' command' ],
        [ 'HEXISTS h', '-ERR wrong number of arguments for \'HEXISTS\' command' ],
        [ 'HEXISTS h a b', '-ERR wrong number of arguments for \'HEXISTS\' command' ],
      ]
    end

    it 'returns 0 if the hash does not exist' do
      assert_command_results [
        [ 'HEXISTS h f1', ':0' ],
      ]
    end

    it 'returns 0 if the field does not exist in the hash' do
      assert_command_results [
        [ 'HSET h f1 v1 f2 v2', ':2' ],
        [ 'HEXISTS h not-a-field', ':0' ],
      ]
    end

    it 'returns 1 if the field exists in the hash' do
      assert_command_results [
        [ 'HSET h f1 v1 f2 v2', ':2' ],
        [ 'HEXISTS h f2', ':1' ],
      ]
    end
  end

  describe 'HINCRBY' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HINCRBY', '-ERR wrong number of arguments for \'HINCRBY\' command' ],
        [ 'HINCRBY h', '-ERR wrong number of arguments for \'HINCRBY\' command' ],
        [ 'HINCRBY h a', '-ERR wrong number of arguments for \'HINCRBY\' command' ],
        [ 'HINCRBY h a b c', '-ERR wrong number of arguments for \'HINCRBY\' command' ],
      ]
    end

    it 'returns the new value, as a RESP integer of the value for the field, after the incr' do
      assert_command_results [
        [ 'HSET h f1 1 f2 2', ':2' ],
        [ 'HINCRBY h f1 99', ':100' ],
        [ 'HINCRBY h f2 99', ':101' ],
      ]
    end

    it 'returns an error if the value for the field is not an integer' do
      assert_command_results [
        [ 'HSET h f1 not-an-int a-float 1.0', ':2' ],
        [ 'HINCRBY h f1 1', '-ERR hash value is not an integer' ],
        [ 'HINCRBY h a-float 1', '-ERR hash value is not an integer' ],
      ]
    end

    it 'returns an error if the increment is not an integer' do
      assert_command_results [
        [ 'HINCRBY h f2 not-an-int', '-ERR value is not an integer or out of range' ],
        [ 'HINCRBY h f2 1.0', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'creates a new field/value pair with a value of 0 if the field does not exist' do
      assert_command_results [
        [ 'HSET h f1 v1', ':1' ],
        [ 'HINCRBY h f2 10', ':10' ],
      ]
    end

    it 'creates a new hash and a new field/value pair with a value of 0 if the hash does not exist' do
      assert_command_results [
        [ 'HINCRBY h f1 1', ':1' ],
        [ 'DEL h', ':1' ],
        [ 'HINCRBY h f1 100', ':100' ],
      ]
    end

    it 'handles a min overflow' do
      assert_command_results [
        [ 'HSET h close-to-min -9223372036854775807', ':1' ],
        [ 'HINCRBY h close-to-min -2', '-ERR increment or decrement would overflow' ],
      ]
    end

    it 'handles a max overflow' do
      assert_command_results [
        [ 'HSET h close-to-max 9223372036854775806', ':1' ],
        [ 'HINCRBY h close-to-max 2', '-ERR increment or decrement would overflow' ],
      ]
    end
  end

  describe 'HINCRBYFLOAT' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HINCRBYFLOAT', '-ERR wrong number of arguments for \'HINCRBYFLOAT\' command' ],
        [ 'HINCRBYFLOAT h', '-ERR wrong number of arguments for \'HINCRBYFLOAT\' command' ],
        [ 'HINCRBYFLOAT h a', '-ERR wrong number of arguments for \'HINCRBYFLOAT\' command' ],
        [ 'HINCRBYFLOAT h a b c', '-ERR wrong number of arguments for \'HINCRBYFLOAT\' command' ],
      ]
    end

    it 'returns the new value, as a RESP string of the value for the field, after the incr' do
      assert_command_results [
        [ 'HSET h a 1.0', ':1' ],
        [ 'HINCRBYFLOAT h a 0.34', '1.34' ],
      ]
    end

    it 'returns an error if the value for the field is not a number' do
      assert_command_results [
        [ 'HSET h f1 not-an-int a-float 1.0', ':2' ],
        [ 'HINCRBYFLOAT h f1 1', '-ERR hash value is not a float' ],
      ]
    end

    it 'returns an error if the increment is not a number (float or int)' do
      assert_command_results [
        [ 'HINCRBYFLOAT h f1 a', '-ERR value is not a valid float' ],
      ]
    end

    it 'creates a new field/value pair with a value of 0 if the field does not exist' do
      assert_command_results [
        [ 'HSET h f2 v2', ':1' ],
        [ 'HINCRBYFLOAT h f1 1.2', '1.2' ],
      ]
    end

    it 'creates a new hash and a new field/value pair with a value of 0 if the hash does not exist' do
      assert_command_results [
        [ 'HINCRBYFLOAT h f1 1.2', '1.2' ],
      ]
    end
  end

  describe 'HKEYS' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HKEYS', '-ERR wrong number of arguments for \'HKEYS\' command' ],
        [ 'HKEYS h f1', '-ERR wrong number of arguments for \'HKEYS\' command' ],
      ]
    end

    it 'returns a nil array if the hash does not exist' do
      assert_command_results [
        [ 'HKEYS h', BYORedis::NULL_ARRAY ],
      ]
    end

    it 'returns an array of all the fields in the hash' do
      hkeys_tests_as_list
      hkeys_tests_as_dict
    end
  end

  describe 'HLEN' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HLEN', '-ERR wrong number of arguments for \'HLEN\' command' ],
        [ 'HLEN h f1', '-ERR wrong number of arguments for \'HLEN\' command' ],
      ]
    end

    it 'returns 0 if the hash does not exist' do
      assert_command_results [
        [ 'HLEN h', ':0' ],
      ]
    end

    it 'returns the number of field/value pairs in the hash' do
      hlen_tests_as_list
      hlen_tests_as_dict
    end
  end

  describe 'HMGET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HMGET', '-ERR wrong number of arguments for \'HMGET\' command' ],
        [ 'HMGET h', '-ERR wrong number of arguments for \'HMGET\' command' ],
      ]
    end

    it 'returns an array with nil values if the hash does not exist' do
      assert_command_results [
        [ 'HMGET h a b c', [ nil, nil, nil ] ],
      ]
    end

    it 'returns an array of all the values for the given fields in the hash' do
      assert_command_results [
        [ 'HSET h f1 v1 f2 v2 f3 v3', ':3' ],
        [ 'HMGET h f1 f3', [ 'v1', 'v3' ] ],
      ]
    end

    it 'returns an array including nil values for non existing fields' do
      assert_command_results [
        [ 'HSET h f1 v1 f2 v2 f3 v3', ':3' ],
        [ 'HMGET h f1 not-a-thing f3 neither-is-this', [ 'v1', nil, 'v3', nil ] ],
      ]
    end
  end

  describe 'HSETNX' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HSETNX', '-ERR wrong number of arguments for \'HSETNX\' command' ],
        [ 'HSETNX h', '-ERR wrong number of arguments for \'HSETNX\' command' ],
        [ 'HSETNX h f', '-ERR wrong number of arguments for \'HSETNX\' command' ],
        [ 'HSETNX h f v a', '-ERR wrong number of arguments for \'HSETNX\' command' ],
      ]
    end

    it 'does nothing if the field already exists' do
      assert_command_results [
        [ 'HSET h f1 v1 f2 v2 f3 v3', ':3' ],
        [ 'HSETNX h f1 new-value', ':0' ],
        [ 'HGET h f1', 'v1' ],
      ]
    end

    it 'returns 1 if the hash does not exist' do
      assert_command_results [
        [ 'HSETNX h f1 new-value', ':1' ],
        [ 'HGET h f1', 'new-value' ],
      ]
    end

    it 'returns 1 if the field does not exist' do
      assert_command_results [
        [ 'HSET h f1 v1 f2 v2 f3 v3', ':3' ],
        [ 'HSETNX h new-field new-value', ':1' ],
        [ 'HGET h new-field', 'new-value' ],
      ]
    end
  end

  describe 'HSTRLEN' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HSTRLEN', '-ERR wrong number of arguments for \'HSTRLEN\' command' ],
        [ 'HSTRLEN h', '-ERR wrong number of arguments for \'HSTRLEN\' command' ],
        [ 'HSTRLEN h a b', '-ERR wrong number of arguments for \'HSTRLEN\' command' ],
      ]
    end

    it 'returns 0 if the field does not exist' do
      assert_command_results [
        [ 'HSET h f1 v1 f2 v2 f3 v3', ':3' ],
        [ 'HSTRLEN h not-a-thing', ':0' ],
      ]
    end

    it 'returns 0 if the hash does not exist' do
      assert_command_results [
        [ 'HSTRLEN h not-a-thing', ':0' ],
      ]
    end

    it 'returns the length of the string stored for the given field' do
      assert_command_results [
        [ 'HSET h f1 v1 f2 v2 f4 v3 a-long-string aaaaaaaaaa an-emoji 👋 another-one 🤷‍♂️', ':6' ],
        [ 'HSTRLEN h f1', ':2' ],
        [ 'HSTRLEN h a-long-string', ':10' ],
        [ 'HSTRLEN h an-emoji', ':4' ],
        [ 'HSTRLEN h another-one', ':13' ],
      ]
    end
  end

  describe 'HVALS' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'HVALS', '-ERR wrong number of arguments for \'HVALS\' command' ],
        [ 'HVALS a b', '-ERR wrong number of arguments for \'HVALS\' command' ],
      ]
    end

    it 'returns an empty array if the hash does not exist' do
      assert_command_results [
        [ 'HVALS h', [] ],
      ]
    end

    it 'returns an array of all the values in the hash' do
      hvals_tests_as_list
      hvals_tests_as_dict
    end
  end

  def hset_tests_as_dict
    hset_tests_as_list(max_list_size: 1)
  end

  def hset_tests_as_list(max_list_size: 256)
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = max_list_size.to_s
    assert_command_results [
      [ 'HSET h f1 v1 f2 v2', ':2' ],
      [ 'HSET h f2 k2', ':0' ],
      [ 'HSET h f2 k2-a', ':0' ],
      [ 'HSET h f2 k2-b f3 k3', ':1' ],
    ]
  end

  def hgetall_tests_as_dict
    hgetall_tests_as_list(max_list_size: 1)
  end

  def hgetall_tests_as_list(max_list_size: 256)
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = max_list_size.to_s
    assert_unordered_array_match(
      hset_command: 'HSET h f1 v1 f2 v2',
      main_command: 'HGETALL h',
      expected_result: [ [ '$2', 'f1' ], [ '$2', 'f2' ], [ '$2', 'v1' ], [ '$2', 'v2' ] ])
  end

  def hget_tests_as_list(max_list_size: 256)
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = max_list_size.to_s
    assert_command_results [
      [ 'HSET h f1 v1 f2 v2', ':2' ],
      [ 'HGET h f1', 'v1' ],
      [ 'HGET h f2', 'v2' ],
      [ 'HGET h f3', BYORedis::NULL_BULK_STRING ],
    ]
  end

  def hget_tests_as_dict
    hget_tests_as_list(max_list_size: 1)
  end

  def hdel_tests_as_list(max_list_size: 256)
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = max_list_size.to_s
    assert_command_results [
      [ 'HSET h f1 v1 f2 v2 f3 v3', ':3' ],
      [ 'HDEL h f1 not-a-field f2', ':2' ],
    ]
  end

  def hdel_tests_as_dict
    hdel_tests_as_list(max_list_size: 1)
  end

  def hdel_tests_keyspace_clear_as_list(max_list_size: 256)
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = max_list_size.to_s
    assert_command_results [
      [ 'HSET h f1 v1 f2 v2', ':2' ],
      [ 'HDEL h f1 f2', ':2' ],
      [ 'TYPE h', '+none' ],
    ]
  end

  def hdel_tests_keyspace_clear_as_dict
    hdel_tests_keyspace_clear_as_list(max_list_size: 1)
  end

  def hkeys_tests_as_list(max_list_size: 256)
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = max_list_size.to_s
    assert_unordered_array_match(hset_command: 'HSET h f1 v1 f2 v2',
                                 main_command: 'HKEYS h',
                                 expected_result: [ [ '$2', 'f1' ], [ '$2', 'f2' ] ])
  end

  def hkeys_tests_as_dict
    hkeys_tests_as_list(max_list_size: 1)
  end

  def hvals_tests_as_list(max_list_size: 256)
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = max_list_size.to_s
    assert_unordered_array_match(hset_command: 'HSET h f1 v1 f2 v2',
                                 main_command: 'HVALS h',
                                 expected_result: [ [ '$2', 'v1' ], [ '$2', 'v2' ] ])
  end

  def hvals_tests_as_dict
    hvals_tests_as_list(max_list_size: 1)
  end

  def hlen_tests_as_list(max_list_size: 256)
    ENV['HASH_MAX_ZIPLIST_ENTRIES'] = max_list_size.to_s
    assert_command_results [
      [ 'HSET h f1 v1 f2 v2', ':2' ],
      [ 'HLEN h', ':2' ],
    ]
  end

  def hlen_tests_as_dict
    hlen_tests_as_list(max_list_size: 1)
  end

  def assert_unordered_array_match(hset_command:, main_command:, expected_result:)
    with_server do |socket|
      socket.write to_query(*hset_command.split)
      IO.select([ socket ], [], [], 0.1)
      assert_response(':2', read_response(socket))

      socket.write to_query(*main_command.split)
      IO.select([ socket ], [], [], 0.1)
      response = read_response(socket)

      # The hash representation does not maintain ordering, so we need to sort the elements to
      # perform a deterministic comparison
      resp_array_elements = response.split.then do |r|
        r.shift # Remove the number of element *4 in this example
        r.each_slice(2).sort
      end

      assert_equal(expected_result, resp_array_elements)
    end
  end
end
