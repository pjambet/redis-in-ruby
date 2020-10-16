# coding: utf-8

require_relative './test_helper'

describe 'Set Commands' do
  describe 'SADD' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SADD', '-ERR wrong number of arguments for \'SADD\' command' ],
        [ 'SADD s', '-ERR wrong number of arguments for \'SADD\' command' ],
      ]
    end

    it 'fails if the key is not a hash' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SADD not-a-set 1 2', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'creates a set if necessary' do
      assert_command_results [
        [ 'SADD s 1', ':1' ],
        [ 'TYPE s', '+set' ],
      ]
    end

    it 'adds the given int member(s) to an existing set' do
      assert_command_results [
        [ 'SADD s 10', ':1' ],
        [ 'SADD s 5 1 100', ':3' ],
        [ 'SADD s 1024 256 5 1 65536', ':3' ],
      ]
    end

    it 'adds the given string member(s) to an existing set' do
      assert_command_results [
        [ 'SADD s m1 m2', ':2' ],
        [ 'SADD s m5 m3 m2 m1000 m100', ':4' ],
        [ 'SADD s m1000 m101', ':1' ],
      ]
    end

    it 'handles a mix of strings and int members' do
      assert_command_results [
        [ 'SADD s 20 10 30', ':3' ],
        [ 'SADD s m1 10 m2', ':2' ],
        [ 'SADD s m5 m3 m2 m1000 m100', ':4' ],
        [ 'SADD s m1000 15 m101', ':2' ],
      ]
    end
  end

  describe 'SCARD' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SCARD', '-ERR wrong number of arguments for \'SCARD\' command' ],
        [ 'SCARD s a', '-ERR wrong number of arguments for \'SCARD\' command' ],
      ]
    end

    it 'fails if the key is not a hash' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SCARD not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns 0 for a non existing set' do
      assert_command_results [
        [ 'SCARD s', ':0' ],
      ]
    end

    it 'returns the cardinality of the set' do
      assert_command_results [
        [ 'SADD s 1 2 3', ':3' ],
        [ 'SCARD s', ':3' ],
        [ 'SADD s 20 10 30 a b c', ':6' ],
        [ 'SCARD s', ':9' ],
        [ 'SADD s a b 10 20', ':0' ],
        [ 'SCARD s', ':9' ],
      ]
    end
  end

  describe 'SDIFF' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SDIFF', '-ERR wrong number of arguments for \'SDIFF\' command' ],
      ]
    end

    it 'fails if the key is not a hash' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SDIFF not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'SADD a-set 1 2', ':2' ],
        [ 'SDIFF a-set not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an empty array with non existing sets' do
      assert_command_results [
        [ 'SDIFF s', [] ],
        [ 'SDIFF s1 s2 s3', [] ],
      ]
    end

    it 'returns the entire set with no other arguments' do
      assert_command_results [
        [ 'SADD s 20 10 30', ':3' ],
        [ 'SDIFF s', [ '10', '20', '30' ] ],
      ]
    end

    it 'returns all the elements from the first set that are not in the other ones' do
      assert_command_results [
        [ 'SADD s1 a b c d', ':4' ],
        [ 'SADD s2 c', ':1' ],
        [ 'SADD s3 a c e', ':3' ],
        [ 'SDIFF s1 s2 s3', unordered([ 'b', 'd' ]) ],
      ]
    end
  end

  describe 'SDIFFSTORE' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SDIFFSTORE', '-ERR wrong number of arguments for \'SDIFFSTORE\' command' ],
        [ 'SDIFFSTORE dest', '-ERR wrong number of arguments for \'SDIFFSTORE\' command' ],
      ]
    end

    it 'returns an error if one of the inputs is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SADD a-set 1 2 3', ':3' ],
        [ 'SDIFFSTORE dest not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'SDIFFSTORE dest non-existing not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'SDIFFSTORE dest a-set not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'does nothing and return 0 with a non existing set' do
      assert_command_results [
        [ 'SDIFFSTORE dest s', ':0' ],
        [ 'TYPE dest', '+none' ],
      ]
    end

    it 'stores the same set in dest with a single set argument' do
      assert_command_results [
        [ 'SADD s1 20 10 30', ':3' ],
        [ 'SDIFFSTORE dest s1', ':3' ],
        [ 'SCARD dest', ':3' ],
        [ 'SMEMBERS dest', [ '10', '20', '30' ] ],
        [ 'SADD s2 20 b c a 10', ':5' ],
        [ 'SDIFFSTORE dest s2', ':5' ],
        [ 'SCARD dest', ':5' ],
        [ 'SMEMBERS dest', unordered([ '20', '10', 'b', 'c', 'a' ]) ],
      ]
    end

    it 'stores the diff in dest' do
      assert_command_results [
        [ 'SADD s1 20 10 30', ':3' ],
        [ 'SADD s2 40 30 50', ':3' ],
        [ 'SDIFFSTORE dest s1 s2', ':2' ],
        [ 'SMEMBERS dest', [ '10', '20' ] ],
        [ 'SADD s1 b c a', ':3' ],
        [ 'SADD s3 b a d', ':3' ],
        [ 'SDIFFSTORE dest s1 s2 s3', ':3' ],
        [ 'SMEMBERS dest', unordered([ '10', '20', 'c' ]) ],
      ]
    end

    it 'can use one of the input sets as the dest, and overwrites it'

    it 'overwrites destination if it already exists' do
      assert_command_results [
        [ 'SET dest a', '+OK' ],
        [ 'SADD s1 20 10 30', ':3' ],
        [ 'SDIFFSTORE dest s1', ':3' ],
        [ 'SMEMBERS dest', unordered([ '10', '20', '30' ]) ],
      ]
    end
  end

  describe 'SINTER' do
    it 'handles unexpected number of arguments'

    it 'returns an error if one of the inputs is not a set'
  end

  describe 'SINTERSTORE' do
    it 'handles unexpected number of arguments'

    it 'returns an error if one of the inputs is not a set'
  end

  describe 'SISMEMBER' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SISMEMBER', '-ERR wrong number of arguments for \'SISMEMBER\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SISMEMBER not-a-set f', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns 0 if the element is not a member of the set, 1 otherwise' do
      assert_command_results [
        [ 'SADD s 20 10 30', ':3' ],
        [ 'SISMEMBER s a', ':0' ],
        [ 'SISMEMBER s 20', ':1' ],
        [ 'SISMEMBER s 21', ':0' ],
        [ 'SADD s a d c', ':3' ],
        [ 'SISMEMBER s a', ':1' ],
        [ 'SISMEMBER s e', ':0' ],
      ]
    end

    it 'returns 0 if the set does not exist' do
      assert_command_results [
        [ 'SISMEMBER s a', ':0' ],
        [ 'SISMEMBER s 1', ':0' ],
      ]
    end
  end

  describe 'SMISMEMBER' do # New in 6.2.0
    it 'handles unexpected number of arguments'
  end

  describe 'SMEMBERS' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SMEMBERS', '-ERR wrong number of arguments for \'SMEMBERS\' command' ],
        [ 'SMEMBERS a b', '-ERR wrong number of arguments for \'SMEMBERS\' command' ],
      ]
    end

    it 'returns an error if one of the inputs is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SMEMBERS not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns all the elements in the set' do
      assert_command_results [
        [ 'SADD s 20 10 30', ':3' ],
        [ 'SMEMBERS s', [ '10', '20', '30' ] ],
        [ 'SADD s b c d a', ':4' ],
        [ 'SMEMBERS s', unordered([ '10', '20', '30', 'a', 'b', 'c', 'd' ]) ],
      ]
    end
  end

  describe 'SMOVE' do
    it 'handles unexpected number of arguments'

    it 'returns an error if the source is not a set'
  end

  describe 'SPOP' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SPOP', '-ERR wrong number of arguments for \'SPOP\' command' ],
        [ 'SPOP a 1 a', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SPOP not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an error if count is not an integer' do
      assert_command_results [
        [ 'SPOP s a', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns an error if count is negative' do
      assert_command_results [
        [ 'SPOP s -1', '-ERR index out of range' ],
      ]
    end

    it 'removes an element from the set and returns it' do
      assert_command_results [
        [ 'SADD s 20 10 30', ':3' ],
        [ 'SPOP s', one_of([ '10', '20', '30' ]) ],
        [ 'SADD s b a c', ':3' ],
        [ 'SPOP s', one_of([ '10', '20', '30', 'a', 'b', 'c' ]) ],
        [ 'SCARD s', ':4' ],
      ]
    end

    it 'returns up to count elements with the count argument'

    it 'returns a nil string for a non existing set' do
      assert_command_results [
        [ 'SPOP s', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns an empty array for a non existing set with a count argument' do
      assert_command_results [
        [ 'SPOP s 1', BYORedis::EMPTY_ARRAY ],
      ]
    end

    it 'returns an empty array for an existing set with a 0 count' do
      assert_command_results [
        [ 'SADD s a', ':1' ],
        [ 'SPOP s 0', BYORedis::EMPTY_ARRAY ],
      ]
    end
  end

  describe 'SRANDMEMBER' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SRANDMEMBER', '-ERR wrong number of arguments for \'SRANDMEMBER\' command' ],
        [ 'SRANDMEMBER a 1 a', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SRANDMEMBER not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an error if count is not an integer' do
      assert_command_results [
        [ 'SRANDMEMBER s a', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns an empty array with a negative count for a non existing set' do
      assert_command_results [
        [ 'SRANDMEMBER s -10', BYORedis::EMPTY_ARRAY ],
      ]
    end

    it 'returns up to count elements, allowing duplicates if count is negative' do
      with_server do |socket|
        socket.write(to_query('SADD', 's', '1', '2', '3'))
        response = read_response(socket)
        assert_equal(":3\r\n", response)

        # With less than the total of elements, and only ints
        socket.write(to_query('SRANDMEMBER', 's', '-2'))
        response = read_response(socket)

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*2', length)
        assert_equal(4, parts.length)
        parts.each_slice(2) do |part|
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ] ], part)
        end

        # With more than the total of elements, and only ints
        socket.write(to_query('SRANDMEMBER', 's', '-10'))
        response = read_response(socket)

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*10', length)
        assert_equal(20, parts.length)
        parts.each_slice(2) do |part|
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ] ], part)
        end

        socket.write(to_query('SADD', 's', 'b', 'c', 'a'))
        response = read_response(socket)
        assert_equal(":3\r\n", response)

        # With less than the total of elements and a mix of ints and strings
        socket.write(to_query('SRANDMEMBER', 's', '-2'))
        response = read_response(socket)

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*2', length)
        assert_equal(4, parts.length)
        parts.each_slice(2) do |part|
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ], [ '$1', 'a' ],
                            [ '$1', 'b' ], [ '$1', 'c' ] ], part)
        end

        # With more than the total of elements and a mix of ints and strings
        socket.write(to_query('SRANDMEMBER', 's', '-20'))
        response = read_response(socket)

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*20', length)
        assert_equal(40, parts.length)
        parts.each_slice(2) do |part|
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ], [ '$1', 'a' ],
                            [ '$1', 'b' ], [ '$1', 'c' ] ], part)
        end
      end
    end

    it 'returns an element from the set but does not remove it' do
      assert_command_results [
        [ 'SADD s 20 10 30', ':3' ],
        [ 'SRANDMEMBER s', one_of([ '10', '20', '30' ]) ],
        [ 'SMEMBERS s', unordered([ '10', '20', '30']) ],
        [ 'SADD s b a c', ':3' ],
        [ 'SRANDMEMBER s', one_of([ '10', '20', '30', 'a', 'b', 'c' ]) ],
        [ 'SMEMBERS s', unordered([ '10', '20', '30', 'a', 'b', 'c' ]) ],
        [ 'SCARD s', ':6' ],
      ]
    end

    it 'returns the whole set if count is positive and greater than the set\'s cardinality' do
      assert_command_results [
        [ 'SADD s 20 10 30', ':3' ],
        [ 'SRANDMEMBER s 3', unordered([ '10', '20', '30' ]) ],
        [ 'SRANDMEMBER s 4', unordered([ '10', '20', '30' ]) ],
        [ 'SADD s b c a', ':3' ],
        [ 'SRANDMEMBER s 6', unordered([ '10', '20', '30', 'a', 'b', 'c' ]) ],
        [ 'SRANDMEMBER s 7', unordered([ '10', '20', '30', 'a', 'b', 'c' ]) ],
      ]
    end

    it 'returns up to count elements with the count argument' do
      with_server do |socket|
        socket.write(to_query('SADD', 's', '1', '2', '3'))
        response = read_response(socket)
        assert_equal(":3\r\n", response)

        socket.write(to_query('SRANDMEMBER', 's', '1'))
        response = read_response(socket)

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*1', length)
        assert_equal(2, parts.length)
        parts.each_slice(2) do |part|
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ] ], part)
        end

        socket.write(to_query('SRANDMEMBER', 's', '2'))
        response = read_response(socket)

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*2', length)
        assert_equal(4, parts.length)
        parts.each_slice(2) do |part|
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ] ], part)
        end

        socket.write(to_query('SADD', 's', 'b', 'c', 'a'))
        response = read_response(socket)
        assert_equal(":3\r\n", response)
        socket.write(to_query('SRANDMEMBER', 's', '2'))
        response = read_response(socket)

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*2', length)
        assert_equal(4, parts.length)
        parts.each_slice(2) do |part|
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ], [ '$1', 'a' ],
                            [ '$1', 'b' ], [ '$1', 'c' ] ], part)
        end
      end
    end

    it 'returns a nil string for a non existing set' do
      assert_command_results [
        [ 'SRANDMEMBER s', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns an empty array for a non existing set with a count argument' do
      assert_command_results [
        [ 'SRANDMEMBER s 1', BYORedis::EMPTY_ARRAY ],
      ]
    end

    it 'returns an empty array for an existing set with a 0 count' do
      assert_command_results [
        [ 'SADD s a', ':1' ],
        [ 'SRANDMEMBER s 0', BYORedis::EMPTY_ARRAY ],
      ]
    end
  end

  describe 'SREM' do
    it 'handles unexpected number of arguments'

    it 'returns an error if the key is not a set'
  end

  describe 'SUNION' do
    it 'handles unexpected number of arguments'

    it 'returns an error if one of the inputs is not a set'
  end

  describe 'SUNIONSTORE' do
    it 'handles unexpected number of arguments'

    it 'returns an error if one of the inputs is not a set'
  end
end
