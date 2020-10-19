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

    it 'can use one of the input sets as the dest, and overwrites it' do
      assert_command_results [
        [ 'SADD s1 20 10 30', ':3' ],
        [ 'SADD s2 10 30 40', ':3' ],
        [ 'SDIFFSTORE s1 s1 s2', ':1' ],
        [ 'SMEMBERS s1', [ '20' ] ],
      ]
    end

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
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SINTER', '-ERR wrong number of arguments for \'SINTER\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SINTER not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an empty array if one the keys does not exist' do
      assert_command_results [
        [ 'SINTER s', BYORedis::EMPTY_ARRAY ],
      ]
    end

    it 'returns the set itself if not other sets are given' do
      assert_command_results [
        [ 'SADD s 1 2 3', ':3' ],
        [ 'SINTER s', [ '1', '2', '3' ] ],
        [ 'SADD s b a', ':2' ],
        [ 'SINTER s', unordered([ 'a', 'b', '1', '2', '3' ]) ],
      ]
    end

    it 'returns the intersection of all the sets' do
      assert_command_results [
        [ 'SADD s1 1 2 3 4', ':4' ],
        [ 'SADD s2 3', ':1' ],
        [ 'SADD s3 1 3 5', ':3' ],
        [ 'SINTER s1 s2 s3', unordered([ '3' ]) ],
        [ 'DEL s1 s2 s3', ':3' ],
        [ 'SADD s1 a b c d', ':4' ],
        [ 'SADD s2 c', ':1' ],
        [ 'SADD s3 a c e', ':3' ],
        [ 'SINTER s1 s2 s3', unordered([ 'c' ]) ],
      ]
    end
  end

  describe 'SINTERSTORE' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SINTERSTORE', '-ERR wrong number of arguments for \'SINTERSTORE\' command' ],
        [ 'SINTERSTORE dest', '-ERR wrong number of arguments for \'SINTERSTORE\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SINTERSTORE dest not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns 0 and delete dest if one the keys does not exist' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SINTERSTORE not-a-set s', ':0' ],
        [ 'TYPE not-a-set', '+none' ],
      ]
    end

    it 'store the the set itself in dest if no other sets are given' do
      assert_command_results [
        [ 'SADD s 1 2 3', ':3' ],
        [ 'SINTERSTORE dest s', ':3' ],
        [ 'SMEMBERS dest', unordered([ '1', '2', '3' ]) ],
        [ 'SADD s b a', ':2' ],
        [ 'SINTERSTORE dest s', ':5' ],
        [ 'SMEMBERS dest', unordered([ '1', '2', '3', 'a', 'b' ]) ],
      ]
    end

    it 'can use one of the input sets as the dest, and overwrites it' do
      assert_command_results [
        [ 'SADD s1 20 10 30', ':3' ],
        [ 'SADD s2 10 30 40', ':3' ],
        [ 'SINTERSTORE s1 s1 s2', ':2' ],
        [ 'SMEMBERS s1', unordered([ '10', '30' ]) ],
      ]
    end

    it 'stores the intersection of all the sets in dest' do
      assert_command_results [
        [ 'SADD s1 1 2 3 4', ':4' ],
        [ 'SADD s2 3', ':1' ],
        [ 'SADD s3 1 3 5', ':3' ],
        [ 'SINTERSTORE dest s1 s2 s3', ':1' ],
        [ 'SMEMBERS dest', [ '3' ] ],
        [ 'DEL s1 s2 s3', ':3' ],
        [ 'SADD s1 a b c d', ':4' ],
        [ 'SADD s2 c', ':1' ],
        [ 'SADD s3 a c e', ':3' ],
        [ 'SINTERSTORE dest s1 s2 s3', ':1' ],
        [ 'SMEMBERS dest', [ 'c' ] ],
      ]
    end
  end

  describe 'SISMEMBER' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SISMEMBER', '-ERR wrong number of arguments for \'SISMEMBER\' command' ],
        [ 'SISMEMBER s', '-ERR wrong number of arguments for \'SISMEMBER\' command' ],
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

  describe 'SMISMEMBER' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SMISMEMBER', '-ERR wrong number of arguments for \'SMISMEMBER\' command' ],
        [ 'SMISMEMBER s', '-ERR wrong number of arguments for \'SMISMEMBER\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SMISMEMBER not-a-set f', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an array of 0s and 1s depending on whether the members are in the set or not' do
      assert_command_results [
        [ 'SADD s 20 10 30', ':3' ],
        [ 'SMISMEMBER s a', [ 0 ] ],
        [ 'SMISMEMBER s 20 a 21', [ 1, 0, 0 ] ],
        [ 'SADD s a d c', ':3' ],
        [ 'SMISMEMBER s a', [ 1 ] ],
        [ 'SMISMEMBER s d e f', [ 1, 0, 0 ] ],
      ]
    end

    it 'returns an array of 0s if the set does not exist' do
      assert_command_results [
        [ 'SMISMEMBER s a b c', [ 0, 0, 0 ] ],
        [ 'SMISMEMBER s 1 2 3', [ 0, 0, 0 ] ],
      ]
    end
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
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SMOVE', '-ERR wrong number of arguments for \'SMOVE\' command' ],
        [ 'SMOVE src', '-ERR wrong number of arguments for \'SMOVE\' command' ],
        [ 'SMOVE src dest', '-ERR wrong number of arguments for \'SMOVE\' command' ],
        [ 'SMOVE src dest member a', '-ERR wrong number of arguments for \'SMOVE\' command' ],
      ]
    end

    it 'returns an error if the source is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SMOVE not-a-set dest a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an error if the destination is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SMOVE src not-a-set a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns 0 if src is empty' do
      assert_command_results [
        [ 'SMOVE src dest a', ':0' ],
      ]
    end

    it 'returns 1 if the member was moved' do
      assert_command_results [
        [ 'SADD s 1 2 3', ':3' ],
        [ 'SMOVE s dest 4', ':0' ],
        [ 'SMOVE s dest 2', ':1' ],
        [ 'SMEMBERS s', unordered([ '1', '3' ]) ],
        [ 'SMEMBERS dest', unordered([ '2' ]) ],
        [ 'SADD s 2', ':1' ],
        [ 'SMOVE s dest 2', ':1' ], # Still returns 1 even if it already exists in dest
        [ 'SMEMBERS s', unordered([ '1', '3' ]) ],
        [ 'SMEMBERS dest', unordered([ '2' ]) ],
        [ 'DEL s dest', ':2' ],
        [ 'SADD s a b c', ':3' ],
        [ 'SMOVE s dest e', ':0' ],
        [ 'SMOVE s dest b', ':1' ],
        [ 'SMEMBERS s', unordered([ 'a', 'c' ]) ],
        [ 'SMEMBERS dest', unordered([ 'b' ]) ],
        [ 'SADD s b', ':1' ],
        [ 'SMOVE s dest b', ':1' ], # Still returns 1 even if it already exists in dest
        [ 'SMEMBERS s', unordered([ 'a', 'c' ]) ],
        [ 'SMEMBERS dest', unordered([ 'b' ]) ],
      ]
    end
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

    def tests_for_spop(socket, set_members, count)
      socket.write(to_query(*[ 'SADD', 's' ] + set_members))
      response = read_response(socket)
      assert_equal(":#{ set_members.size }\r\n", response)

      socket.write(to_query('SPOP', 's', count.to_s))
      response = read_response(socket)
      response_parts = response.split("\r\n")
      length = response_parts.shift
      assert_equal("*#{ count }", length)
      response_parts = response_parts.each_slice(2).to_a.sort
      assert_equal(response_parts, response_parts.uniq) # No duplicates
      response_parts.each do |part|
        # part[0] is the length of the RESP string such as $1, part[1] is the string
        assert(set_members.include?(part[1]))
      end
    end

    it 'returns up to count elements with the count argument' do
      with_server do |socket|
        tests_for_spop(socket, [ '1', '2', '3', '4' ], 2) # Case 2 for an intset
        socket.write(to_query('DEL', 's'))
        read_response(socket)
        tests_for_spop(socket, [ 'a', 'b', 'c', 'd' ], 2) # Case 2 for a hash table
        socket.write(to_query('DEL', 's'))
        read_response(socket)
        tests_for_spop(socket, [ '1', '2', '3', '4', '5', '6' ], 5) # Case 3 for an intset
        socket.write(to_query('DEL', 's'))
        read_response(socket)
        tests_for_spop(socket, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 5) # Case 3 for a hash table
      end
    end

    it 'returns the whole set if count is equal to or greater than the cardinality' do
      assert_command_results [
        [ 'SADD s 1 2 3 4', ':4' ],
        [ 'SPOP s 5', unordered([ '1', '2', '3', '4' ]) ],
        [ 'TYPE s', '+none' ],
        [ 'SADD s a b c d', ':4' ],
        [ 'SPOP s 5', unordered([ 'a', 'b', 'c', 'd' ]) ],
        [ 'TYPE s', '+none' ],
      ]
    end

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
        socket.write(to_query('SADD', 's', '1', '2', '3', '4', '5', '6', '7', '8', '9'))
        response = read_response(socket)
        assert_equal(":9\r\n", response)

        socket.write(to_query('SRANDMEMBER', 's', '1'))
        response = read_response(socket)

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*1', length)
        assert_equal(2, parts.length)
        parts.each_slice(2) do |part|
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ], [ '$1', '4' ],
                            [ '$1', '5' ], [ '$1', '6' ], [ '$1', '7' ], [ '$1', '8' ],
                            [ '$1', '9' ] ], part)
        end

        socket.write(to_query('SRANDMEMBER', 's', '2'))
        response = read_response(socket)

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*2', length)
        assert_equal(4, parts.length)
        parts.each_slice(2) do |part|
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ], [ '$1', '4' ],
                            [ '$1', '5' ], [ '$1', '6' ], [ '$1', '7' ], [ '$1', '8' ],
                            [ '$1', '9' ] ], part)
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
          assert_includes([ [ '$1', '1' ], [ '$1', '2' ], [ '$1', '3' ], [ '$1', '4' ],
                            [ '$1', '5' ], [ '$1', '6' ], [ '$1', '7' ], [ '$1', '8' ],
                            [ '$1', '9' ], [ '$1', 'a' ], [ '$1', 'b' ], [ '$1', 'c' ] ], part)
        end

        socket.write(
          to_query(*(300.times.map { |i| (i + 100).to_s }.prepend('SADD', 's')))
        )
        response = read_response(socket)
        assert_equal(":300\r\n", response)
        socket.write(to_query('SRANDMEMBER', 's', '311'))
        response = read_response(socket)
        assert(!response.nil?,
               'Expected to have received a response before timeout for SRANDMEMBER')

        parts = response.split("\r\n")
        length = parts.shift
        assert_equal('*311', length)
        assert_equal(622, parts.length)
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
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SREM', '-ERR wrong number of arguments for \'SREM\' command' ],
        [ 'SREM s', '-ERR wrong number of arguments for \'SREM\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SREM not-a-set s1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the number of removed elements' do
      assert_command_results [
        [ 'SADD s 1 2 3', ':3' ],
        [ 'SREM s 3 2 4 5', ':2' ],
        [ 'SREM s a', ':0' ],
        [ 'SMEMBERS s', unordered([ '1' ]) ],
        [ 'SADD s b c a', ':3' ],
        [ 'SREM s d a b', ':2' ],
        [ 'SMEMBERS s', unordered([ '1', 'c' ]) ],
      ]
    end

    it 'returns 0 if the set does not exist' do
      assert_command_results [
        [ 'SREM s a', ':0' ],
      ]
    end
  end

  describe 'SUNION' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SUNION', '-ERR wrong number of arguments for \'SUNION\' command' ],
      ]
    end

    it 'returns an error if one of the inputs is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SADD a-set 1 2 3', ':3' ],
        [ 'SUNION not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'SUNION non-existing not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'SUNION a-set not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an empty array for a non existing set' do
      assert_command_results [
        [ 'SUNION s', [] ],
      ]
    end

    it 'returns the union of all the given sets' do
      assert_command_results [
        [ 'SADD s1 1 2 3', ':3' ],
        [ 'SADD s2 3 4 5', ':3' ],
        [ 'SUNION s1 s2', unordered([ '1', '2', '3', '4', '5' ]) ],
        [ 'SADD s1 a b c', ':3' ],
        [ 'SADD s2 c d e', ':3' ],
        [ 'SUNION s1 s2', unordered([ '1', '2', '3', '4', '5', 'a', 'b', 'c', 'd', 'e' ]) ],
      ]
    end
  end

  describe 'SUNIONSTORE' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SUNIONSTORE', '-ERR wrong number of arguments for \'SUNIONSTORE\' command' ],
        [ 'SUNIONSTORE dest', '-ERR wrong number of arguments for \'SUNIONSTORE\' command' ],
      ]
    end

    it 'returns an error if one of the inputs is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'SADD a-set 1 2 3', ':3' ],
        [ 'SUNIONSTORE dest not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'SUNIONSTORE dest non-existing not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'SUNIONSTORE dest a-set not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns 0 for a non existing set' do
      assert_command_results [
        [ 'SUNIONSTORE dest s', ':0' ],
        [ 'TYPE dest', '+none' ],
      ]
    end

    it 'returns the size of the new set and store the union of all the given sets' do
      assert_command_results [
        [ 'SADD s1 1 2 3', ':3' ],
        [ 'SADD s2 3 4 5', ':3' ],
        [ 'SUNIONSTORE dest s1 s2', ':5' ],
        [ 'SMEMBERS dest', unordered([ '1', '2', '3', '4', '5' ]) ],
        [ 'SADD s1 a b c', ':3' ],
        [ 'SADD s2 c d e', ':3' ],
        [ 'SUNIONSTORE dest s1 s2', ':10' ],
        [ 'SMEMBERS dest', unordered([ '1', '2', '3', '4', '5', 'a', 'b', 'c', 'd', 'e' ]) ],
      ]
    end

    it 'stores the same set in dest with a single set argument' do
      assert_command_results [
        [ 'SADD s1 20 10 30', ':3' ],
        [ 'SUNIONSTORE dest s1', ':3' ],
        [ 'SCARD dest', ':3' ],
        [ 'SMEMBERS dest', [ '10', '20', '30' ] ],
        [ 'SADD s2 20 b c a 10', ':5' ],
        [ 'SUNIONSTORE dest s2', ':5' ],
        [ 'SCARD dest', ':5' ],
        [ 'SMEMBERS dest', unordered([ '20', '10', 'b', 'c', 'a' ]) ],
      ]
    end

    it 'can use one of the input sets as the dest, and overwrites it' do
      assert_command_results [
        [ 'SADD s1 20 10 30', ':3' ],
        [ 'SADD s2 10 30 40', ':3' ],
        [ 'SUNIONSTORE s1 s1 s2', ':4' ],
        [ 'SMEMBERS s1', unordered([ '10', '20', '30', '40' ]) ],
      ]
    end

    it 'overwrites destination if it already exists' do
      assert_command_results [
        [ 'SET dest a', '+OK' ],
        [ 'SADD s1 20 10 30', ':3' ],
        [ 'SUNIONSTORE dest s1', ':3' ],
        [ 'SMEMBERS dest', unordered([ '10', '20', '30' ]) ],
      ]
    end
  end
end
