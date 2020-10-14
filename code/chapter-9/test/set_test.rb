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
      with_server do |socket|
        socket.write to_query('SADD', 's1', 'a', 'b', 'c', 'd')
        socket.write to_query('SADD', 's2', 'c')
        socket.write to_query('SADD', 's3', 'a', 'c', 'e')
        sleep 0.01 # Sleep long enough for the server to process all three commands
        read_response(socket)

        socket.write to_query('SDIFF', 's1', 's2', 's3')
        response = read_response(socket)

        sorted_response = response.split.then do |r|
          r.shift
          r.each_slice(2).sort
        end
        assert_equal([ [ '$1', 'b' ], [ '$1', 'd' ] ], sorted_response)
      end
    end
  end

  describe 'SDIFFSTORE' do
    it 'handles unexpected number of arguments'
  end

  describe 'SINTER' do
    it 'handles unexpected number of arguments'
  end
  describe 'SINTERSTORE' do
    it 'handles unexpected number of arguments'
  end

  describe 'SISMEMBER' do
    it 'handles unexpected number of arguments'
  end

  describe 'SMEMBERS' do
    it 'handles unexpected number of arguments'
  end

  describe 'SMOVE' do
    it 'handles unexpected number of arguments'
  end

  describe 'SPOP' do
    it 'handles unexpected number of arguments'
  end

  describe 'SRANDMEMBER' do
    it 'handles unexpected number of arguments'
  end

  describe 'SREM' do
    it 'handles unexpected number of arguments'
  end

  describe 'SUNION' do
    it 'handles unexpected number of arguments'
  end

  describe 'SUNIONSTORE' do
    it 'handles unexpected number of arguments'
  end
end
