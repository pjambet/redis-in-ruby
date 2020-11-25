#!/usr/bin/env ruby
require_relative './test_helper'

describe 'Bitops Commands' do
  describe 'GETBIT' do
    it 'handles and unexpected number of arguments' do
      assert_command_results [
        [ 'GETBIT', '-ERR wrong number of arguments for \'GETBIT\' command' ],
        [ 'GETBIT s', '-ERR wrong number of arguments for \'GETBIT\' command' ],
      ]
    end

    it 'validates that offset is a positive integer' do
      assert_command_results [
        [ 'GETBIT s a', '-ERR bit offset is not an integer or out of range' ],
        [ 'GETBIT s -1', '-ERR bit offset is not an integer or out of range' ],
      ]
    end

    it 'returns an error if key is not a string' do
      assert_command_results [
        [ 'HSET not-a-string a b', ':1' ],
        [ 'GETBIT not-a-string a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the bit at offset' do
      with_server do |socket|
        socket.write(to_query('SET', 's', 'abc'))
        assert_equal(read_response(socket), BYORedis::OK_SIMPLE_STRING)
        # The string abc has the following bytes:
        # [ "01100001", "01100010", "01100011" ] and this line returns an array of ints:
        # [0, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 1]
        bits = 'abc'.chars.map(&:ord).map { |byte| '%08b' % byte }.flat_map { |s| s.split('') }.map(&:to_i)
        bits.each_with_index do |bit, index|
          socket.write(to_query('GETBIT', 's', index.to_s))
          assert_equal(read_response(socket),  BYORedis::RESPInteger.new(bit).serialize)
        end
      end
    end

    it 'returns 0 if the offset it larger than the string' do
      assert_command_results [
        [ 'SET s a', '+OK' ],
        [ 'GETBIT s 100', ':0' ],
      ]
    end
  end

  describe 'SETBIT' do
    it 'handles and unexpected number of arguments' do
      assert_command_results [
        [ 'SETBIT', '-ERR wrong number of arguments for \'SETBIT\' command' ],
        [ 'SETBIT s', '-ERR wrong number of arguments for \'SETBIT\' command' ],
      ]
    end

    it 'returns an error if key is not a string' do
      assert_command_results [
        [ 'HSET not-a-string a b', ':1' ],
        [ 'SETBIT not-a-string 0 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'validates that offset is a positive integer' do
      assert_command_results [
        [ 'SETBIT s a 0', '-ERR bit offset is not an integer or out of range' ],
        [ 'SETBIT s -1 0', '-ERR bit offset is not an integer or out of range' ],
      ]
    end

    it 'validates that value is 0 or 1' do
      assert_command_results [
        [ 'SETBIT s 0 2', '-ERR bit is not an integer or out of range' ],
        [ 'SETBIT s 0 a', '-ERR bit is not an integer or out of range' ],
        [ 'SETBIT s 0 -1', '-ERR bit is not an integer or out of range' ],
      ]
    end

    it 'sets the bit at the given offset and return 0 if it did not exist' do
      assert_command_results [
        [ 'SETBIT s 0 1', ':0' ],
        [ 'GETBIT s 0', ':1' ],
      ]
    end

    it 'increases the string size if necessary' do
      assert_command_results [
        [ 'SETBIT s 0 1', ':0' ], # s has 8 bits, 1 byte, size 1
        [ 'GETBIT s 0', ':1' ],
        [ 'SETBIT s 8 1', ':0' ], # s has 16 bits, 2 bytes
        [ 'GETBIT s 8', ':1' ],
        [ 'GET s', "\x80\x80" ],
        [ 'SETBIT s 1048576 1', ':0' ],
        [ 'STRLEN s', ':131073' ],
      ]
    end

    it 'sets the bit at the given offset and returns the previous value' do
      assert_command_results [
        [ 'SETBIT s 6 1', ':0' ],
        [ 'GETBIT s 6', ':1' ],
        [ 'SETBIT s 6 1', ':1' ],
        [ 'GETBIT s 6', ':1' ],
        [ 'SETBIT s 6 0', ':1' ],
        [ 'GETBIT s 6', ':0' ],
      ]
    end
  end

  describe 'BITOP' do
    it 'handles and unexpected number of arguments' do
      assert_command_results [
        [ 'BITOP', '-ERR wrong number of arguments for \'BITOP\' command' ],
        [ 'BITOP s', '-ERR wrong number of arguments for \'BITOP\' command' ],
      ]
    end

    # it 'returns an error if key is not a string' do
    #   assert_command_results [
    #     [ 'HSET not-a-string a b', ':1' ],
    #     [ 'BITOP not-a-string 0 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
    #   ]
    # end

    it 'works with the example from redis.io' do
      assert_command_results [
        [ 'SET key1 foobar', '+OK' ],
        [ 'SET key2 abcdef', '+OK' ],
        [ 'BITOP AND dest key1 key2', ':6' ],
        [ 'GET dest', '`bc`ab' ],
      ]
    end

    # it 'works with AND a non existing key'

    it 'works with AND and multiple arguments' do
      assert_command_results [
        [ 'SETBIT s1 1 1', ':0' ],
        [ 'SETBIT s2 1 1', ':0' ],
        [ 'SETBIT s3 1 0', ':0' ],
        [ 'BITOP AND dest s1', ':1' ],
        [ 'GET dest', '@' ], # The byte 64, \x40
        [ 'SETBIT s4 17 0', ':0' ], # 3 bytes \x00\x00\x80, TODO doesn't work with 16, need to debug that
        [ 'BITOP AND dest s1 s4', ':3' ],
        [ 'GET dest', "\x00\x00\x00" ],
      ]
    end

    # it 'works with OR and multiple arguments'
    # it 'works with XOR and multiple arguments'
    # it 'works with NOT and one argument'
    # it 'returns an error with NOT and more than one argument'
  end

  # describe 'BITCOUNT' do
  #   it 'handles and unexpected number of arguments'
  #   it 'returns an error if key is not a string'

  #   it 'counts the number of 1s in the whole string without a range'
  #   it 'counts the number of 1s within the given byte range'
  # end

  # describe 'BITPOS' do
  #   it 'handles and unexpected number of arguments'
  #   it 'returns an error if key is not a string'

  #   it 'returns the position of the first 1 (as a 0 based bit offset) in the whole string without a range'
  #   it 'returns the position of the first 0 (as a 0 based bit offset) in the whole string without a range'
  #   it 'returns the position of the first 1 (as a 0 based bit offset) in the given byte range'
  #   it 'returns the position of the first 0 (as a 0 based bit offset) in the given byte range'
  # end

  # describe 'BITFIELD' do
  #   it 'handles and unexpected number of arguments'
  #   it 'returns an error if key is not a string'

  #   it 'is a no-op and return an empty array with no operations'
  #   it 'can GET with all types of formats'
  #   it 'can GET with pound-prefixed offsets'
  #   it 'can SET with all types of formats'
  #   it 'can INCRBY with all types of formats'
  #   it 'handles changing the OVERFLOW behavior in the same command'
  #   it 'handles the WRAP overflow'
  #   it 'handles the SAT overflow'
  #   it 'handles the FAIL overlow'
  # end
end
