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
        [ 'SETBIT s 16 0', ':0' ],
        [ 'GET s', "\x80\x80\x00" ],
        [ 'SETBIT s 1048576 1', ':0' ],
        [ 'STRLEN s', ':131073' ],
        [ 'SETBIT s2 16 0', ':0' ], # a new string, of size 3
        [ 'GET s2', "\x00\x00\x00" ],
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

    it 'returns an error if key is not a string' do
      assert_command_results [
        [ 'HSET not-a-string a b', ':1' ],
        [ 'BITOP AND dest not-a-string', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'BITOP OR dest not-a-string', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'BITOP XOR dest not-a-string', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
        [ 'BITOP NOT dest not-a-string', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an error with an invalid operation' do
      assert_command_results [
        [ 'BITOP not-a-thing dest s', '-ERR syntax error' ],
      ]
    end

    it 'works with the example from redis.io' do
      assert_command_results [
        [ 'SET key1 foobar', '+OK' ],
        [ 'SET key2 abcdef', '+OK' ],
        [ 'BITOP AND dest key1 key2', ':6' ],
        [ 'GET dest', '`bc`ab' ],
      ]
    end

    it 'returns an empty string for any operations and a non existing key' do
      assert_command_results [
        [ 'SET dest something', '+OK' ],
        [ 'BITOP AND dest s1 s2', ':0' ],
        [ 'GET dest', BYORedis::NULL_BULK_STRING ],
        [ 'BITOP OR dest s1 s2', ':0' ],
        [ 'GET dest', BYORedis::NULL_BULK_STRING ],
        [ 'BITOP XOR dest s', ':0' ],
        [ 'GET dest', BYORedis::NULL_BULK_STRING ],
        [ 'BITOP NOT dest s', ':0' ],
        [ 'GET dest', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'works with AND and multiple arguments' do
      assert_command_results [
        [ 'SETBIT s1 1 1', ':0' ],
        [ 'SETBIT s2 1 1', ':0' ],
        [ 'SETBIT s3 1 0', ':0' ],
        [ 'BITOP AND dest s1', ':1' ],
        [ 'GET dest', '@' ], # The byte 64, \x40, 0100 000
        [ 'BITOP AND dest s1 s2 s3', ':1' ],
        [ 'GET dest', "\x00" ],
        [ 'SETBIT s4 17 1', ':0' ], # 3 bytes \x00\x00\x80
        [ 'BITOP AND dest s1 s4', ':3' ],
        [ 'GET dest', "\x00\x00\x00" ],
      ]
    end

    it 'works with OR and multiple arguments' do
      assert_command_results [
        [ 'SETBIT s1 1 1', ':0' ],
        [ 'SETBIT s2 1 1', ':0' ],
        [ 'SETBIT s3 1 0', ':0' ],
        [ 'BITOP OR dest s1', ':1' ],
        [ 'GET dest', '@' ], # The byte 64, \x40, 0100 0000
        [ 'BITOP OR dest s1 s2 s3', ':1' ],
        [ 'GET dest', '@' ],
        [ 'SETBIT s4 17 1', ':0' ], # 3 bytes \x00\x00\x80,
        [ 'BITOP OR dest s1 s4', ':3' ],
        [ 'GET dest', "\x40\x00\x40" ], # ['01000000', '00000000', '01000000'].pack('B8B8B8')
      ]
    end

    it 'works with XOR and multiple arguments' do
      assert_command_results [
        [ 'SETBIT s1 1 1', ':0' ],
        [ 'SETBIT s2 1 1', ':0' ],
        [ 'SETBIT s3 1 0', ':0' ],
        [ 'BITOP XOR dest s1', ':1' ],
        [ 'GET dest', '@' ], # The byte 64, \x40, 0100 0000
        [ 'BITOP XOR dest s1 s2', ':1' ],
        [ 'GET dest', "\x00" ],
        [ 'BITOP XOR dest s1 s3', ':1' ],
        [ 'GET dest', "\x40" ],
        [ 'BITOP XOR dest s1 s2 s3', ':1' ],
        [ 'GET dest', "\x00" ],
        [ 'SETBIT s4 17 1', ':0' ], # 3 bytes \x00\x00\x80,
        [ 'BITOP XOR dest s1 s4', ':3' ],
        [ 'GET dest', "\x40\x00\x40" ], # ['01000000', '00000000', '01000000'].pack('B8B8B8')
      ]
    end

    it 'works with NOT and one argument' do
      assert_command_results [
        [ 'SETBIT s1 1 1', ':0' ],
        [ 'SETBIT s2 1 0', ':0' ],
        [ 'BITOP NOT dest s1', ':1' ],
        [ 'GET dest', "\xBF" ], # 10111111
      ]
    end

    it 'returns an error with NOT and more than one argument' do
      assert_command_results [
        [ 'BITOP NOT dest s1 s2', '-ERR BITOP NOT must be called with a single source key.' ],
      ]
    end
  end

  describe 'BITCOUNT' do
    it 'handles and unexpected number of arguments' do
      assert_command_results [
        [ 'BITCOUNT', '-ERR wrong number of arguments for \'BITCOUNT\' command' ],
      ]
    end

    it 'returns an error if key is not a string' do
      assert_command_results [
        [ 'HSET not-a-string a b', ':1' ],
        [ 'BITCOUNT not-a-string', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'ignores everything past key if it does not exist' do
      assert_command_results [
        [ 'BITCOUNT s a a a a', ':0' ],
      ]
    end

    it 'validates that start & end are integers' do
      assert_command_results [
        [ 'SET s abc', '+OK' ],
        [ 'BITCOUNT s a', '-ERR syntax error' ],
        [ 'BITCOUNT s a a', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'counts the number of 1s in the whole string without a range' do
      assert_command_results [
        [ 'SETBIT s 16 0', ':0' ],
        [ 'BITCOUNT s', ':0' ],
        [ 'SETBIT s 256 1', ':0' ],
        [ 'BITCOUNT s', ':1' ],
        [ 'SETBIT s2 0 1', ':0' ],
        [ 'SETBIT s2 1 1', ':0' ],
        [ 'SETBIT s2 2 1', ':0' ],
        [ 'SETBIT s2 3 1', ':0' ],
        [ 'SETBIT s2 4 1', ':0' ],
        [ 'SETBIT s2 5 1', ':0' ],
        [ 'SETBIT s2 6 1', ':0' ],
        [ 'SETBIT s2 7 1', ':0' ],
        [ 'BITCOUNT s2', ':8' ],
      ]
    end

    it 'counts the number of 1s within the given byte range' do
      assert_command_results [
        [ 'SETBIT s 16 0', ':0' ],
        [ 'BITCOUNT s 0 -1', ':0' ],
        [ 'SETBIT s 256 1', ':0' ],
        [ 'BITCOUNT s 0 -1', ':1' ],
        [ 'BITCOUNT s 0 -2', ':0' ],
        [ 'BITCOUNT s 0 31', ':0' ],
        [ 'BITCOUNT s 31 32', ':1' ],
        [ 'BITCOUNT s 31 33', ':1' ],
      ]
    end
  end

  describe 'BITPOS' do
    it 'handles and unexpected number of arguments' do
      assert_command_results [
        [ 'BITPOS', '-ERR wrong number of arguments for \'BITPOS\' command' ],
        [ 'BITPOS s', '-ERR wrong number of arguments for \'BITPOS\' command' ],
        [ 'SET s s', '+OK' ], # it would otherwise shortcut the response and return early if nil
        [ 'BITPOS s 0 0 -1 a', '-ERR syntax error' ],
        [ 'BITPOS s 0 0 -1 1', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if key is not a string' do
      assert_command_results [
        [ 'HSET not-a-string a b', ':1' ],
        [ 'BITPOS not-a-string 0', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'validates that bit is 0 or 1' do
      assert_command_results [
        [ 'BITPOS s a', '-ERR value is not an integer or out of range' ],
        [ 'BITPOS s -1', '-ERR The bit argument must be 1 or 0.' ],
      ]
    end

    it 'ignores everything past key if it does not exist' do
      assert_command_results [
        [ 'BITPOS s 0 a a a', ':0' ],
      ]
    end

    it 'validates that start & end are integers' do
      assert_command_results [
        [ 'SET s abc', '+OK' ],
        [ 'BITPOS s 0 a a', '-ERR value is not an integer or out of range' ],
        [ 'BITPOS s 0 a 1', '-ERR value is not an integer or out of range' ],
        [ 'BITPOS s 0 1 a', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns 0 for 0 and an empty string' do
      assert_command_results [
        [ 'BITPOS s 0', ':0' ],
      ]
    end

    it 'returns -1 for 1 and an empty string' do
      assert_command_results [
        [ 'BITPOS s 1', ':-1' ],
      ]
    end

    it 'accepts start without end for the range definition' do
      assert_command_results [
        [ 'SETBIT s 64 1', ':0' ],
        [ 'BITPOS s 0 0', ':0' ],
        [ 'BITPOS s 1 0', ':64' ],
        [ 'BITPOS s 0 8', ':65' ],
        [ 'BITPOS s 1 8', ':64' ],
        [ 'BITPOS s 0 9', ':-1' ],
        [ 'BITPOS s 1 9', ':-1' ],
      ]
    end

    # Special case because the string is always assumed to be 0-padded to the right
    it 'returns the first index outside the string if bit is 0 and the string is only 1s' do
      assert_command_results [
        [ 'SETBIT s 0 1', ':0' ],
        [ 'SETBIT s 1 1', ':0' ],
        [ 'SETBIT s 2 1', ':0' ],
        [ 'SETBIT s 3 1', ':0' ],
        [ 'SETBIT s 4 1', ':0' ],
        [ 'SETBIT s 5 1', ':0' ],
        [ 'SETBIT s 6 1', ':0' ],
        [ 'SETBIT s 7 1', ':0' ],
        [ 'BITPOS s 0', ':-1' ],
      ]
    end

    it 'returns the position of the first 1 (as a 0 based bit offset) in the whole string without a range' do
      assert_command_results [
        [ 'SETBIT s 64 1', ':0' ],
        [ 'BITPOS s 0', ':0' ],
        [ 'BITPOS s 1', ':64' ],
      ]
    end

    it 'returns the position of the first 0 (as a 0 based bit offset) in the whole string without a range' do
      with_server do |socket|
        socket.write(to_query('SET', 's', "\xff\xff\xff\xf0"))
        assert_equal(read_response(socket), BYORedis::OK_SIMPLE_STRING)

        socket.write(to_query('BITPOS', 's', '0'))
        assert_equal(read_response(socket), ":28\r\n")
      end
    end

    it 'returns the position of the first 1 (as a 0 based bit offset) in the given byte range' do
      assert_command_results [
        [ 'SETBIT s 64 1', ':0' ],
        [ 'BITPOS s 1 1 0', ':-1' ],
        [ 'BITPOS s 1 0 1', ':-1' ],
        [ 'BITPOS s 1 8 8', ':64' ],
        [ 'BITPOS s 1 8 9', ':64' ],
        [ 'BITPOS s 1 8 -1', ':64' ],
        [ 'BITPOS s 1 -2 -1', ':64' ],
        [ 'BITPOS s 1 9 -1', ':-1' ],
      ]
    end

    it 'returns the position of the first 0 (as a 0 based bit offset) in the given byte range' do
      with_server do |socket|
        socket.write(to_query('SET', 's', "\xff\xff\xff\xf0"))
        assert_equal(read_response(socket), BYORedis::OK_SIMPLE_STRING)

        socket.write(to_query('BITPOS', 's', '0', '1', '0'))
        assert_equal(read_response(socket), ":-1\r\n")
        socket.write(to_query('BITPOS', 's', '0', '0', '1'))
        assert_equal(read_response(socket), ":-1\r\n")
        socket.write(to_query('BITPOS', 's', '0', '3', '4'))
        assert_equal(read_response(socket), ":28\r\n")
        socket.write(to_query('BITPOS', 's', '0', '3', '-1'))
        assert_equal(read_response(socket), ":28\r\n")
        socket.write(to_query('BITPOS', 's', '0', '-2', '-1'))
        assert_equal(read_response(socket), ":28\r\n")
        socket.write(to_query('BITPOS', 's', '0', '4', '4'))
        assert_equal(read_response(socket), ":-1\r\n")
      end
    end
  end

  describe 'BITFIELD' do
    it 'handles and unexpected number of arguments' do
      assert_command_results [
        [ 'BITFIELD', '-ERR wrong number of arguments for \'BITFIELD\' command' ],
      ]
    end

    it 'returns an error if key is not a string' do
      assert_command_results [
        [ 'HSET not-a-string a b', ':1' ],
        [ 'BITFIELD not-a-string', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'is a no-op and return an empty array with no operations' do
      assert_command_results [
        [ 'BITFIELD s', [] ],
      ]
    end

    it 'can GET signed and unsigned integers' do
      assert_command_results [
        [ 'BITFIELD s GET i8 0', [ 0 ] ],
        [ 'BITFIELD s GET u8 0', [ 0 ] ],
        [ 'SETBIT s 8 1', ':0' ],
        [ 'BITFIELD s GET u8 8', [ 128 ] ],
        [ 'BITFIELD s GET i8 8', [ -128 ] ],
        [ 'BITFIELD s GET u8 4', [ 8 ] ],
        [ 'BITFIELD s GET i8 4', [ 8 ] ],
        [ 'SETBIT s 4 1', ':0' ],
        [ 'BITFIELD s GET u8 4', [ 136 ] ],
        [ 'BITFIELD s GET i8 4', [ -120 ] ],
        [ 'BITFIELD s GET u8 3', [ 68 ] ],
        [ 'BITFIELD s GET i8 3', [ 68 ] ],
      ]
    end

    it 'can GET with pound-prefixed offsets' do
      assert_command_results [
        [ 'SETBIT s 0 1', ':0' ],
        [ 'SETBIT s 4 1', ':0' ],
        [ 'SETBIT s 8 1', ':0' ],
        [ 'BITFIELD s GET u8 #0', [ 136 ] ],
        [ 'BITFIELD s GET u8 #1', [ 128 ] ],
        [ 'BITFIELD s GET u8 #2', [ 0 ] ],
        [ 'BITFIELD s GET i8 #0', [ -120 ] ],
        [ 'BITFIELD s GET i8 #1', [ -128 ] ],
        [ 'BITFIELD s GET i8 #2', [ 0 ] ],
      ]
    end

    # it 'can GET with all types of formats' do
    #   assert_command_results [
    #     [ 'BITFIELD s GET i1 0', ':0' ],
    #   ]
    # end
  #   it 'can SET with all types of formats'
  #   it 'can INCRBY with all types of formats'
  #   it 'handles changing the OVERFLOW behavior in the same command'
  #   it 'handles the WRAP overflow with incr'
  #   it 'handles the SAT overflow with incr'
  #   it 'handles the FAIL overlow with incr'
  #   it 'handles the WRAP overflow with set'
  #   it 'handles the SAT overflow with set'
  #   it 'handles the FAIL overlow with set'
  end
end
