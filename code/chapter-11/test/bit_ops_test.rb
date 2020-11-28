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
        [ 'BITFIELD s GET i8 0 GET u8 0', [ 0, 0 ] ],
        [ 'SETBIT s 8 1', ':0' ],
        [ 'BITFIELD s GET u8 8 GET i8 8 GET u8 4 GET i8 4', [ 128, -128, 8, 8 ] ],
        [ 'SETBIT s 4 1', ':0' ],
        [ 'BITFIELD s GET u8 4 GET i8 4 GET u8 3 GET i8 3', [ 136, -120, 68, 68 ] ],
      ]
    end

    it 'can GET with pound-prefixed offsets' do
      assert_command_results [
        [ 'SETBIT s 0 1', ':0' ],
        [ 'SETBIT s 4 1', ':0' ],
        [ 'SETBIT s 8 1', ':0' ],
        [ 'BITFIELD s GET u8 #0 GET u8 #1 GET u8 #2', [ 136, 128, 0 ] ],
        [ 'BITFIELD s GET i8 #0 GET i8 #1 GET i8 #2', [ -120, -128, 0 ] ],
      ]
    end

    it 'can GET with all types of formats' do
      with_server do |socket|
        # \xaa is 1010 1010
        socket.write(to_query('SET', 's', "\xaa" * 10))
        assert_equal(read_response(socket), BYORedis::OK_SIMPLE_STRING)
        operations = []

        operations.push('GET', 'i64', '2')
        63.times do |i|
          operations.push('GET', "i#{ 63 - i }", (i + 3).to_s)
          operations.push('GET', "u#{ 63 - i }", (i + 2).to_s)
        end

        expected_responses = [
          -6148914691236517206, 3074457345618258602, 6148914691236517205, -1537228672809129302,
           1537228672809129301, 768614336404564650, 1537228672809129301, -384307168202282326,
           384307168202282325, 192153584101141162, 384307168202282325, -96076792050570582,
           96076792050570581, 48038396025285290, 96076792050570581, -24019198012642646,
           24019198012642645, 12009599006321322, 24019198012642645, -6004799503160662,
           6004799503160661, 3002399751580330, 6004799503160661, -1501199875790166,
           1501199875790165, 750599937895082, 1501199875790165, -375299968947542,
           375299968947541, 187649984473770, 375299968947541, -93824992236886, 93824992236885,
           46912496118442, 93824992236885, -23456248059222, 23456248059221, 11728124029610,
           23456248059221, -5864062014806, 5864062014805, 2932031007402, 5864062014805,
           -1466015503702, 1466015503701, 733007751850, 1466015503701, -366503875926,
           366503875925, 183251937962, 366503875925, -91625968982, 91625968981, 45812984490,
           91625968981, -22906492246, 22906492245, 11453246122, 22906492245, -5726623062,
           5726623061, 2863311530, 5726623061, -1431655766, 1431655765, 715827882, 1431655765,
           -357913942, 357913941, 178956970, 357913941, -89478486, 89478485, 44739242, 89478485,
           -22369622, 22369621, 11184810, 22369621, -5592406, 5592405, 2796202, 5592405,
           -1398102, 1398101, 699050, 1398101, -349526, 349525, 174762, 349525, -87382, 87381,
           43690, 87381, -21846, 21845, 10922, 21845, -5462, 5461, 2730, 5461, -1366, 1365, 682,
           1365, -342, 341, 170, 341, -86, 85, 42, 85, -22, 21, 10, 21, -6, 5, 2, 5, -2, 1, 0, 1,
        ]

        socket.write(to_query('BITFIELD', 's', *operations))
        response = read_response(socket, read_timeout: 0.5)
        response_parts = response.split("\r\n")
        assert_equal('*127', response_parts.shift)
        response_parts.each_with_index do |response_part, i|
          assert_equal(":#{ expected_responses[i] }", response_part, "Failure for #{ operations[i * 3, 3] }")
        end
      end
    end

    it 'can SET with all types of formats' do
      assert_command_results [
        [ 'SETBIT s 4 1', ':0' ],
        [ 'SETBIT s 24 1', ':0' ],
        [ 'BITFIELD s SET u8 0 128 GET u8 0', [ 8, 128 ] ],
        [ 'BITFIELD s SET u16 4 1065 GET u16 4', [ 0, 1065 ] ],
        [ 'BITFIELD s SET u16 0 2047 GET u16 0', [ 32834, 2047 ] ],
        [ 'BITFIELD s SET i8 0 127 GET i8 0', [ 7, 127 ] ],
        [ 'BITFIELD s SET i16 4 1065 GET i16 4', [ -7, 1065 ] ],
        [ 'BITFIELD s SET i16 0 2047 GET i16 0', [ 28738, 2047 ] ],
        [ 'BITFIELD s SET i14 3 -200 GET i14 3', [ 4095, -200 ] ],
      ]
    end

    it 'handles the WRAP overflow with set' do
      assert_command_results [
        [ 'BITFIELD s SET i4 0 10 GET i4 0', [ 0, -6 ] ],
        [ 'GET s', "\xa0" ],
        [ 'BITFIELD s SET u4 0 18 GET u4 0', [ 10, 2 ] ],
      ]
      assert_command_results [
        [ 'DEL s', ':1' ],
        [ 'BITFIELD s OVERFLOW WRAP SET i4 0 10 GET i4 0', [ 0, -6 ] ],
        [ 'BITFIELD s OVERFLOW WRAP SET u4 0 18 GET u4 0', [ 10, 2 ] ],
      ]
    end

    it 'handles the SAT overflow with set' do
      assert_command_results [
        [ 'BITFIELD s OVERFLOW SAT SET i4 0 10 GET i4 0', [ 0, 7 ] ],
        [ 'BITFIELD s OVERFLOW SAT SET u4 0 18 GET u4 0', [ 7, 15 ] ],
        [ 'BITFIELD s OVERFLOW SAT SET u4 0 -2 GET u4 0', [ 15, 15 ] ],
      ]
    end

    it 'handles the FAIL overlow with set' do
      assert_command_results [
        [ 'SETBIT s 7 1', ':0' ],
        [ 'BITFIELD s OVERFLOW FAIL SET i4 0 10 GET i4 0', [ nil, 0 ] ],
        [ 'GET s', "\x01" ],
        [ 'BITFIELD s OVERFLOW FAIL SET u4 0 18 GET u4 0', [ nil, 0 ] ],
        [ 'GET s', "\x01" ],
      ]
    end

    it 'can INCRBY with all types of formats' do
      assert_command_results [
        [ 'BITFIELD s INCRBY i4 0 6 GET i4 0', [ 6, 6 ] ],
        [ 'BITFIELD s INCRBY i4 0 -2 GET i4 0', [ 4, 4 ] ],
        [ 'BITFIELD s INCRBY i3 0 1 GET i3 1', [ 3, -2 ] ],
        [ 'BITFIELD s INCRBY i9 3 63 GET i9 3', [ 63, 63 ] ],
        [ 'BITFIELD s INCRBY i7 4 10 GET i7 4', [ 41, 41 ] ],
      ]
    end

    it 'handles changing the OVERFLOW behavior in the same command' do
      assert_command_results [
        [ 'BITFIELD s INCRBY i4 0 8 OVERFLOW SAT INCRBY i4 0 20 OVERFLOW FAIL INCRBY i4 0 1', [ -8, 7, nil ] ],
        [ 'BITFIELD s INCRBY u4 0 17 OVERFLOW SAT INCRBY u4 0 20 OVERFLOW FAIL INCRBY u4 0 1', [ 1, 15, nil ] ],
      ]
    end

    # it 'handles the WRAP overflow with incr' do
    #   assert_command_results [
    #     [ 'BITFIELD s OVERFLOW WRAP INCRBY i4 0 10', ':-6' ],
    #     [ 'BITFIELD s OVERFLOW WARP INCRBY u4 0 10', ':4' ],
    #   ]
    # end

    # it 'handles the SAT overflow with incr' do
    #   assert_command_results [
    #     [ 'BITFIELD s OVERFLOW SAT INCRBY i4 0 10', ':7' ],
    #     [ 'BITFIELD s OVERFLOW SAT INCRBY u4 0 10', ':15' ],
    #   ]
    # end

    # it 'handles the FAIL overlow with incr' do
    #   assert_command_results [
    #     [ 'BITFIELD s OVERFLOW FAIL INCRBY i4 0 10', ':7' ],
    #     [ 'BITFIELD s OVERFLOW FAIL INCRBY u4 0 16', ':15' ],
    #   ]
    # end
  end
end
