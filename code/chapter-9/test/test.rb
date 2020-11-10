# coding: utf-8

require_relative './test_helper'

describe 'BYORedis::Server' do
  describe 'when initialized' do
    it 'listens on port 2000' do
      with_server do
        # lsof stands for "list open files", see for more info https://stackoverflow.com/a/4421674
        lsof_result = `lsof -nP -i4TCP:2000 | grep LISTEN`
        assert_match 'ruby', lsof_result
      end
    end
  end

  describe 'closing closed connections' do
    it 'explicitly closes the connection' do
      with_server do
        Timeout.timeout(1) do
          nc_result = `echo "GET 1" | nc -c localhost 2000`
          assert_match "$-1\r\n", nc_result
        end
      end
    end
  end

  describe 'case sensitivity' do
    it 'ignores it' do
      assert_command_results [
        [ 'gEt 1', BYORedis::NULL_BULK_STRING ],
        [ 'set 1 2', '+OK' ],
        [ 'get 1', '2' ],
      ]
    end
  end

  describe 'TYPE' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'TYPE a b', '-ERR wrong number of arguments for \'TYPE\' command' ],
      ]
    end

    it 'returns the type of the key if it exists' do
      assert_command_results [
        [ 'SET a-string something', '+OK' ],
        [ 'LPUSH a-list something', ':1' ],
        [ 'HSET a-hash something else', ':1' ],
        [ 'TYPE a-list', '+list' ],
        [ 'TYPE a-string', '+string' ],
        [ 'TYPE a-hash', '+hash' ],
      ]
    end

    it 'returns none if the key does not exist' do
      assert_command_results [
        [ 'TYPE not-a-key', '+none' ],
      ]
    end
  end

  describe 'GET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'GET', '-ERR wrong number of arguments for \'GET\' command' ],
      ]
    end

    it 'returns (nil) for unknown keys' do
      assert_command_results [
        [ 'GET 1', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns an error if the key exists and the value is not a string' do
      assert_command_results [
        [ 'LPUSH a-list 1', ':1' ],
        [ 'GET a-list', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the value previously set by SET' do
      assert_command_results [
        [ 'SET 1 2', '+OK' ],
        [ 'GET 1', '2' ],
        [ 'SET 1 😂', '+OK' ],
        [ 'GET 1', '😂' ],
      ]
    end

    it 'returns an error if the key is not a string' do
      assert_command_results [
        [ 'RPUSH not-a-string a', ':1' ],
        [ 'GET not-a-string', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end
  end

  describe 'TTL' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'TTL', '-ERR wrong number of arguments for \'TTL\' command' ],
      ]
    end

    it 'returns the TTL for a key with a TTL' do
      assert_command_results [
        [ 'SET key value EX 2', '+OK' ],
        [ 'TTL key', ':2' ],
        [ 'sleep 0.5' ],
        [ 'TTL key', ':1' ],
      ]
    end

    it 'returns -1 for a key without a TTL' do
      assert_command_results [
        [ 'SET key value', '+OK' ],
        [ 'TTL key', ':-1' ],
      ]
    end

    it 'returns -2 if the key does not exist' do
      assert_command_results [
        [ 'TTL key', ':-2' ],
      ]
    end
  end

  describe 'PTTL' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'PTTL', '-ERR wrong number of arguments for \'PTTL\' command' ],
      ]
    end

    it 'returns the TTL in ms for a key with a TTL' do
      assert_command_results [
        [ 'SET key value EX 2', '+OK' ],
        [ 'PTTL key', '2000+/-20' ], # Initial 2000ms +/- 20ms
        [ 'sleep 0.5' ],
        [ 'PTTL key', '1500+/-20' ], # Initial 2000ms, minus ~500ms of sleep, +/- 20ms
      ]
    end

    it 'returns -1 for a key without a TTL' do
      assert_command_results [
        [ 'SET key value', '+OK' ],
        [ 'PTTL key', ':-1' ],
      ]
    end

    it 'returns -2 if the key does not exist' do
      assert_command_results [
        [ 'PTTL key', ':-2' ],
      ]
    end
  end

  describe 'SET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SET', '-ERR wrong number of arguments for \'SET\' command' ],
      ]
    end

    it 'returns OK' do
      assert_command_results [
        [ 'SET 1 3', '+OK' ],
      ]
    end

    it 'works with empty string' do
      assert_multipart_command_results [
        [ [ to_query('SET', '', '') ], '+OK' ],
        [ [ to_query('GET', '') ], '' ],
      ]
    end

    it 'handles the EX option with a valid argument' do
      assert_command_results [
        [ 'SET 1 3 EX 1', '+OK' ],
        [ 'GET 1', '3' ],
        [ 'sleep 1' ],
        [ 'GET 1', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'rejects the EX option with an invalid argument' do
      assert_command_results [
        [ 'SET 1 3 EX foo', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'overrides a key of a different type' do
      assert_command_results [
        [ 'RPUSH not-a-string a', ':1' ],
        [ 'SET not-a-string a', '+OK' ],
      ]
    end

    it 'handles the PX option with a valid argument' do
      assert_command_results [
        [ 'SET 1 3 PX 100', '+OK' ],
        [ 'GET 1', '3' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'rejects the PX option with an invalid argument' do
      assert_command_results [
        [ 'SET 1 3 px foo', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'handles the NX option' do
      assert_command_results [
        [ 'SET 1 2 nX', '+OK' ],
        [ 'SET 1 2 nx', BYORedis::NULL_BULK_STRING ],
        [ 'SET 1 2 NX', BYORedis::NULL_BULK_STRING ],
        [ 'SET 1 2 Nx', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'handles the XX option' do
      assert_command_results [
        [ 'SET 1 2 XX', BYORedis::NULL_BULK_STRING ],
        [ 'SET 1 2', '+OK' ],
        [ 'SET 1 2 XX', '+OK' ],
        [ 'SET 1 2 xx', '+OK' ],
        [ 'SET 1 2 xX', '+OK' ],
      ]
    end

    it 'removes ttl without KEEPTTL' do
      assert_command_results [
        [ 'SET 1 3 PX 100', '+OK' ],
        [ 'SET 1 2', '+OK' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', '2' ],
      ]
    end

    it 'handles the KEEPTTL option' do
      assert_command_results [
        [ 'SET 1 3 PX 100', '+OK' ],
        [ 'SET 1 2 KEEPTTL', '+OK' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'accepts multiple options' do
      assert_command_results [
        [ 'SET 1 3 NX EX 1', '+OK' ],
        [ 'GET 1', '3' ],
        [ 'SET 1 3 XX keepttl', '+OK' ],
      ]
    end

    it 'rejects with more than one expire related option' do
      assert_command_results [
        [ 'SET 1 3 PX 1 EX 2', '-ERR syntax error' ],
        [ 'SET 1 3 PX 1 KEEPTTL', '-ERR syntax error' ],
        [ 'SET 1 3 KEEPTTL EX 2', '-ERR syntax error' ],
      ]
    end

    it 'rejects with both XX & NX' do
      assert_command_results [
        [ 'SET 1 3 NX XX', '-ERR syntax error' ],
      ]
    end
  end

  describe 'DEL' do
    it 'deletes existing keys' do
      assert_command_results [
        [ 'SET key value', '+OK' ],
        [ 'GET key', 'value' ],
        [ 'DEL key', ':1' ],
        [ 'GET key', BYORedis::NULL_BULK_STRING ],
        [ 'SET key-1 value', '+OK' ],
        [ 'SET key-2 value', '+OK' ],
        [ 'DEL key-1 key-2 not-a-key', ':2' ],
      ]
    end

    it 'returns 0 for a non existing key' do
      assert_command_results [
        [ 'DEL not-a-key', ':0' ],
      ]
    end
  end

  describe 'Unknown commands' do
    it 'returns an error message' do
      assert_command_results [
        [ 'NOT A COMMAND', '-ERR unknown command `NOT`, with args beginning with: `A`, `COMMAND`,' ],
      ]
    end
  end

  describe 'partial commands' do
    it 'accepts commands received through multiple reads' do
      assert_multipart_command_results [
        [ [ to_query('SET', 'first-key', 'first-value') ], '+OK' ],
        [ [ to_query('SET', 'second-key', 'second-value') ], '+OK' ],
        [ [ to_query('SET', 'third-key', 'third-value') ], '+OK' ],
        [ [ "*2\r\n$3\r\nGET\r\n", "$9\r\nfirst-key\r\n" ], 'first-value' ],
        [ [ "*2\r\n$3\r\nGET\r\n", "$10\r\nsecond-key\r\n*2" ], 'second-value' ],
        [ [ "\r\n$3\r\nGET\r\n$9\r\nthird-key\r\n" ], 'third-value' ],
      ]
    end

    it 'does not nothing if the command is incomplete' do
      assert_command_results [
        [ "*2\r\n$3\r\nGET\r\n$10\r\nincomple", nil ],
      ]
    end
  end

  describe 'protocol errors' do
    it 'returns a protocol error when expecting a bulk string and not reading the leading $' do
      assert_command_results [
        [ "*2\r\n$3\r\nGET\r\na-key\r\n", "-ERR Protocol error: expected '$', got 'a'" ],
      ]
    end

    it 'returns a protocol error when the length is invalid' do
      assert_command_results [
        [ "*1\r\n$foo\r\n", '-ERR Protocol error: invalid bulk length' ],
      ]
    end

    it 'returns a protocol when the array length is invalid' do
      assert_command_results [
        [ "*foo\r\n", '-ERR Protocol error: invalid multibulk length' ],
      ]
    end
  end

  describe 'pipelining' do
    it 'works with both inline & regular commands when starting with an inline command' do
      assert_multipart_command_results [
        [ [ "GET a\r\n*2\r\n$3\r\nGET\r\n$1\r\nb\r\n" ], "$-1\r\n$-1\r\n" ],
      ]
    end

    it 'works with both inline & regular commands when starting with a regular command' do
      assert_multipart_command_results [
        [ [ "*2\r\n$3\r\nGET\r\n$1\r\nb\r\nGET a\r\n" ], "$-1\r\n$-1\r\n" ],
      ]
    end
  end

  describe 'inline commands' do
    it 'accepts inline commands' do
      assert_command_results [
        [ "SET a-key a-value\r\n", '+OK' ],
        [ "GET a-key\r\n", "$7\r\na-value\r\n" ],
        [ "GET a-key\r", "$7\r\na-value\r\n" ],
        [ "GET a-key\n", "$7\r\na-value\r\n" ],
      ]
      assert_multipart_command_results [
        [ [ "SET a-key a-value\r\nSET ", 'another-key' ], '+OK' ],
        [ [ ' another-value', "\r\n" ], '+OK' ],
        [ [ "GET a-key\r\n" ], 'a-value' ],
        [ [ "GET another-key\r\n" ], 'another-value' ],
      ]
    end

    it 'rejects everything that is not a command and does not start with a *' do
      assert_command_results [
        [ "-a\r\n", '-ERR unknown command `-a`, with args beginning with: ' ],
      ]
    end
  end

  describe 'resizing' do
    it 'stores items after reaching the initial size' do
      assert_command_results [
        [ 'SET 1 2', '+OK' ],
        [ 'GET 1', '2' ],

        [ 'SET 3 4', '+OK' ],
        [ 'GET 3', '4' ],

        [ 'SET 5 6', '+OK' ],
        [ 'GET 5', '6' ],

        [ 'SET 7 8', '+OK' ],
        [ 'GET 7', '8' ],

        [ 'SET 9 10', '+OK' ],
        [ 'GET 9', '10' ],

        [ 'GET 1', '2' ],
        [ 'GET 3', '4' ],
        [ 'GET 5', '6' ],
        [ 'GET 7', '8' ],
        [ 'GET 9', '10' ],

        [ 'SET 11 12', '+OK' ],
        [ 'SET 13 14', '+OK' ],
        [ 'SET 14 16', '+OK' ],
        [ 'SET 17 18', '+OK' ],
        [ 'SET 19 20', '+OK' ],
      ]
    end
  end
end
