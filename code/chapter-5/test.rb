# coding: utf-8

require_relative './test_helper'

describe 'Redis::Server' do
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
        [ 'gEt 1', NULL_BULK_STRING ],
        [ 'set 1 2', '+OK' ],
        [ 'get 1', '2' ],
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
        [ 'GET 1', NULL_BULK_STRING ],
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

    it 'handles the EX option with a valid argument' do
      assert_command_results [
        [ 'SET 1 3 EX 1', '+OK' ],
        [ 'GET 1', '3' ],
        [ 'sleep 1' ],
        [ 'GET 1', NULL_BULK_STRING ],
      ]
    end

    it 'rejects the EX option with an invalid argument' do
      assert_command_results [
        [ 'SET 1 3 EX foo', '-ERR value is not an integer or out of range' ]
      ]
    end

    it 'handles the PX option with a valid argument' do
      assert_command_results [
        [ 'SET 1 3 PX 100', '+OK' ],
        [ 'GET 1', '3' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', NULL_BULK_STRING ],
      ]
    end

    it 'rejects the PX option with an invalid argument' do
      assert_command_results [
        [ 'SET 1 3 px foo', '-ERR value is not an integer or out of range' ]
      ]
    end

    it 'handles the NX option' do
      assert_command_results [
        [ 'SET 1 2 nX', '+OK' ],
        [ 'SET 1 2 NX', NULL_BULK_STRING ],
      ]
    end

    it 'handles the XX option' do
      assert_command_results [
        [ 'SET 1 2 XX', NULL_BULK_STRING ],
        [ 'SET 1 2', '+OK' ],
        [ 'SET 1 2 XX', '+OK' ],
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
        [ 'GET 1', NULL_BULK_STRING ],
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

  describe 'COMMAND' do
    it 'describes all the supported commands' do
      assert_command_results [
        # Results from: echo "COMMAND INFO COMMAND GET SET TTL PTTL" | nc -c -v localhost 6379
        [ 'COMMAND', "*5\r\n*7\r\n$7\r\ncommand\r\n:-1\r\n*3\r\n+random\r\n+loading\r\n+stale\r\n:0\r\n:0\r\n:0\r\n*2\r\n+@slow\r\n+@connection\r\n*7\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n*3\r\n+@read\r\n+@string\r\n+@fast\r\n*7\r\n$3\r\nset\r\n:-3\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n*3\r\n+@write\r\n+@string\r\n+@slow\r\n*7\r\n$3\r\nttl\r\n:2\r\n*3\r\n+readonly\r\n+random\r\n+fast\r\n:1\r\n:1\r\n:1\r\n*3\r\n+@keyspace\r\n+@read\r\n+@fast\r\n*7\r\n$4\r\npttl\r\n:2\r\n*3\r\n+readonly\r\n+random\r\n+fast\r\n:1\r\n:1\r\n:1\r\n*3\r\n+@keyspace\r\n+@read\r\n+@fast\r\n" ],
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
      assert_command_results [
        [ 'SET first-key first-value', '+OK' ],
        [ 'SET second-key second-value', '+OK' ],
        [ 'SET third-key third-value', '+OK' ],
      ]
      assert_multipart_command_results [
        [ to_query('SET', 'first-key', 'first-value'), '+OK' ],
        [ to_query('SET', 'second-key', 'second-value'), '+OK' ],
        [ to_query('SET', 'third-key', 'third-value'), '+OK' ],
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
  end

  describe 'inline commands' do
    it 'accepts inline commands' do
      assert_command_results [
        [ "SET a-key a-value\r\n", '+OK' ],
        [ "GET a-key\r\n", "$7\r\na-value\r\n" ],
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
end
