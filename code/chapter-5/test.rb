# coding: utf-8
require 'minitest/autorun'
require 'timeout'
require 'stringio'

require './server'

describe 'Redis::Server' do

  NULL_BULK_STRING = "$-1\r\n"

  def connect_to_server
    socket = nil
    # The server might not be ready to listen to accepting connections by the time we try to connect from the main
    # thread, in the parent process. Using timeout here guarantees that we won't wait more than 1s, which should
    # more than enough time for the server to start, and the retry loop inside, will retry to connect every 10ms
    # until it succeeds
    Timeout::timeout(1) do
      loop do
        begin
          socket = TCPSocket.new 'localhost', 2000
          break
        rescue
          sleep 0.01
        end
      end
    end
    socket
  end

  def with_server

    child = Process.fork do
      unless !!ENV['DEBUG']
        # We're effectively silencing the server with these two lines
        # stderr would have logged something when it receives SIGINT, with a complete stacktrace
        $stderr = StringIO.new
        # stdout would have logged the "Server started ..." & "New client connected ..." lines
        $stdout = StringIO.new
      end

      begin
        Redis::Server.new
      rescue Interrupt => e
        # Expected code path given we call kill with 'INT' below
      end
    end

    yield

  ensure
    if child
      kill_res = Process.kill('TERM', child)
      begin
        Timeout::timeout(1) do
          Process.wait(child)
        end
      rescue Timeout::Error
        Process.kill('KILL', child)
      end
    end
  end

  def assert_command_results(command_result_pairs)
    with_server do
      socket = connect_to_server
      command_result_pairs.each do |command, expected_result|
        if command.is_a?(String) && command.start_with?('sleep')
          sleep command.split[1].to_f
          next
        end
        begin
          if command.is_a?(Array)
            # The command is split between multiple sends
            command.each do |command_part|
              socket.write command_part
              # Sleep for one milliseconds to give a chance to the server to read
              # the first partial command
              sleep 0.001
            end
          else
            socket.write Redis::RESPArray.new(command.split).serialize
          end

          response = ""
          loop do
            select_res = IO.select([socket], [], [], 0.1)
            last_response = socket.read_nonblock(1024, exception: false)
            if last_response == :wait_readable || last_response.nil? || select_res.nil?
              response = nil
              break
            else
              response += last_response
              break if response.length < 1024
            end
          end
          response&.force_encoding('utf-8')
          # Matches "2000+\-10", aka 2000 plus or minus 10
          assertion_match = expected_result&.match /(\d+)\+\/-(\d+)/
          if assertion_match
            response_match = response.match /\A:(\d+)\r\n\z/
            assert response_match[0]
            assert_in_delta assertion_match[1].to_i, response_match[1].to_i, assertion_match[2].to_i
          else
            if expected_result && !%w(+ - : $ *).include?(expected_result[0])
              # Convert to a Bulk String unless it is a simple string (starts with a +)
              # or an error (starts with -)
              expected_result = Redis::RESPBulkString.new(expected_result).serialize
            end

            if expected_result && !expected_result.end_with?("\r\n")
              expected_result += "\r\n"
            end

            if expected_result.nil?
              assert_nil response
            else
              assert_equal expected_result, response
            end
          end
        end
      end
    ensure
      socket.close if socket
    end
  end

  describe 'when initialized' do
    it 'listens on port 2000' do
      with_server do
        # lsof stands for "list open files", see for more info https://stackoverflow.com/a/4421674
        lsof_result = `lsof -nP -i4TCP:2000 | grep LISTEN`
        assert_match "ruby", lsof_result
      end
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
        [ 'GET 1', '2'],
        [ 'SET 1 😂', '+OK' ],
        [ 'GET 1', '😂'],
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
        [ 'SET key value EX 2', '+OK'],
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
        [ 'SET key value EX 2', '+OK'],
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
        [ 'SET 1 3 EX foo', '-ERR value is not an integer or out of range']
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
        [ 'SET 1 3 PX foo', '-ERR value is not an integer or out of range']
      ]
    end

    it 'handles the NX option' do
      assert_command_results [
        [ 'SET 1 2 NX', '+OK' ],
        [ 'SET 1 2 NX', NULL_BULK_STRING ],
      ]
    end

    it 'handles the XX option' do
      assert_command_results [
        [ 'SET 1 2 XX', NULL_BULK_STRING],
        [ 'SET 1 2', '+OK'],
        [ 'SET 1 2 XX', '+OK'],
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
        [ 'SET 1 3 XX KEEPTTL', '+OK' ],
      ]
    end

    it 'rejects with more than one expire related option' do
      assert_command_results [
        [ 'SET 1 3 PX 1 EX 2', '-ERR syntax error'],
        [ 'SET 1 3 PX 1 KEEPTTL', '-ERR syntax error'],
        [ 'SET 1 3 KEEPTTL EX 2', '-ERR syntax error'],
      ]
    end

    it 'rejects with both XX & NX' do
      assert_command_results [
        [ 'SET 1 3 NX XX', '-ERR syntax error'],
      ]
    end
  end

  describe 'COMMAND' do
    it 'describes all the supported commands' do
      assert_command_results [
        [ 'COMMAND', "*1\r\n*7\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n*3\r\n+@read\r\n+@string\r\n+@fast\r\n" ],
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
        [ 'SET first-key first-value', '+OK'],
        [ 'SET second-key second-value', '+OK'],
        [ 'SET third-key third-value', '+OK'],
        [ [ "*2\r\n$3\r\nGET\r\n", "$9\r\nfirst-key\r\n" ], 'first-value' ],
        [ [ "*2\r\n$3\r\nGET\r\n", "$10\r\nsecond-key\r\n*2" ], 'second-value' ],
        [ [ "\r\n$3\r\nGET\r\n$9\r\nthird-key\r\n" ], 'third-value' ],
      ]
    end

    it 'does not nothing if the command is incomplete' do
      assert_command_results [
        [ [ "*2\r\n$3\r\nGET\r\n$10\r\nincomple" ], nil ]
      ]
    end
  end

  describe 'protocol errors' do
    it 'returns a protocol error when expecting a bulk string and not reading the leading $' do
      assert_command_results [
        [ [ "*2\r\n$3\r\nGET\r\na-key" ], "-ERR Protocol error: expected '$', got 'a'" ]
      ]
    end
  end

  describe 'inline commands' do
    it 'accepts inline commands' do
      assert_command_results [
        [ [ "SET a-key a-value\r\n" ], '+OK' ],
        [ [ "GET a-key\r\n" ], "$7\r\na-value\r\n" ],
      ]
    end

    it 'rejects everything that is not a command and does not start with a *' do
      assert_command_results [
        [ [ "-a\r\n" ], '-ERR unknown command `-a`, with args beginning with: ' ]
      ]
    end
  end
end
