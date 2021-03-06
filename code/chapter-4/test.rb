require 'minitest/autorun'
require 'timeout'
require 'stringio'
require './server'

describe 'RedisServer' do

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
        RedisServer.new
      rescue Interrupt => e
        # Expected code path given we call kill with 'INT' below
      end
    end

    yield

  ensure
    if child
      Process.kill('INT', child)
      Process.wait(child)
    end
  end

  def assert_command_results(command_result_pairs)
    with_server do
      command_result_pairs.each do |command, expected_result|
        if command.start_with?('sleep')
          sleep command.split[1].to_f
          next
        end
        begin
          socket = connect_to_server
          socket.puts command
          response = socket.gets
          # Matches "2000+\-10", aka 2000 plus or minus 10
          regexp_match = expected_result.match /(\d+)\+\/-(\d+)/
          if regexp_match
            # The result is a range
            assert_in_delta regexp_match[1].to_i, response.to_i, regexp_match[2].to_i
          else
            assert_equal expected_result + "\n", response
          end
        ensure
          socket.close if socket
        end
      end
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
        [ 'GET', '(error) ERR wrong number of arguments for \'GET\' command' ],
      ]
    end

    it 'returns (nil) for unknown keys' do
      assert_command_results [
        [ 'GET 1', '(nil)' ],
      ]
    end

    it 'returns the value previously set by SET' do
      assert_command_results [
        [ 'SET 1 2', 'OK' ],
        [ 'GET 1', '2']
      ]
    end
  end

  describe 'TTL' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'TTL', '(error) ERR wrong number of arguments for \'TTL\' command' ],
      ]
    end

    it 'returns the TTL for a key with a TTL' do
      assert_command_results [
        [ 'SET key value EX 2', 'OK'],
        [ 'TTL key', '2' ],
        [ 'sleep 0.5' ],
        [ 'TTL key', '1' ],
      ]
    end

    it 'returns -1 for a key without a TTL' do
      assert_command_results [
        [ 'SET key value', 'OK' ],
        [ 'TTL key', '-1' ],
      ]
    end

    it 'returns -2 if the key does not exist' do
      assert_command_results [
        [ 'TTL key', '-2' ],
      ]
    end
  end

  describe 'PTTL' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'PTTL', '(error) ERR wrong number of arguments for \'PTTL\' command' ],
      ]
    end

    it 'returns the TTL in ms for a key with a TTL' do
      assert_command_results [
        [ 'SET key value EX 2', 'OK'],
        [ 'PTTL key', '2000+/-20' ], # Initial 2000ms +/- 20ms
        [ 'sleep 0.5' ],
        [ 'PTTL key', '1500+/-20' ], # Initial 2000ms, minus ~500ms of sleep, +/- 20ms
      ]
    end

    it 'returns -1 for a key without a TTL' do
      assert_command_results [
        [ 'SET key value', 'OK' ],
        [ 'PTTL key', '-1' ],
      ]
    end

    it 'returns -2 if the key does not exist' do
      assert_command_results [
        [ 'PTTL key', '-2' ],
      ]
    end
  end

  describe 'SET' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'SET', '(error) ERR wrong number of arguments for \'SET\' command' ],
      ]
    end

    it 'returns OK' do
      assert_command_results [
        [ 'SET 1 3', 'OK' ],
      ]
    end

    it 'handles the EX option with a valid argument' do
      assert_command_results [
        [ 'SET 1 3 EX 1', 'OK' ],
        [ 'GET 1', '3' ],
        [ 'sleep 1' ],
        [ 'GET 1', '(nil)' ],
      ]
    end

    it 'rejects the EX option with an invalid argument' do
      assert_command_results [
        [ 'SET 1 3 EX foo', '(error) ERR value is not an integer or out of range']
      ]
    end

    it 'handles the PX option with a valid argument' do
      assert_command_results [
        [ 'SET 1 3 PX 100', 'OK' ],
        [ 'GET 1', '3' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', '(nil)' ],
      ]
    end

    it 'rejects the PX option with an invalid argument' do
      assert_command_results [
        [ 'SET 1 3 PX foo', '(error) ERR value is not an integer or out of range']
      ]
    end

    it 'handles the NX option' do
      assert_command_results [
        [ 'SET 1 2 NX', 'OK' ],
        [ 'SET 1 2 NX', '(nil)' ],
      ]
    end

    it 'handles the XX option' do
      assert_command_results [
        [ 'SET 1 2 XX', '(nil)'],
        [ 'SET 1 2', 'OK'],
        [ 'SET 1 2 XX', 'OK'],
      ]
    end

    it 'removes ttl without KEEPTTL' do
      assert_command_results [
        [ 'SET 1 3 PX 100', 'OK' ],
        [ 'SET 1 2', 'OK' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', '2' ],
      ]
    end

    it 'handles the KEEPTTL option' do
      assert_command_results [
        [ 'SET 1 3 PX 100', 'OK' ],
        [ 'SET 1 2 KEEPTTL', 'OK' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', '(nil)' ],
      ]
    end

    it 'accepts multiple options' do
      assert_command_results [
        [ 'SET 1 3 NX EX 1', 'OK' ],
        [ 'GET 1', '3' ],
        [ 'SET 1 3 XX KEEPTTL', 'OK' ],
      ]
    end

    it 'rejects with more than one expire related option' do
      assert_command_results [
        [ 'SET 1 3 PX 1 EX 2', '(error) ERR syntax error'],
        [ 'SET 1 3 PX 1 KEEPTTL', '(error) ERR syntax error'],
        [ 'SET 1 3 KEEPTTL EX 2', '(error) ERR syntax error'],
      ]
    end

    it 'rejects with both XX & NX' do
      assert_command_results [
        [ 'SET 1 3 NX XX', '(error) ERR syntax error'],
      ]
    end
  end

  describe 'Unknown commands' do
    it 'returns an error message' do
      assert_command_results [
        [ 'NOT A COMMAND', '(error) ERR unknown command `NOT`, with args beginning with: `A`, `COMMAND`,' ],
      ]
    end
  end
end
