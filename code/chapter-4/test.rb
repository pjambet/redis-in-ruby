require 'minitest/autorun'
require 'timeout'
require 'stringio'
require './server'

describe 'BasicServer' do

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

  def with_server(debug: false)

    child = Process.fork do
      unless debug
        # We're effectively silencing the server with these two lines
        # stderr would have logged something when it receives SIGINT, with a complete stacktrace
        $stderr = StringIO.new
        # stdout would have logged the "Server started ..." & "New client connected ..." lines
        $stdout = StringIO.new
      end
      BasicServer.new
    end

    yield

  ensure
    if child
      Process.kill('INT', child)
      Process.wait(child)
    end
  end

  def assert_command_results(command_result_pairs)
    with_server(debug: !!ENV['DEBUG']) do
      command_result_pairs.each do |command, expected_result|
        if command.start_with?('sleep')
          sleep command.split[1].to_i
          next
        end
        begin
          socket = connect_to_server
          socket.puts command
          response = socket.gets
          assert_equal expected_result + "\n", response
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

    it 'handles the PX option with a valid argument'
    it 'rejects the PX option with an invalid argument'

    it 'handles the NX option' do
      assert_command_results [
        [ 'SET 1 2 NX', 'OK' ],
        [ 'SET 1 2 NX', '(nil)' ],
      ]
    end

    it 'handles the XX option'

    it 'removes ttl without KEEPTTL' do
      assert_command_results [
        [ 'SET 1 3 EX 1', 'OK' ],
        [ 'SET 1 2', 'OK' ],
        [ 'sleep 1' ],
        [ 'GET 1', '2' ],
      ]
    end

    it 'handles the KEEPTTL option' do
      assert_command_results [
        [ 'SET 1 3 EX 1', 'OK' ],
        [ 'SET 1 2 KEEPTTL', 'OK' ],
        [ 'sleep 1' ],
        [ 'GET 1', '(nil)' ],
      ]
    end
    it 'accepts multiple options'
    it 'rejects with both PX & EX'
    it 'rejects with both XX & NX'
  end

  describe 'Unknown commands' do
    it 'returns an error message' do
      assert_command_results [
        [ 'NOT A COMMAND', '(error) ERR unknown command `NOT`, with args beginning with: `A`, `COMMAND`,' ],
      ]
    end
  end
end
