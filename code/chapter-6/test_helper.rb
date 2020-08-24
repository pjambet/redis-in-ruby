require 'minitest/autorun'
require 'timeout'
require 'stringio'

require_relative './server'

NULL_BULK_STRING = "$-1\r\n"

def connect_to_server
  socket = nil
  # The server might not be ready to listen to accepting connections by the time we try to connect from the main
  # thread, in the parent process. Using timeout here guarantees that we won't wait more than 1s, which should
  # more than enough time for the server to start, and the retry loop inside, will retry to connect every 10ms
  # until it succeeds
  Timeout.timeout(1) do
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

  server_socket = connect_to_server

  yield server_socket

ensure
  server_socket&.close
  if child
    kill_res = Process.kill('INT', child)
    begin
      Timeout.timeout(1) do
        Process.wait(child)
      end
    rescue Timeout::Error
      Process.kill('KILL', child)
    end
  end
end

# The arguments in an array of array of the form
# [
#   [ [ "COMMAND-PART-I", "COMMAND-PART-II", ... ], "EXPECTED_RESULT" ],
#   ...
# ]
def assert_multipart_command_results(multipart_command_result_pairs)
  with_server do |server_socket|
    multipart_command_result_pairs.each do |command, expected_result|
      command.each do |command_part|
        server_socket.write command_part
        # Sleep for one milliseconds to give a chance to the server to read
        # the first partial command
        sleep 0.001
      end

      response = read_response(server_socket)

      if response.length < expected_result.length
        # If the response we got is shorter, maybe we need to give the server a bit more time
        # to finish processing everything we wrote, so give it another shot
        sleep 0.1
        response += read_response(server_socket)
      end

      assert_response(expected_result, response)
    end
  end
end

def assert_command_results(command_result_pairs)
  with_server do |server_socket|
    command_result_pairs.each do |command, expected_result|
      if command.is_a?(String) && command.start_with?('sleep')
        sleep command.split[1].to_f
        next
      end
      command_string = if command.start_with?('*')
                         command
                       else
                         Redis::RESPArray.new(command.split).serialize
                       end
      server_socket.write command_string

      response = read_response(server_socket)

      assert_response(expected_result, response)
    end
  end
end

def assert_response(expected_result, response)
  assertion_match = expected_result&.match(/(\d+)\+\/-(\d+)/)
  if assertion_match
    response_match = response.match(/\A:(\d+)\r\n\z/)
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

def read_response(server_socket)
  response = ''
  loop do
    select_res = IO.select([ server_socket ], [], [], 0.1)
    last_response = server_socket.read_nonblock(1024, exception: false)
    if last_response == :wait_readable || last_response.nil? || select_res.nil?
      response = nil
      break
    else
      response += last_response
      break if response.length < 1024
    end
  end
  response&.force_encoding('utf-8')
end

def to_query(*command_parts)
  [ Redis::RESPArray.new(command_parts).serialize ]
end
