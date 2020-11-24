require 'timeout'
require 'stringio'
require 'logger'

ENV['LOG_LEVEL'] = 'FATAL' unless ENV['LOG_LEVEL']

require_relative '../server'

$child_process_pid = nil
$socket_to_server = nil

def restart_server
  kill_child
  $child_process_pid = nil
  start_server
  $socket_to_server = nil
end

def start_server
  if $child_process_pid.nil?

    if !!ENV['DEBUG']
      options = {}
    else
      options = { [ :out, :err ] => '/dev/null' }
    end

    start_server_script = <<~RUBY
    begin
      BYORedis::Server.new
    rescue Interrupt
    end
    RUBY

    $child_process_pid =
      Process.spawn('ruby', '-r', './server', '-e', start_server_script, options)
  end
end

# Make sure that we stop the server if tests are interrupted with Ctrl-C
Signal.trap('INT') do
  kill_child
  exit(0)
end

require 'minitest/autorun'

def do_teardown
  with_server do |socket|
    socket.write(to_query('FLUSHDB'))
    read_response(socket)
    args = BYORedis::Config::DEFAULT.flat_map do |key, value|
      [ key.to_s, value.to_s ]
    end
    socket.write(to_query('CONFIG', 'SET', *args))
    read_response(socket)
  end
end

class MiniTest::Test
  def teardown
    return unless $child_process_pid

    with_server do
      do_teardown
    end
  rescue Errno::EPIPE, IOError => e
    $socket_to_server&.close
    $socket_to_server = nil
    connect_to_server
    do_teardown
    p "Exception during teardown: #{ e.class }/ #{ e }"
  end
end

def kill_child
  if $child_process_pid
    Process.kill('INT', $child_process_pid)
    begin
      Timeout.timeout(1) do
        Process.wait($child_process_pid)
      end
    rescue Timeout::Error
      Process.kill('KILL', $child_process_pid)
    end
  end
rescue Errno::ESRCH
  # There was no process
ensure
  if $socket_to_server
    $socket_to_server.close
    $socket_to_server = nil
  end
end

MiniTest.after_run do
  kill_child
end

def connect_to_server
  start_server if $child_process_pid.nil?

  return $socket_to_server if !$socket_to_server.nil? && !$socket_to_server.closed?

  # The server might not be ready to listen to accepting connections by the time we try to
  # connect from the main thread, in the parent process. Using timeout here guarantees that we
  # won't wait more than 1s, which should more than enough time for the server to start, and the
  # retry loop inside, will retry to connect every 10ms until it succeeds
  connect_with_timeout
rescue Timeout::Error
  # If we failed to connect, there's a chance that it's because the previous test crashed the
  # server, so retry once
  p "Restarting server because of timeout when connecting"
  restart_server
  connect_with_timeout
end

def connect_with_timeout
  Timeout.timeout(1) do
    loop do
      begin
        $socket_to_server = TCPSocket.new 'localhost', 2000
        break
      rescue StandardError
        $socket_to_server = nil
        sleep 0.2
      end
    end
  end
  $socket_to_server
end

def with_server
  server_socket = connect_to_server

  yield server_socket

  server_socket.close
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
                         BYORedis::RESPArray.new(command.split).serialize
                       end
      server_socket.write command_string

      response = read_response(server_socket)

      assert_response(expected_result, response)
    end
  end
end

def assert_response(expected_result, response)
  assertion_match = expected_result&.match(/(\d+)\+\/-(\d+)/) if expected_result.is_a?(String)
  if assertion_match
    response_match = response.match(/\A:(\d+)\r\n\z/)
    assert response_match[0]
    assert_in_delta assertion_match[1].to_i, response_match[1].to_i, assertion_match[2].to_i
  else
    if expected_result&.is_a?(Array)
      expected_result = BYORedis::RESPArray.new(expected_result).serialize
    elsif expected_result&.is_a?(UnorderedArray)
      expected_result = BYORedis::RESPArray.new(expected_result.array.sort).serialize
      response = response.then do |r|
        parts = r.split
        response_size = parts.shift
        sorted_parts = parts.each_slice(2).sort_by { |p|  p[1]  }
        sorted_parts.flatten.prepend(response_size).map { |p| p << "\r\n" }.join
      end
    elsif expected_result&.is_a?(OneOf)
      expected = expected_result.array.map do |r|
        if r.start_with?(':')
          r + "\r\n"
        else
          BYORedis::RESPBulkString.new(r).serialize
        end
      end
      assert_includes(expected, response)
      return
    elsif expected_result && !%w(+ - : $ *).include?(expected_result[0])
      # Convert to a Bulk String unless it is a simple string (starts with a +)
      # or an error (starts with -)
      expected_result = BYORedis::RESPBulkString.new(expected_result).serialize
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

def read_response(server_socket, read_timeout: 0.2)
  response = nil
  loop do
    select_res = IO.select([ server_socket ], [], [], read_timeout)
    last_response = server_socket.read_nonblock(1024, exception: false)
    if last_response == :wait_readable || last_response.nil? || select_res.nil?
      break
    else
      if response.nil?
        response = last_response
      else
        response += last_response
      end
      break if response.length < 1024
    end
  end
  response&.force_encoding('utf-8')
rescue Errno::ECONNRESET
  response&.force_encoding('utf-8')
end

def to_query(*command_parts)
  BYORedis::RESPArray.new(command_parts).serialize
end

UnorderedArray = Struct.new(:array)

def unordered(array)
  UnorderedArray.new(array)
end

OneOf = Struct.new(:array)

def one_of(array)
  OneOf.new(array)
end

def test_with_config_values(combinations)
  # This line goes from a hash like:
  # { config_1: [ 'config_1_value_1', 'config_2_value_2' ],
  #   config_2: [ 'config_2_value_1', 'config_2_value_2' ] }
  # to:
  # [ [ [:config_1, "config_1_value_1"], [:config_1, "config_2_value_2"] ],
  #   [ [:config_2, "config_2_value_1"], [:config_2, "config_2_value_2"] ] ]
  config_pairs = combinations.map { |key, values| values.map { |value| [ key, value ] } }

  # This line combines all the config values into an array of all combinations:
  # [ [ [ :config_1, "config_1_value_1"], [:config_2, "config_2_value_1" ] ],
  #   [ [ :config_1, "config_1_value_1"], [:config_2, "config_2_value_2" ] ],
  #   [ [ :config_1, "config_2_value_2"], [:config_2, "config_2_value_1" ] ],
  #   [ [ :config_1, "config_2_value_2"], [:config_2, "config_2_value_2" ] ] ]
  all_combinations = config_pairs[0].product(*config_pairs[1..-1])

  # And finally, using the Hash.[] method, we create an array of hashes and obtain:
  #  [ { :config_1=>"config_1_value_1", :config_2=>"config_2_value_1" },
  #    { :config_1=>"config_1_value_1", :config_2=>"config_2_value_2" },
  #    { :config_1=>"config_2_value_2", :config_2=>"config_2_value_1" },
  #    { :config_1=>"config_2_value_2", :config_2=>"config_2_value_2" } ]
  all_combination_hashes = all_combinations.map { |pairs| Hash[pairs] }

  all_combination_hashes.each do |config_hash|
    with_server do |socket|
      socket.write(to_query('FLUSHDB'))
      resp = read_response(socket)
      assert_equal("+OK\r\n", resp)

      config_parts = config_hash.flat_map { |key, value| [ key.to_s, value.to_s ] }
      socket.write(to_query('CONFIG', 'SET', *config_parts))
      resp = read_response(socket)
      assert_equal("+OK\r\n", resp)
    end

    yield
  end
end
