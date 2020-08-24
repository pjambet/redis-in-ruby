require 'socket'
require 'timeout'
require 'logger'
require 'strscan'

LOG_LEVEL = ENV['DEBUG'] ? Logger::DEBUG : Logger::INFO
$random_bytes = Random.bytes(16)

require_relative './dict'
require_relative './types'
require_relative './expire_helper'
require_relative './get_command'
require_relative './set_command'
require_relative './ttl_command'
require_relative './pttl_command'
require_relative './command_command'


module Redis

  class Server

    COMMANDS = Dict.new($random_bytes)
    COMMANDS.add('command', CommandCommand)
    COMMANDS.add('get', GetCommand)
    COMMANDS.add('set', SetCommand)
    COMMANDS.add('ttl', TtlCommand)
    COMMANDS.add('pttl', PttlCommand)

    MAX_EXPIRE_LOOKUPS_PER_CYCLE = 20
    DEFAULT_FREQUENCY = 10 # How many times server_cron runs per second

    IncompleteCommand = Class.new(StandardError)
    ProtocolError = Class.new(StandardError) do
      def serialize
        RESPError.new(message).serialize
      end
    end

    TimeEvent = Struct.new(:process_at, :block)
    Client = Struct.new(:socket, :buffer) do
      def initialize(socket)
        self.socket = socket
        self.buffer = ''
      end
    end

    def initialize
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL

      @clients = []
      @data_store = Dict.new($random_bytes)
      @expires = Dict.new($random_bytes)

      @server = TCPServer.new 2000
      @time_events = []
      @logger.debug "Server started at: #{ Time.now }"
      add_time_event(Time.now.to_f.truncate + 1) do
        server_cron
      end

      start_event_loop
    end

    private

    def add_time_event(process_at, &block)
      @time_events << TimeEvent.new(process_at, block)
    end

    def nearest_time_event
      now = (Time.now.to_f * 1000).truncate
      nearest = nil
      @time_events.each do |time_event|
        if nearest.nil?
          nearest = time_event
        elsif time_event.process_at < nearest.process_at
          nearest = time_event
        else
          next
        end
      end

      nearest
    end

    def select_timeout
      if @time_events.any?
        nearest = nearest_time_event
        now = (Time.now.to_f * 1000).truncate
        if nearest.process_at < now
          0
        else
          (nearest.process_at - now) / 1000.0
        end
      else
        0
      end
    end

    def client_sockets
      @clients.map(&:socket)
    end

    def start_event_loop
      loop do
        timeout = select_timeout
        @logger.debug "select with a timeout of #{ timeout }"
        result = IO.select(client_sockets + [@server], [], [], timeout)
        sockets = result ? result[0] : []
        process_poll_events(sockets)
        process_time_events
      end
    end

    def process_poll_events(sockets)
      sockets.each do |socket|
        begin
          if socket.is_a?(TCPServer)
            @clients << Client.new(@server.accept)
          elsif socket.is_a?(TCPSocket)
            client = @clients.find { |client| client.socket == socket }
            client_command_with_args = socket.read_nonblock(1024, exception: false)
            if client_command_with_args.nil?
              @clients.delete(client)
              socket.close
            elsif client_command_with_args == :wait_readable
              # There's nothing to read from the client, we don't have to do anything
              next
            elsif client_command_with_args.empty?
              @logger.debug "Empty request received from #{ socket }"
            else
              client.buffer += client_command_with_args
              split_commands(client.buffer) do |command_parts|
                response = handle_client_command(command_parts)
                @logger.debug "Response: #{ response.class } / #{ response.inspect }"
                @logger.debug "Writing: '#{ response.serialize.inspect }'"
                socket.write response.serialize
              end
            end
          else
            raise "Unknown socket type: #{ socket }"
          end
        rescue Errno::ECONNRESET
          @clients.delete_if { |client| client.socket == socket }
        rescue IncompleteCommand
          # Not clearing the buffer or anything
          next
        rescue ProtocolError => e
          socket.write e.serialize
          socket.close
          @clients.delete(client)
        end
      end
    end

    def split_commands(client_buffer)
      @logger.debug "Full result from read: '#{ client_buffer.inspect }'"

      scanner = StringScanner.new(client_buffer.dup)
      until scanner.eos?
        if scanner.peek(1) == '*'
          yield parse_as_resp_array(client_buffer, scanner)
        else
          yield parse_as_inline_command(client_buffer, scanner)
        end
        client_buffer.slice!(0, scanner.charpos)
      end
    end

    def parse_as_resp_array(client_buffer, scanner)
      command_parts = parse_value_from_string(scanner)
      unless command_parts.is_a?(Array)
        client_buffer.slice!(0, scanner.charpos)
        raise ProtocolError, 'ERR Protocol Error: not an array'
      end

      command_parts
    end

    def parse_as_inline_command(client_buffer, scanner)
      command = scanner.scan_until(/\r\n/)
      if command.nil?
        # client_buffer.slice!(0, scanner.charpos)
        raise IncompleteCommand
      end

      command.split.map(&:strip)
    end

    # We're not parsing integers, errors or simple strings since none of the implemented
    # commands use these data types
    def parse_value_from_string(scanner)
      type_char = scanner.getch
      case type_char
      when '$'
        expected_length = scanner.scan_until(/\r\n/)
        raise IncompleteCommand if expected_length.nil?

        expected_length = expected_length.to_i
        # Redis does not error on length == 0
        raise ProtocolError, 'ERR Protocol error: invalid bulk length' if expected_length <= 0

        bulk_string = scanner.rest.slice(0, expected_length)

        raise IncompleteCommand if bulk_string.nil? || bulk_string.length != expected_length

        scanner.pos += bulk_string.bytesize + 2
        bulk_string
      when '*'
        expected_length = scanner.scan_until(/\r\n/)
        raise IncompleteCommand if expected_length.nil?

        expected_length = expected_length.to_i
        # Redis does not return for zero or less array lengths
        raise ProtocolError, 'ERR Protocol error: invalid array length' if expected_length < 0

        array_result = []

        expected_length.times do
          raise IncompleteCommand if scanner.eos?

          parsed_value = parse_value_from_string(scanner)
          raise IncompleteCommand if parsed_value.nil?

          array_result << parsed_value
        end

        array_result
      else
        raise ProtocolError, "ERR Protocol error: expected '$', got '#{ type_char }'"
      end
    end

    def process_time_events
      @time_events.delete_if do |time_event|
        next if time_event.process_at > Time.now.to_f * 1000

        return_value = time_event.block.call

        if return_value.nil?
          true
        else
          time_event.process_at = (Time.now.to_f * 1000).truncate + return_value
          @logger.debug "Rescheduling time event #{ Time.at(time_event.process_at / 1000.0).to_f }"
          false
        end
      end
    end

    def handle_client_command(command_parts)
      @logger.debug "Received command: #{ command_parts }"
      command_str = command_parts[0]
      args = command_parts[1..-1]

      command_class = COMMANDS[command_str.downcase]

      if command_class
        command = command_class.new(@data_store, @expires, args)
        command.call
      else
        formatted_args = args.map { |arg| "`#{ arg }`," }.join(' ')
        message = "ERR unknown command `#{ command_str }`, with args beginning with: #{ formatted_args }"
        RESPError.new(message)
      end
    end

    def server_cron
      start_timestamp = Time.now
      keys_fetched = 0

      @expires.each do |key, _|
        if @expires[key] < Time.now.to_f * 1000
          @logger.debug "Evicting #{ key }"
          @expires.delete(key)
          @data_store.delete(key)
        end

        keys_fetched += 1
        if keys_fetched >= MAX_EXPIRE_LOOKUPS_PER_CYCLE
          break
        end
      end

      end_timestamp = Time.now
      @logger.debug do
        format(
          'Processed %<number_of_keys>i keys in %<duration>.3f ms',
          number_of_keys: keys_fetched,
          duration: (end_timestamp - start_timestamp) * 1000,
        )
      end

      databases_cron

      1000 / DEFAULT_FREQUENCY
    end

    def databases_cron
      # try_resize
      @data_store.resize if ht_needs_resize(@data_store)
      @expires.resize if ht_needs_resize(@expires)

      # incrementally_rehash
      @data_store.rehash_milliseconds(1)
      @expires.rehash_milliseconds(1)
    end

    def slots(dict)
      dict.hash_tables[0].size + dict.hash_tables[1].size
    end

    def size(dict)
      dict.hash_tables[0].used + dict.hash_tables[1].used
    end

    def ht_needs_resize(dict)
      # See https://github.com/antirez/redis/blob/6.0/src/server.c#L1422
      size = slots(dict)
      used = size(dict)

      # TODO: Move to a constant
      size > Dict::INITIAL_SIZE && ((used * 100) / size < 10)
    end
  end
end
