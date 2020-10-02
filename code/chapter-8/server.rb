require 'socket'
require 'logger'
require 'strscan'
require 'securerandom'

LOG_LEVEL = if ENV['DEBUG']
              Logger::DEBUG
            elsif ENV['LOG_LEVEL']
              # Will crash for unknown log levels, on purpose
              Logger.const_get(ENV['LOG_LEVEL'].upcase)
            else
              Logger::INFO
            end
RANDOM_BYTES = SecureRandom.bytes(16)

require_relative './db'
require_relative './dict'
require_relative './option_utils'
require_relative './utils'
require_relative './sorted_array'
require_relative './base_command'
require_relative './blocked_client_handler'
require_relative './resp_types'
require_relative './expire_helper'
require_relative './del_command'
require_relative './get_command'
require_relative './set_command'
require_relative './ttl_command'
require_relative './pttl_command'
require_relative './list_commands'
require_relative './type_command'
require_relative './command_command'

module BYORedis

  class Server

    COMMANDS = Dict.new
    COMMANDS.set('command', CommandCommand)
    COMMANDS.set('del', DelCommand)
    COMMANDS.set('get', GetCommand)
    COMMANDS.set('set', SetCommand)
    COMMANDS.set('ttl', TtlCommand)
    COMMANDS.set('pttl', PttlCommand)
    COMMANDS.set('lrange', LRangeCommand)
    COMMANDS.set('lpush', LPushCommand)
    COMMANDS.set('lpushx', LPushXCommand)
    COMMANDS.set('rpush', RPushCommand)
    COMMANDS.set('rpushx', RPushXCommand)
    COMMANDS.set('llen', LLenCommand)
    COMMANDS.set('lpop', LPopCommand)
    COMMANDS.set('blpop', BLPopCommand)
    COMMANDS.set('rpop', RPopCommand)
    COMMANDS.set('brpop', BRPopCommand)
    COMMANDS.set('rpoplpush', RPopLPushCommand)
    COMMANDS.set('brpoplpush', BRPopLPushCommand)
    COMMANDS.set('ltrim', LTrimCommand)
    COMMANDS.set('lset', LSetCommand)
    COMMANDS.set('lrem', LRemCommand)
    COMMANDS.set('lpos', LPosCommand)
    COMMANDS.set('linsert', LInsertCommand)
    COMMANDS.set('lindex', LIndexCommand)
    COMMANDS.set('type', TypeCommand)

    MAX_EXPIRE_LOOKUPS_PER_CYCLE = 20
    DEFAULT_FREQUENCY = 10 # How many times server_cron runs per second
    HASHTABLE_MIN_FILL = 10

    IncompleteCommand = Class.new(StandardError)
    ProtocolError = Class.new(StandardError) do
      def serialize
        RESPError.new(message).serialize
      end
    end

    TimeEvent = Struct.new(:process_at, :block)
    Client = Struct.new(:socket, :buffer, :blocked_state) do
      attr_reader :id

      def initialize(socket)
        @id = socket.fileno.to_s
        self.socket = socket
        self.buffer = ''
      end
    end

    BlockedState = Struct.new(:timeout, :keys, :operation, :target, :client)

    def initialize
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL

      @clients = Dict.new
      @db = DB.new
      @blocked_client_handler = BlockedClientHandler.new(self, @db)
      @server = TCPServer.new 2000
      @time_events = []
      @logger.debug "Server started at: #{ Time.now }"
      add_time_event(Time.now.to_f.truncate + 1) do
        server_cron
      end

      start_event_loop
    end

    def disconnect_client(client)
      @clients.delete(client.id)

      if client.blocked_state
        @db.client_timeouts.delete(client.blocked_state)

        client.blocked_state.keys.each do |key|
          list = @db.blocking_keys[key]
          if list
            list.remove(1, client)
            @db.blocking_keys.delete(key) if list.empty?
          end
        end
      end

      client.socket.close
    end

    private

    def add_time_event(process_at, &block)
      @time_events << TimeEvent.new(process_at, block)
    end

    def nearest_time_event
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
      sockets = []
      @clients.each { |_, client| sockets << client.socket }
      sockets
    end

    def start_event_loop
      loop do
        handle_blocked_clients_timeout
        process_unblocked_clients

        timeout = select_timeout
        @logger.debug "select with a timeout of #{ timeout }"
        result = IO.select(client_sockets + [ @server ], [], [], timeout)
        sockets = result ? result[0] : []
        process_poll_events(sockets)
        process_time_events
      end
    end

    def safe_accept_client
      @server.accept
    rescue Errno::ECONNRESET, Errno::EPIPE => e
      @logger.warn "Error when accepting client: #{ e }"
      nil
    end

    def safe_read(client)
      client.socket.read_nonblock(1024, exception: false)
    rescue Errno::ECONNRESET, Errno::EPIPE
      disconnect_client(client)
    end

    def process_poll_events(sockets)
      sockets.each do |socket|
        if socket.is_a?(TCPServer)
          socket = safe_accept_client
          next unless socket

          @clients[socket.fileno.to_s] = Client.new(socket)
        elsif socket.is_a?(TCPSocket)
          client = @clients[socket.fileno.to_s]
          client_command_with_args = safe_read(client)

          if client_command_with_args.nil?
            disconnect_client(client)
          elsif client_command_with_args == :wait_readable
            # There's nothing to read from the client, we don't have to do anything
            next
          elsif client_command_with_args.empty?
            @logger.debug "Empty request received from #{ socket }"
          else
            client.buffer += client_command_with_args

            process_client_buffer(client)
          end
        else
          raise "Unknown socket type: #{ socket }"
        end
      end
    end

    def process_client_buffer(client)
      split_commands(client.buffer) do |command_parts|
        return if client.blocked_state

        response = handle_client_command(command_parts)
        if response.is_a?(BlockedState)
          block_client(client, response)
        else
          @logger.debug "Response: #{ response.class } / #{ response.inspect }"
          serialized_response = response.serialize
          @logger.debug "Writing: '#{ serialized_response.inspect }'"
          unless Utils.safe_write(client.socket, serialized_response)
            disconnect_client(client)
          end

          handle_clients_blocked_on_keys
        end
      end
    rescue IncompleteCommand
      # Not clearing the buffer or anything
    rescue ProtocolError => e
      client.socket.write e.serialize
      disconnect_client(client)
    end

    def split_commands(client_buffer)
      @logger.debug "Client buffer content: '#{ client_buffer.inspect }'"

      scanner = StringScanner.new(client_buffer.dup)
      until scanner.eos?
        if scanner.peek(1) == '*'
          yield parse_as_resp_array(scanner)
        else
          yield parse_as_inline_command(scanner)
        end
        client_buffer.slice!(0, scanner.charpos)
      end
    end

    def parse_as_resp_array(scanner)
      unless scanner.getch == '*'
        raise 'Unexpectedly attempted to parse a non array as an array'
      end

      expected_length = scanner.scan_until(/\r\n/)
      raise IncompleteCommand if expected_length.nil?

      expected_length = parse_integer(expected_length, 'invalid multibulk length')
      command_parts = []

      expected_length.times do
        raise IncompleteCommand if scanner.eos?

        parsed_value = parse_as_resp_bulk_string(scanner)
        raise IncompleteCommand if parsed_value.nil?

        command_parts << parsed_value
      end

      command_parts
    end

    def parse_as_resp_bulk_string(scanner)
      type_char = scanner.getch
      unless type_char == '$'
        raise ProtocolError, "ERR Protocol error: expected '$', got '#{ type_char }'"
      end

      expected_length = scanner.scan_until(/\r\n/)
      raise IncompleteCommand if expected_length.nil?

      expected_length = parse_integer(expected_length, 'invalid bulk length')
      bulk_string = scanner.rest.slice(0, expected_length)

      raise IncompleteCommand if bulk_string.nil? || bulk_string.length != expected_length

      scanner.pos += bulk_string.bytesize + 2
      bulk_string
    end

    def parse_as_inline_command(scanner)
      command = scanner.scan_until(/(\r\n|\r|\n)+/)
      raise IncompleteCommand if command.nil?

      command.split.map(&:strip)
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
        command = command_class.new(@db, args)
        command.execute_command
      else
        formatted_args = args.map { |arg| "`#{ arg }`," }.join(' ')
        message = "ERR unknown command `#{ command_str }`, with args beginning with: #{ formatted_args }"
        RESPError.new(message)
      end
    end

    def server_cron
      start_timestamp = Time.now
      keys_fetched = 0

      @db.expires.each do |key, _|
        if @db.expires[key] < Time.now.to_f * 1000
          @logger.debug "Evicting #{ key }"
          @db.expires.delete(key)
          @db.data_store.delete(key)
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
      @db.data_store.resize if ht_needs_resize(@db.data_store)
      @db.expires.resize if ht_needs_resize(@db.expires)

      @db.data_store.rehash_milliseconds(1)
      @db.expires.rehash_milliseconds(1)
    end

    def slots(dict)
      dict.hash_tables[0].size + dict.hash_tables[1].size
    end

    def size(dict)
      dict.hash_tables[0].used + dict.hash_tables[1].used
    end

    def ht_needs_resize(dict)
      size = slots(dict)
      used = size(dict)

      size > Dict::INITIAL_SIZE && ((used * 100) / size < HASHTABLE_MIN_FILL)
    end

    def parse_integer(integer_str, error_message)
      begin
        value = Integer(integer_str)
        if value < 0
          raise ProtocolError, "ERR Protocol error: #{ error_message }"
        else
          value
        end
      rescue ArgumentError
        raise ProtocolError, "ERR Protocol error: #{ error_message }"
      end
    end

    def unblock_client(client)
      @db.unblocked_clients.right_push client

      return if client.blocked_state.nil?

      # Remove this client from the blocking_keys lists
      client.blocked_state.keys.each do |key2|
        list = @db.blocking_keys[key2]
        if list
          list.remove(1, client)
          @db.blocking_keys.delete(key2) if list.empty?
        end
      end

      @db.client_timeouts.delete(client.blocked_state)

      client.blocked_state = nil
    end

    def handle_blocked_clients_timeout
      @db.client_timeouts.delete_if do |blocked_state|
        client = blocked_state.client
        if client.blocked_state.nil?
          @logger.warn "Unexpectedly found a non blocked client in timeouts: #{ client }"
          true
        elsif client.blocked_state.timeout < Time.now
          @logger.debug "Expired timeout: #{ client }"
          unblock_client(client)

          unless Utils.safe_write(client.socket, NullArrayInstance.serialize)
            @logger.warn "Error writing back to #{ client }: #{ e.message }"
            disconnect_client(client)
          end

          true
        else
          # Impossible to find more later on since client_timeouts is sorted
          break
        end
      end
    end

    def process_unblocked_clients
      return if @db.unblocked_clients.empty?

      cursor = @db.unblocked_clients.left_pop

      while cursor
        client = cursor.value

        if @clients.include?(client.id)
          process_client_buffer(client)
        else
          @logger.warn "Unblocked client #{ client } must have disconnected"
        end

        cursor = @db.unblocked_clients.left_pop
      end
    end

    def block_client(client, blocked_state)
      if client.blocked_state
        @logger.warn "Client was already blocked: #{ blocked_state }"
        return
      end

      blocked_state.client = client

      # Add the state to the client
      client.blocked_state = blocked_state
      @db.client_timeouts << blocked_state

      # Add this client to the list of clients waiting on this key
      blocked_state.keys.each do |key|
        client_list = @db.blocking_keys[key]
        if client_list.nil?
          client_list = List.new
          @db.blocking_keys[key] = client_list
        end
        client_list.right_push(client)
      end
    end

    def handle_clients_blocked_on_keys
      return if @db.ready_keys.used == 0

      @db.ready_keys.each do |key, _|
        unblocked_clients = @blocked_client_handler.handle(key)

        unblocked_clients.each do |client|
          unblock_client(client)
        end
      end

      @db.ready_keys = Dict.new
    end
  end
end
