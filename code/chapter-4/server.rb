require 'socket'
require 'timeout'
require 'logger'
LOG_LEVEL = ENV['DEBUG'] ? Logger::DEBUG : Logger::INFO

require_relative './get_command'
require_relative './set_command'

class BasicServer

  COMMANDS = [
    'GET',
    'SET',
  ]

  MAX_EXPIRE_LOOKUPS_PER_CYCLE = 20
  DEFAULT_FREQUENCY = 10 # How many times server_cron runs per second

  TimeEvent = Struct.new(:process_at, :block)

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.level = LOG_LEVEL

    @clients = []
    @data_store = {}
    @expires = {}

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

  def start_event_loop
    loop do
      # Selecting blocks, so if there's no client, we don't have to call it, which would
      # block, we can just keep looping
      timeout = if @time_events.any?
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
      @logger.debug "select with a timeout of #{ timeout }"
      result = IO.select(@clients + [@server], [], [], timeout)
      sockets = result ? result[0] : []
      process_poll_events(sockets)
      process_time_events
    end
  end

  def process_poll_events(sockets)
    sockets.each do |socket|
      begin
        if socket.is_a?(TCPServer)
          @clients << @server.accept
        elsif socket.is_a?(TCPSocket)
          client_command_with_args = socket.read_nonblock(1024, exception: false)
          if client_command_with_args.nil?
            @clients.delete(socket)
          elsif client_command_with_args == :wait_readable
            # There's nothing to read from the client, we don't have to do anything
            next
          elsif client_command_with_args.strip.empty?
            @logger.debug "Empty request received from #{ client }"
          else
            commands = client_command_with_args.strip.split("\n")
            commands.each do |command|
              response = handle_client_command(command.strip)
              @logger.debug "Response: #{ response }"
              socket.puts response
            end
          end
        else
          raise "Unknown socket type: #{ socket }"
        end
      rescue Errno::ECONNRESET
        @clients.delete(socket)
      end
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

  def handle_client_command(client_command_with_args)
    @logger.debug "Received command: #{ client_command_with_args }"
    command_parts = client_command_with_args.split
    command = command_parts[0]
    args = command_parts[1..-1]
    if COMMANDS.include?(command)
      if command == 'GET'
        get_command = GetCommand.new(@data_store, @expires, args)
        get_command.call
      elsif command == 'SET'
        set_command = SetCommand.new(@data_store, @expires, args)
        set_command.call
      end
    else
      formatted_args = args.map { |arg| "`#{ arg }`," }.join(' ')
      "(error) ERR unknown command `#{ command }`, with args beginning with: #{ formatted_args }"
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
      sprintf(
        "Processed %i keys in %.3f ms", keys_fetched, (end_timestamp - start_timestamp) * 1000)
    end

    1000 / DEFAULT_FREQUENCY
  end
end
