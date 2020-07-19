require 'socket'
require 'timeout'

require_relative './get_command'
require_relative './set_command'

class BasicServer

  COMMANDS = [
    'GET',
    'SET',
  ]

  MAX_EXPIRE_LOOKUPS_PER_CYCLE = 20

  def initialize
    @clients = []
    @data_store = {}
    @expires = {}

    @server = TCPServer.new 2000
    @time_events = []
    puts "Server started at: #{ Time.now }"
    add_time_event do
      server_cron
    end

    start_event_loop
  end

  private

  def add_time_event(&block)
    @time_events << block
  end

  def start_event_loop
    loop do
      # Selecting blocks, so if there's no client, we don't have to call it, which would
      # block, we can just keep looping
      result = IO.select(@clients + [@server], [], [], 1)
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
            puts "Empty request received from #{ client }"
          else
            response = handle_client_command(client_command_with_args.strip)
            puts "Response: #{ response }"
            socket.puts response
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
    @time_events.each { |time_event| time_event.call }
  end

  def handle_client_command(client_command_with_args)
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
    t0 = Time.now
    keys_fetched = 0

    @expires.each do |key, value|
      if @expires[key] < Time.now.to_f * 1000
        # puts "Evicting #{ key }"
        @expires.delete(key)
        @data_store.delete(key)
      end

      keys_fetched += 1
      if keys_fetched >= MAX_EXPIRE_LOOKUPS_PER_CYCLE
        break
      end
    end

    t1 = Time.now
    # puts sprintf("It tooks %.7f ms to process %i keys", (t1 - t0), keys_fetched)
  end
end
