require 'socket'
require 'timeout'

class BasicServer

  COMMANDS = [
    "GET",
    "SET",
  ]

  def initialize
    @clients = []
    @data_store = {}

    server = TCPServer.new 2000
    puts "Server started at: #{ Time.now }"
    Thread.new do
      loop do
        sleep 1
        puts "Accepting clients"
        new_client = server.accept
        puts "New client connected: #{ new_client }"
        @clients << new_client
        sleep 1
      end
    end

    loop do
        @clients.each do |client|
        begin
          Timeout::timeout(0.1) do
            puts "reading from client: #{ client }"
            client_command_with_args = client.gets
            if client_command_with_args.nil?
              @clients.delete(client)
            elsif client_command_with_args.strip.empty?
              puts "Empty request received from #{ client }"
            else
              response = handle_client_command(client_command_with_args)
              client.puts response
            end
          end
        rescue Timeout::Error
          puts "Did not receive anything from client after 0.1s, moving on"
          next
        rescue Errno::ECONNRESET
          @clients.delete(client)
        end
      end
    end
  end

  private

  def handle_client_command(client_command_with_args)
    command_parts = client_command_with_args.split
    command = command_parts[0]
    args = command_parts[1..-1]
    if COMMANDS.include?(command)
      if command == "GET"
        if args.length != 1
          "(error) ERR wrong number of arguments for '#{ command }' command"
        else
          @data_store.fetch(args[0], "(nil)")
        end
      elsif command == "SET"
        if args.length != 2
          "(error) ERR wrong number of arguments for '#{ command }' command"
        else
          @data_store[args[0]] = args[1]
          'OK'
        end
      end
    else
      formatted_args = args.map { |arg| "`#{ arg }`," }.join(" ")
      "(error) ERR unknown command `#{ command }`, with args beginning with: #{ formatted_args }"
    end
  end
end
