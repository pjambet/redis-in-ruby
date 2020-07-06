require 'socket'

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
        new_client = server.accept
        @clients << new_client
      end
    end

    loop do
      @clients.each do |client|
        if client.eof?
          p "Client eof #{ client }"
          client.close
          @clients.delete(client)
        else
          client_command_with_args = client.gets
          if client_command_with_args && client_command_with_args.length > 0
            response = handle_client_command(client_command_with_args)
            client.puts response
          else
            puts "Empty request received from #{ client }"
          end
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
