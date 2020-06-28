require 'socket'

server = TCPServer.new 2000
loop do
  Thread.start(server.accept) do |client|
    client.puts 'Hello !'
    client.puts "Time is #{Time.now}"
    client.close
  end
end
