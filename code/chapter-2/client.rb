require 'socket'

class BasicClient

  COMMANDS = [
    "GET",
    "SET",
  ]

  def get(key)
    socket = TCPSocket.new 'localhost', 2000
    result = nil
    socket.puts "GET #{ key }"
    result = socket.gets
    socket.close
    result
  end

  def set(key, value)
    socket = TCPSocket.new 'localhost', 2000
    result = nil
    socket.puts "SET #{ key } #{ value }"
    result = socket.gets
    socket.close
    result
  end
end
