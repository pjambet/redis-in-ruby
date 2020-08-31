module BYORedis

  InvalidArgsLength = Class.new(StandardError) do
    def resp_error(command_name)
      RESPError.new("ERR wrong number of arguments for '#{ command_name }' command")
    end
  end
  WrongTypeError = Class.new(StandardError) do
    def resp_error
      RESPError.new('WRONGTYPE Operation against a key holding the wrong kind of value')
    end
  end
  RESPSyntaxError = Class.new(StandardError) do
    def resp_error
      RESPError.new('ERR syntax error')
    end
  end

  module Utils
    def self.assert_args_length(args_length, args)
      if args.length != args_length
        raise InvalidArgsLength, "Expected #{ args_length }, got #{ args.length }: #{ args }"
      end
    end

    def self.assert_args_length_greater_than(args_length, args)
      if args.length <= args_length
        raise InvalidArgsLength,
              "Expected more than #{ args_length } args, got #{ args.length }: #{ args }"
      end
    end

    def self.safe_write(socket, message)
      socket.write(message)
    rescue Errno::ECONNRESET, Errno::EPIPE, IOError
      false
    end
  end
end
