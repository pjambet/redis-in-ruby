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

    def self.assert_args_length_multiple_of(multiple, args)
      if args.length % multiple != 0
        raise InvalidArgsLength,
              "Expected args count to be a multiple of #{ multiple }, got #{ args }"
      end
    end

    def self.safe_write(socket, message)
      socket.write(message)
    rescue Errno::ECONNRESET, Errno::EPIPE, IOError
      false
    end

    ULLONG_MAX = 2**64 - 1
    ULLONG_MIN = 0
    LLONG_MAX = 2**63 - 1
    LLONG_MIN = 2**63 * -1

    def self.string_to_integer(string)
      raise 'Empty string' if string.empty?

      return 0 if string.length == 1 && string[0] == '0'

      if string[0] == '-'
        negative = true
        string = string[1..-1]
        raise 'Nothing after -' if string.empty?
      else
        negative = false
      end

      raise 'Leading zero' if !(string[0] >= '1' && string[0] <= '9')

      zero_ord = '0'.ord
      num = string[0].ord - zero_ord

      1.upto(string.length - 1) do |i|
        # Check for overflow: if (v > (ULLONG_MAX / 10)) /* Overflow. */
        raise 'Overflow before *' if num > ULLONG_MAX / 10

        num *= 10
        # Check for overflow: if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
        raise 'Overflow before +' if num > ULLONG_MAX - (string[i].ord - zero_ord)

        num += string[i].ord - zero_ord
      end

      if negative
      # if (v > ((unsigned long long)(-(LLONG_MIN+1))+1)) /* Overflow. */
        # LLONG_MIN = -9223372036854775808
        # We add 1 to it, multiple by -1 and and one to it again
        # Essentially, if num is greater than 9223372036854775808, we can't turn it into a ll
        if num > ((LLONG_MIN + 1) * -1) + 1
          raise "Too big"
        else
          -num
        end
      else
        if num > LLONG_MAX
          raise "Overflow, too big"
        else
          num
        end
      end
    end

    def self.integer_to_stringer(integer)
    end
  end
end
