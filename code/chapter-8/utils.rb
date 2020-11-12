require 'bigdecimal'

module BYORedis

  ULLONG_MAX = 2**64 - 1 # 18,446,744,073,709,551,615
  ULLONG_MIN = 0
  LLONG_MAX = 2**63 - 1 # 9,223,372,036,854,775,807
  LLONG_MIN = 2**63 * - 1 # -9,223,372,036,854,775,808

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
  ValidationError = Class.new(StandardError) do
    def resp_error
      RESPError.new(message)
    end
  end
  IntegerOverflow = Class.new(StandardError)
  InvalidIntegerString = Class.new(StandardError)
  InvalidFloatString = Class.new(StandardError)

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

    def self.string_to_integer(string)
      raise InvalidIntegerString, 'Empty string' if string.empty?

      bytes = string.bytes
      zero_ord = '0'.ord # 48, 'a'.ord == 97, so

      return 0 if bytes.length == 1 && bytes[0] == zero_ord

      if bytes[0] == '-'.ord
        negative = true
        bytes.shift
        raise InvalidIntegerString, 'Nothing after -' if bytes.empty?
      else
        negative = false
      end

      unless bytes[0] >= '1'.ord && bytes[0] <= '9'.ord
        raise InvalidIntegerString
      end

      num = bytes[0] - zero_ord

      1.upto(bytes.length - 1) do |i|
        unless bytes[i] >= zero_ord && bytes[i] <= '9'.ord
          raise InvalidIntegerString, "Not a number: '#{ bytes[i] }' / '#{ [ bytes[i] ].pack('C') }'"
        end

        raise IntegerOverflow, 'Overflow before *' if num > ULLONG_MAX / 10

        num *= 10
        raise IntegerOverflow, 'Overflow before +' if num > ULLONG_MAX - (bytes[i] - zero_ord)

        num += bytes[i] - zero_ord
      end

      if negative && num > -LLONG_MIN
        # In Redis, the condition is:
        #
        # if (v > ( (unsigned long long) (-(LLONG_MIN+1)) +1) )
        #
        # But used to be (-(unsigned long long)LLONG_MIN) until this commit:
        # https://github.com/redis/redis/commit/5d08193126df54405dae3073c62b7c19ae03d1a4
        #
        # Both seem to be similar but the current version might be safer on different machines.
        # Essentially it adds one to LLONG_MIN, so that multiplying it by -1 with the - operator
        # falls within the boundaries of a long long, given that min can be -9...808 while max
        # is always 9...807, we then cast the positive value to an unsigned long long, so that
        # we can add 1 to it, turning it into 9...808
        # The C standard does not seem to be very specific around the exact value of LLONG_MIN
        # it seems to either be -9..807 or, as it is on my machine, a mac, -9...808, which is
        # because it uses Two's Complement.
        raise IntegerOverflow, 'Too small for a long long'
      elsif negative
        -num
      elsif num > LLONG_MAX
        raise IntegerOverflow, 'Too big for a long long'
      else
        num
      end
    end

    def self.float_to_string(big_decimal)
      if big_decimal == BigDecimal::INFINITY
        'inf'
      elsif big_decimal == -BigDecimal::INFINITY
        '-inf'
      elsif (truncated = big_decimal.truncate) == big_decimal
        # Remove the .0 part of the number
        integer_to_string(truncated)
      else
        big_decimal.to_s('F')
      end
    end

    def self.integer_to_string(integer)
      return '0' if integer == 0

      v = integer >= 0 ? integer : -integer
      zero_ord = '0'.ord
      bytes = []

      until v == 0
        bytes.prepend(zero_ord + v % 10)
        v /= 10
      end

      bytes.prepend('-'.ord) if integer < 0
      bytes.pack('C*')
    end

    def self.validate_integer(str)
      string_to_integer(str)
    rescue IntegerOverflow, InvalidIntegerString
      raise ValidationError, 'ERR value is not an integer or out of range'
    end

    def self.validate_float(str, error_message)
      case str
      when '+inf', 'inf', 'infinity', '+infinity' then BigDecimal::INFINITY
      when '-inf', '-infinity' then -BigDecimal::INFINITY
      else
        parsed = BigDecimal(str)
        if parsed.nan?
          raise ArgumentError
        else
          parsed
        end
      end
    rescue ArgumentError, TypeError
      raise ValidationError, error_message
    end

    def self.validate_timeout(str)
      timeout = validate_float(str, 'ERR timeout is not a float or out of range')
      raise ValidationError, 'ERR timeout is negative' if timeout < 0 || timeout.infinite?

      timeout
    end
  end
end
