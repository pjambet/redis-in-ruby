module Redis
  class SetCommand

    ValidationError = Class.new(StandardError)

    CommandOption = Struct.new(:kind)
    CommandOptionWithValue = Struct.new(:kind, :validator)

    OPTIONS = {
      'EX' => CommandOptionWithValue.new(
        'expire',
        ->(value) { validate_integer(value) * 1000 },
      ),
      'PX' => CommandOptionWithValue.new(
        'expire',
        ->(value) { validate_integer(value) },
      ),
      'KEEPTTL' => CommandOption.new('expire'),
      'NX' => CommandOption.new('presence'),
      'XX' => CommandOption.new('presence'),
    }

    # ERRORS = {
      # 'expire' => Redis::RESPError.new('ERR value is not an integer or out of range'),
    # }

    def self.validate_integer(str)
      Integer(str)
    rescue ArgumentError, TypeError
      raise ValidationError, 'ERR value is not an integer or out of range'
    end

    def initialize(data_store, expires, args)
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      @data_store = data_store
      @expires = expires
      @args = args

      @options = {}
    end

    def call
      key, value = @args.shift(2)
      if key.nil? || value.nil?
        return RESPError.new("ERR wrong number of arguments for 'SET' command")
      end

      parse_result = parse_options

      if !parse_result.nil?
        return parse_result
      end

      existing_key = @data_store[key]

      if @options['presence'] == 'NX' && !existing_key.nil?
        NullBulkStringInstance
      elsif @options['presence'] == 'XX' && existing_key.nil?
        NullBulkStringInstance
      else

        @data_store[key] = value
        expire_option = @options['expire']

        # The implied third branch is if expire_option == 'KEEPTTL', in which case we don't have
        # to do anything
        if expire_option.is_a? Integer
          @expires[key] = (Time.now.to_f * 1000).to_i + expire_option
        elsif expire_option.nil?
          @expires.delete(key)
        end

        OKSimpleString
      end

    rescue ValidationError => e
      RESPError.new(e.message)
    end

    def self.describe
      [
        'set',
        -3, # arity
        # command flags
        [ 'write', 'denyoom' ].map { |s| RESPSimpleString.new(s) },
        1, # position of first key in argument list
        1, # position of last key in argument list
        1, # step count for locating repeating keys
        # acl categories: https://github.com/antirez/redis/blob/6.0/src/server.c#L161-L166
        [ '@write', '@string', '@slow' ].map { |s| RESPSimpleString.new(s) },
      ]
    end

    private

    def parse_options
      while @args.any?
        option = @args.shift
        option_detail = OPTIONS[option]

        if option_detail
          option_values = parse_option_arguments(option, option_detail)
          existing_option = @options[option_detail.kind]

          if existing_option
            return RESPError.new('ERR syntax error')
          else
            @options[option_detail.kind] = option_values
          end
        else
          return RESPError.new('ERR syntax error')
        end
      end
    end

    def parse_option_arguments(option, option_detail)
      case option_detail
      when CommandOptionWithValue
        option_value = @args.shift
        option_detail.validator.call(option_value)
      when CommandOption
        option
      else
        raise "Unknown command option type: #{ option_detail }"
      end
    end
  end
end
