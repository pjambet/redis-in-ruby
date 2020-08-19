module BYORedis
  class SetCommand

    ValidationError = Class.new(StandardError)
    SyntaxError = Class.new(StandardError)

    CommandOption = Struct.new(:kind)
    CommandOptionWithValue = Struct.new(:kind, :validator)

    OPTIONS = Dict.new
    OPTIONS.set(
      'ex',
      CommandOptionWithValue.new(
        'expire',
        ->(value) { validate_integer(value) * 1000 },
      )
    )
    OPTIONS.set(
      'px',
      CommandOptionWithValue.new(
        'expire',
        ->(value) { validate_integer(value) },
      )
    )
    OPTIONS.set(
      'xx', CommandOption.new('presence')
    )
    OPTIONS.set(
      'nx', CommandOption.new('presence')
    )
    OPTIONS.set(
      'keepttl', CommandOption.new('expire')
    )



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

      @options = Dict.new
    end

    def call
      key, value = @args.shift(2)
      if key.nil? || value.nil?
        return RESPError.new("ERR wrong number of arguments for 'SET' command")
      end

      parse_result = parse_options

      if @options['presence'] == 'nx' && !@data_store[key].nil?
        NullBulkStringInstance
      elsif @options['presence'] == 'xx' && @data_store[key].nil?
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

        OKSimpleStringInstance
      end

    rescue ValidationError => e
      RESPError.new(e.message)
    rescue SyntaxError => e
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
        option_detail = OPTIONS[option.downcase]

        if option_detail
          option_values = parse_option_arguments(option, option_detail)
          existing_option = @options[option_detail.kind]

          if existing_option
            raise SyntaxError, 'ERR syntax error'
          else
            @options[option_detail.kind] = option_values
          end
        else
          raise SyntaxError, 'ERR syntax error'
        end
      end
    end

    def parse_option_arguments(option, option_detail)
      case option_detail
      when CommandOptionWithValue
        option_value = @args.shift
        option_detail.validator.call(option_value)
      when CommandOption
        option.downcase
      else
        raise "Unknown command option type: #{ option_detail }"
      end
    end
  end
end
