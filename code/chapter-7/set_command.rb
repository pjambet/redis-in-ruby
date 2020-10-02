module BYORedis
  class SetCommand < BaseCommand

    CommandOption = Struct.new(:kind)
    CommandOptionWithValue = Struct.new(:kind, :validator)

    OPTIONS = Dict.new
    OPTIONS.set(
      'ex',
      CommandOptionWithValue.new(
        'expire',
        ->(value) { OptionUtils.validate_integer(value) * 1000 },
      )
    )
    OPTIONS.set(
      'px',
      CommandOptionWithValue.new(
        'expire',
        ->(value) { OptionUtils.validate_integer(value) },
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

    def initialize(db, args)
      @db = db
      @args = args

      @options = Dict.new
    end

    def call
      key, value = @args.shift(2)
      if key.nil? || value.nil?
        return RESPError.new("ERR wrong number of arguments for 'SET' command")
      end

      parse_options

      if @options['presence'] == 'nx' && !@db.data_store[key].nil?
        NullBulkStringInstance
      elsif @options['presence'] == 'xx' && @db.data_store[key].nil?
        NullBulkStringInstance
      else

        @db.data_store[key] = value
        expire_option = @options['expire']

        # The implied third branch is if expire_option == 'KEEPTTL', in which case we don't have
        # to do anything
        if expire_option.is_a? Integer
          @db.expires[key] = (Time.now.to_f * 1000).to_i + expire_option
        elsif expire_option.nil?
          @db.expires.delete(key)
        end

        OKSimpleStringInstance
      end
    end

    def self.describe
      Describe.new('set', -3, [ 'write', 'denyoom' ], 1, 1, 1, [ '@write', '@string', '@slow' ])
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
            raise RESPSyntaxError
          else
            @options[option_detail.kind] = option_values
          end
        else
          raise RESPSyntaxError
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
