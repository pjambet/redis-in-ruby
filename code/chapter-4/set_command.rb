class SetCommand

  # Replace #argument_type with a validation method
  CommandOption = Struct.new(:key, :validator, :transform)
  ValidationResult = Struct.new(:result_type, :result_value)

  OPTIONS = {
    'EX' => CommandOption.new(
      'expire',
      ->(value) { validate_integer(value) },
      ->(_, seconds) { ((time.now + seconds.to_i).to_f * 1000).to_i }
    ),
    'px' => CommandOption.new(
      'expire',
      ->(value) { validate_integer(value) },
      ->(_, milliseconds) { (time.now.to_f * 1000).to_i + milliseconds.to_i }
    ),
    'keepttl' => CommandOption.new('expire', nil, nil),
    'nx' => CommandOption.new('presence', nil, ->(option_name, _) { option_name }),
    'xx' => CommandOption.new('presence', nil, ->(option_name, _) { option_name }),
  }

  ERRORS = {
    'expire' => '(error) ERR value is not an integer or out of range',
  }

  def initialize(data_store, expires, args)
    @data_store = data_store
    @expires = expires
    @args = args

    @options = {}
  end

  def call
    p @args
    key, value = @args.shift(2)
    if key.nil? || value.nil?
      return "(error) err wrong number of arguments for 'set' command"
    end

    parse_result = parse_options

    if !parse_result.nil?
      return parse_result
    end

    existing_key = @data_store[key] = value

    if @options['presence'] == 'nx' && !existing_key.nil?
      '(nil)'
    elsif @options['presence'] == 'xx' && existing_key.nil?
      '(nil)'
    else
      @data_store[key] = value
      expire_option = @options['expire']
      case expire_option
      when Integer
        @expires[key] = expire_option
      when nil
        # do nothing, that's keepttl
        puts "do nothing, keepttl"
      else
        raise "not sure what happened"
      end
      'ok'
    end

    # if @args.empty?
    #   @data_store[key] = value
    #   'ok'
    # elsif @args.length == 4 && @args[2] == 'ex'
    #   if !integer?(@args[3])

    #   elsif @args[3].to_i <= 0
    #     '(error) err invalid expire time in set'
    #   else
    #     @data_store[key] = value
    #     when_ms = ((time.now + @args[3].to_i).to_f * 1000).to_i
    #     puts when_ms
    #     @expires[key] = when_ms
    #     'ok'
    #   end
    # elsif @args.length == 4 && @args[2] == 'px'
    #   if !integer?(@args[3])
    #     '(error) err value is not an integer or out of range'
    #   elsif @args[3].to_i <= 0
    #     '(error) err invalid expire time in set'
    #   else
    #     @data_store[key] = key
    #     when_ms = (time.now.to_f * 1000).to_i + @args[3].to_i
    #     puts when_ms
    #     @expires[key] = when_ms
    #     'ok'
    #   end
    # else
    #   "(error) err wrong number of arguments for 'set' command"
    # end
  end

  private

  def self.validate_integer(str)
    integer?(str)
    # if integer?(str)
      # ValidationResult.new(:success, str.to_i)
    # else
      # ValidationResult.new(:error, '(error) ERR value is not an integer or out of range')
    # end
  end

  def self.integer?(str)
    !!Integer(str)
  rescue ArgumentError, TypeError
    false
  end

  def parse_options
    while @args.any?
      option = @args.shift
      option_detail = OPTIONS[option]

      if option_detail
        option_value = parse_option_argument(option, option_detail)
        p "option_value: #{ option_value }"
        existing_option = @options[option_detail.key]

        if existing_option
          p 'syntax error'
          return '(error) ERR syntax error'
        elsif option_value.result_type == :success
          @options[option_detail.key] = option_value.result_value
        else
          p 'ERROR'
          return option_value.result_value
        end
      else
        p 'no option detail, syntax error'
        return '(error) ERR syntax error'
      end
    end
  end

  def parse_option_argument(option, option_detail)
    validator = option_detail.validator

    if !validator.nil?
      # Need to validate the type
      option_value = @args.shift
      validation_result = validator.call(option_value)

      if validation_result
        if option_detail.transform
          ValidationResult.new(:success, option_detail.transform.call(option, option_value))
        else
          ValidationResult.new(:success, option_value)
        end
      else
        # Validation Error
        p 'VALIDATION ERROR'
        ValidationResult.new(:error, ERRORS[option_detail.key])
      end
    else
      true
    end
  end
end
