class SetCommand

  ValidationError = Class.new(StandardError)

  # Replace #argument_type with a validation method
  CommandOption = Struct.new(:kind, :validator, :transform, :number_of_arguments)

  IDENTITY = ->(value) { value }

  OPTIONS = {
    'EX' => CommandOption.new(
      'expire',
      ->(value) { validate_integer(value) },
      ->(seconds) { ((Time.now + seconds.to_i).to_f * 1000).to_i },
      1,
    ),
    'PX' => CommandOption.new(
      'expire',
      ->(value) { validate_integer(value) },
      ->(milliseconds) { (Time.now.to_f * 1000).to_i + milliseconds.to_i },
      1
    ),
    'KEEPTTL' => CommandOption.new('expire', IDENTITY, IDENTITY, 0),
    'NX' => CommandOption.new('presence', IDENTITY, IDENTITY, 0),
    'XX' => CommandOption.new('presence', IDENTITY, IDENTITY, 0),
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
      return "(error) ERR wrong number of arguments for 'SET' command"
    end

    parse_result = parse_options

    if !parse_result.nil?
      return parse_result
    end

    existing_key = @data_store[key]

    p 'options'
    p @options

    if @options['presence'] == 'NX' && !existing_key.nil?
      '(nil)'
    elsif @options['presence'] == 'XX' && existing_key.nil?
      '(nil)'
    else

      @data_store[key] = value
      expire_option = @options['expire']

      case expire_option
      when Integer
        @expires[key] = expire_option
      when 'KEEPTTL'
        @expires.delete(key)
      else
        # raise "not sure what happened"
      end
      'OK'
    end

  rescue ValidationError => e
    p e
    e.message
  end

  private

  def self.validate_integer(str)
    if integer?(str)
      Integer(str)
    else
      raise ValidationError, '(error) ERR value is not an integer or out of range'
    end
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
        option_values = parse_option_arguments(option_detail)
        p "option_value: #{ option_values }"
        existing_option = @options[option_detail.kind]

        if existing_option
          p 'syntax error'
          return '(error) ERR syntax error'
        else
          @options[option_detail.kind] = option_values
        end
      else
        p 'no option detail, syntax error'
        return '(error) ERR syntax error'
      end
    end
  end

  def parse_option_arguments(option_detail)
    validator = option_detail.validator

    option_values = @args.shift(option_detail.number_of_arguments)
    option_values.map do |option_value|
      validation_result = validator.call(option_value)
      option_detail.transform.call(option_value)
    end
  end
end
