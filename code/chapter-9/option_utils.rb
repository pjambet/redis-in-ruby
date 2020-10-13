module BYORedis

  ValidationError = Class.new(StandardError) do
    def resp_error
      RESPError.new(message)
    end
  end

  module OptionUtils

    def self.validate_integer(str)
      Integer(str)
    rescue ArgumentError, TypeError
      raise ValidationError, 'ERR value is not an integer or out of range'
    end

    def self.validate_float(str, field_name)
      Float(str)
    rescue ArgumentError, TypeError
      raise ValidationError, "ERR #{ field_name } is not a float or out of range"
    end

    def self.validate_float_with_message(str, error_message)
      Float(str)
    rescue ArgumentError, TypeError
      raise ValidationError, error_message
    end
  end
end
