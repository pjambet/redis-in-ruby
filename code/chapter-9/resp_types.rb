module BYORedis
  RESPError = Struct.new(:message) do
    def serialize
      "-#{ message }\r\n"
    end
  end

  RESPInteger = Struct.new(:underlying_integer) do
    def serialize
      ":#{ underlying_integer }\r\n"
    end

    def to_i
      underlying_integer.to_i
    end
  end

  RESPSimpleString = Struct.new(:underlying_string) do
    def serialize
      "+#{ underlying_string }\r\n"
    end
  end

  OKSimpleStringInstance = Object.new.tap do |obj|
    OK_SIMPLE_STRING = "+OK\r\n".freeze
    def obj.serialize
      OK_SIMPLE_STRING
    end
  end

  RESPBulkString = Struct.new(:underlying_string) do
    def serialize
      "$#{ underlying_string.bytesize }\r\n#{ underlying_string }\r\n"
    end
  end

  NullBulkStringInstance = Object.new.tap do |obj|
    NULL_BULK_STRING = "$-1\r\n".freeze
    def obj.serialize
      NULL_BULK_STRING
    end
  end

  RESPArray = Struct.new(:underlying_array) do
    def serialize
      serialized_items = underlying_array.map do |item|
        case item
        when RESPSimpleString, RESPBulkString
          item.serialize
        when String
          RESPBulkString.new(item).serialize
        when Integer
          RESPInteger.new(item).serialize
        when Array
          RESPArray.new(item).serialize
        when nil
          NULL_BULK_STRING
        end
      end
      "*#{ underlying_array.length }\r\n#{ serialized_items.join }"
    end
  end

  EmptyArrayInstance = Object.new.tap do |obj|
    EMPTY_ARRAY = "*0\r\n".freeze
    def obj.serialize
      EMPTY_ARRAY
    end
  end

  NullArrayInstance = Object.new.tap do |obj|
    NULL_ARRAY = "*-1\r\n".freeze
    def obj.serialize
      NULL_ARRAY
    end
  end

  class RESPSerializer
    def self.serialize(object)
      case object
      when Array then RESPArray.new(object)
      when RedisSet then SetSerializer.new(object)
      when List then ListSerializer.new(object)
      when Integer then RESPInteger.new(object)
      when String then RESPBulkString.new(object)
      when Dict
        pairs = []
        object.each { |k, v| pairs.push(k, v) }
        RESPArray.new(pairs)
      when nil then NullBulkStringInstance
      else
        raise "Unknown object for RESP serialization #{ object }"
      end
    end
  end
end
