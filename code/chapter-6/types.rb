require 'delegate'

module Redis
  NullArray = Class.new do
    def serialize
      "*-1\r\n"
    end
  end
  NullArrayInstance = NullArray.new

  NullBulkString = Class.new do
    def serialize
      "$-1\r\n"
    end
  end
  NullBulkStringInstance = NullBulkString.new

  RESPError = Class.new(SimpleDelegator) do
    def serialize
      "-#{ self }\r\n"
    end
  end

  RESPInteger = Class.new(SimpleDelegator) do
    def serialize
      ":#{ self }\r\n"
    end
  end

  RESPSimpleString = Class.new(SimpleDelegator) do
    def serialize
      "+#{ self }\r\n"
    end
  end
  OKSimpleString = RESPSimpleString.new('OK')

  RESPBulkString = Class.new(SimpleDelegator) do
    def serialize
      "$#{ bytesize }\r\n#{ self }\r\n"
    end
  end

  RESPArray = Class.new(SimpleDelegator) do
    def serialize
      serialized_items = map do |item|
        case item
        when RESPSimpleString, RESPBulkString
          item.serialize
        when String
          RESPBulkString.new(item).serialize
        when Integer
          RESPInteger.new(item).serialize
        when Array
          RESPArray.new(item).serialize
        end
      end
      "*#{ length }\r\n#{ serialized_items.join }"
    end
  end
end
