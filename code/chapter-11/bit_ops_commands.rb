require_relative './bit_ops'

module BYORedis
  module BitOpsUtils
    def self.validate_offset(string)
      error_message = 'ERR bit offset is not an integer or out of range'
      offset = Utils.validate_integer_with_message(string, error_message)

      if block_given?
        if yield offset
          offset
        else
          raise ValidationError, error_message
        end
      else
        offset
      end
    end

    def self.validate_bit(string)
      Utils.validate_integer_with_message(
        string, 'ERR bit is not an integer or out of range')
    end
  end

  class GetBitCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      string = @db.lookup_string(@args[0])
      offset = BitOpsUtils.validate_offset(@args[1]) { |offset| offset >= 0 }

      RESPInteger.new(BitOps.new(string).get_bit(offset))
    end

    def self.describe
      Describe.new('getbit', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@fast' ])
    end
  end

  class SetBitCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      string = @db.lookup_string(@args[0])
      offset = BitOpsUtils.validate_offset(@args[1]) { |offset| offset >= 0 }
      bit = BitOpsUtils.validate_bit(@args[2])

      if string.nil?
        string = ''
        @db.data_store[@args[0]] = string
      end
      old_value = BitOps.new(string).set_bit(offset, bit)

      RESPInteger.new(old_value)
    end

    def self.describe
      Describe.new('setbit', 3, [ 'write', 'denyoom' ], 1, 1, 1,
                   [ '@write', '@bitmap', '@slow' ])
    end
  end

  class BitOpCommand < BaseCommand
    def call
    end

    def self.describe
      Describe.new('bitop', -4, [ 'write', 'denyoom' ], 2, -1, 1,
                   [ '@write', '@bitmap', '@slow' ])
    end
  end

  class BitCountCommand < BaseCommand
    def call
    end

    def self.describe
      Describe.new('bitcount', -2, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@slow' ])
    end
  end

  class BitPosCommand < BaseCommand
    def call
    end

    def self.describe
      Describe.new('bitpos', -3, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@slow' ])
    end
  end

  class BitFieldCommand < BaseCommand
    def call
    end

    def self.describe
      Describe.new('bitfield', -2, [ 'write', 'denyoom' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@slow' ])
    end
  end
end
