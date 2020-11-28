require_relative './bit_ops'

module BYORedis
  module BitOpsUtils
    def self.validate_offset(string)
      error_message = 'ERR bit offset is not an integer or out of range'
      offset = Utils.validate_integer_with_message(string, error_message)

      if offset >= 0
        offset
      else
        raise ValidationError, error_message
      end
    end

    def self.validate_bit(string)
      error_message = 'ERR bit is not an integer or out of range'
      bit_value = Utils.validate_integer_with_message(string, error_message)

      if (bit_value & ~1) == 0 # equivalent to bit_value == 0 || bit_value == 1
        bit_value
      else
        raise ValidationError, error_message
      end
    end
  end

  class GetBitCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      string = @db.lookup_string(@args[0])
      offset = BitOpsUtils.validate_offset(@args[1])

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
      offset = BitOpsUtils.validate_offset(@args[1])
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
      Utils.assert_args_length_greater_than(2, @args)
      operation = @args.shift.downcase
      dest = @args.shift
      rest = @args.map { |key| @db.lookup_string(key) }

      raise RESPSyntaxError unless [ 'and', 'or', 'xor', 'not' ].include?(operation)
      if operation == 'not' && rest.size > 1
        raise ValidationError, 'ERR BITOP NOT must be called with a single source key.'
      end

      result = BitOps.op(operation.to_sym, rest)
      if result.nil?
        @db.data_store.delete(dest)
        length = 0
      else
        @db.data_store[dest] = result
        length = result.length
      end

      RESPInteger.new(length)
    end

    def self.describe
      Describe.new('bitop', -4, [ 'write', 'denyoom' ], 2, -1, 1,
                   [ '@write', '@bitmap', '@slow' ])
    end
  end

  class BitCountCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      string = @db.lookup_string(@args.shift)
      return RESPInteger.new(0) if string.nil?

      if @args.empty?
        start_byte = 0
        end_byte = string.length - 1
      elsif @args.length == 2
        start_byte = Utils.validate_integer(@args[0])
        end_byte = Utils.validate_integer(@args[1])
      else
        raise RESPSyntaxError
      end

      RESPInteger.new(BitOps.new(string).bit_count(start_byte, end_byte))
    end

    def self.describe
      Describe.new('bitcount', -2, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@slow' ])
    end
  end

  class BitPosCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      string = @db.lookup_string(@args.shift)

      bit_value = Utils.validate_integer_with_message(@args.shift, 'ERR value is not an integer or out of range')

      if (bit_value & ~1) == 0 # equivalent to bit_value == 0 || bit_value == 1
        bit_value
      else
        raise ValidationError, 'ERR The bit argument must be 1 or 0.'
      end

      if string.nil?
        index = bit_value == 0 ? 0 : -1
        return RESPInteger.new(index)
      end

      if @args.empty?
        start_byte = 0
        end_byte = string.length - 1
      elsif @args.length == 1
        start_byte = Utils.validate_integer(@args[0])
        end_byte = string.length - 1
      elsif @args.length == 2
        start_byte = Utils.validate_integer(@args[0])
        end_byte = Utils.validate_integer(@args[1])
      else
        raise RESPSyntaxError
      end

      RESPInteger.new(BitOps.new(string).bit_pos(bit_value, start_byte, end_byte))
    end

    def self.describe
      Describe.new('bitpos', -3, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@slow' ])
    end
  end

  Operation = Struct.new(:name, :type, :size, :offset, :new_value, :overflow)

  class BitFieldCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      key = @args.shift
      string = @db.lookup_string(key)

      operations = parse_operations
      result = []
      bit_ops = BitOps.new(string)

      operations.each do |operation|
        if bit_ops.string.nil? && operation.name == :set || operation.name == :incrby
          string = ''
          bit_ops.string = string
          @db.data_store[key] = string
        end

        result << (string.nil? ? 0 : bit_ops.field_op(operation))
      end

      RESPArray.new(result)
    end

    def self.describe
      Describe.new('bitfield', -2, [ 'write', 'denyoom' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@slow' ])
    end

    private

    def parse_operations
      operations = []
      current_overflow = :wrap
      while arg = @args.shift
        case arg.downcase
        when 'get'
          type, size = validate_type(@args.shift)
          offset = validate_offset(@args.shift, size)

          operations << Operation.new(:get, type, size, offset)
        when 'set'
          type, size = validate_type(@args.shift)
          offset = validate_offset(@args.shift, size)
          new_value = Utils.validate_integer(@args.shift)

          operations << Operation.new(:set, type, size, offset, new_value, current_overflow)
        when 'overflow'
          overflow_type = @args.shift&.downcase
          if [ 'sat', 'wrap', 'fail' ].include?(overflow_type)
            current_overflow = overflow_type.to_sym
          else
            raise RESPSyntaxError
          end
        else raise RESPSyntaxError
        end
      end

      operations
    end

    def validate_type(integer_type)
      error_message =
        'ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.'
      type =
        case integer_type[0].downcase
        when 'i' then :signed
        when 'u' then :unsigned
        else raise ValidationError, error_message
        end

      size = Utils.validate_integer_with_message(integer_type[1..-1], error_message)
      if size < 0 || (type == :signed && size > 64) || (type == :unsigned && size > 63)
        raise ValidationError, error_message
      else
        return type, size
      end
    end

    def validate_offset(offset, size)
      raise RESPSyntaxError if offset.nil? # TODO: add a test for this

      if offset[0] == '#'
        offset = Utils.validate_integer(offset[1..-1]) * size
      else
        offset = Utils.validate_integer(offset)
      end

      offset
    end
  end
end
