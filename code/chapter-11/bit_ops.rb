module BYORedis
  class BitOps

    attr_accessor :string

    def initialize(string)
      @string = string
    end

    def self.initialize_string_for_offset(string, offset)
      # example, with 7, size should be 1
      # 7 is 0000 0111, >> 3 = 0, size is 1
      # with 16, size should be 3
      # 16 is 0001 0000, >> 3 = 0000 0010, 2, size is 3
      # Taken from: https://github.com/antirez/redis/blob/6.0.0/src/bitops.c#L479
      size = (offset >> 3) + 1

      string << "\x00" * size
      string
    end

    def self.bitwise_op(operation, left_operand, right_operand)
      case operation
      when :and then left_operand & right_operand
      when :or  then left_operand | right_operand
      when :xor then left_operand ^ right_operand
      else raise "Operation not supported: #{ operation }"
      end
    end
    private_class_method :bitwise_op

    def self.op(operation, strings)
      # There is always at least one string, so it's safe to call .dup without a null check
      max_length = strings.reduce(0) do |max, string|
        next max if string.nil?

        if string.length > max
          max = string.length
        end
        max
      end
      return if max_length == 0

      result = String.new('', capacity: max_length)
      result << "\x00" * max_length

      0.upto(max_length - 1) do |i|
        output = if strings[0].nil?
                   0
                 else
                   strings[0][i]&.ord || 0
                 end

        if operation == :not
          result[i] = (~output & 0xff).chr
          break # A litte bit unnecessary but hey
        end

        1.upto(strings.size - 1) do |j|
          other_string = strings[j]
          other_byte =
            if other_string.nil?
              0
            else
              other_string[i]&.ord || 0
            end
          output = bitwise_op(operation, output, other_byte)
        end

        result[i] = output.chr
      end

      result
    end

    def get_bit(offset)
      byte_position = offset / 8
      return 0 if @string && byte_position >= @string.length

      bit_shift_offset = 7 - (offset & 7) # Equivalent to 7 - offset % 8
      bit_at_offset(@string[byte_position].ord, bit_shift_offset)
    end

    def set_bit(offset, bit)
      byte_position = offset / 8
      bit_shift_offset = 7 - (offset & 7) # Equivalent to 7 - offset % 8

      if @string.empty?
        BitOps.initialize_string_for_offset(@string, offset)
        old_value = 0
      elsif byte_position >= @string.length
        @string << "\x00" * (byte_position + 1 - @string.length)
        old_value = 0
      end

      byte ||= @string[byte_position].ord
      if old_value.nil?
        old_value = bit_at_offset(byte, bit_shift_offset)
      end

      byte &= ~(1 << bit_shift_offset)
      byte |= ((bit & 0x1) << bit_shift_offset)
      @string[byte_position] = byte.chr

      old_value
    end

    def bit_count(start_byte, end_byte)
      start_byte, end_byte = sanitize_start_and_end(start_byte, end_byte)

      count = 0

      start_byte.upto(end_byte) do |byte_i|
        byte = @string[byte_i].ord
        8.times do
          count += byte & 1
          byte >>= 1
        end
      end

      count
    end

    def bit_pos(bit, start_byte, end_byte)
      start_byte, end_byte = sanitize_start_and_end(start_byte, end_byte)
      pos = 8 * start_byte

      start_byte.upto(end_byte) do |byte_i|
        byte = @string[byte_i].ord
        if bit == 0 && byte == 0xff || bit == 1 && byte == 0
          pos += 8
          next
        end

        one = 2**8 - 1 # 255
        one >>= 1
        one = ~one & 0xff # all zeroes except the MSB
        bit_as_boolean = bit == 1 ? true : false

        # We start with one = 1000 0000
        8.times do |i|
          # The result will be different from 0 if both bytes have a 1 at the same index
          # so if it's not 0, we found a 1, if the result is 0, we found a 0
          return pos if ((one & byte) != 0) == bit_as_boolean

          pos += 1
          one >>= 1
        end
      end

      -1
    end

    def field_op(op)
      p op
      case op.name
      when :get    then get_op(op.offset, op.size, op.type)
      when :set    then set_op(op.offset, op.size, op.type, op.new_value, op.overflow)
      when :incrby then incrby_op(op.offset, op.size, op.type, op.incr, op.overflow)
      else raise "Unknown op: #{ op }"
      end
    end

    private

    def sanitize_start_and_end(start_byte, end_byte)
      end_byte = @string.size + end_byte if end_byte < 0
      start_byte = @string.size + start_byte if start_byte < 0

      end_byte = @string.size - 1 if end_byte >= @string.size
      start_byte = 0 if start_byte < 0

      return start_byte, end_byte
    end

    # say byte is 111, so 0110 1111 in binary
    # with offset 0, we do 0110 1111 >> 7, 0000 0000 & 1 => 0
    # with offset 1, we do 0110 1111 >> 6, 0000 0001 & 1 => 1
    # with offset 2, we do 0110 1111 >> 5, 0000 0011 & 1 => 1
    # with offset 3, we do 0110 1111 >> 4, 0000 0110 & 1 => 0
    # etc ...
    def bit_at_offset(byte, bit_shift_offset)
      (byte >> bit_shift_offset) & 1
    end

    def get_op(offset, size, type)
      start_byte = (offset >> 3) # / 8
      leftover_bits = offset % 8 # maybe we can do a bitshift?
      bytes = []
      # Load 9 bytes, to make sure we have all the bits in case not aligned
      9.times do |i|
        bytes[i] = @string[i + start_byte]&.ord || 0
      end

      # offset is the whole offset, so if it's 8, we're starting at the first bit of
      # the first byte
      bit_offset = offset - (start_byte * 8)
      value = 0

      size.times do |bit_index|
        byte = bit_offset >> 3 # divide by 8, get byte index
        # 0x7 is 0111, so we essentially do bit_offset % 8
        bit = 7 - (bit_offset & 0x7) # get bit index from the left
        byteval = bytes[byte]
        bitval = (byteval >> bit) & 1
        value = (value<<1) | bitval
        bit_offset += 1
      end

      if type == :signed && (value & (1 << (size - 1))) != 0
        value = -1 * ((~value & (2**(size) - 1)) + 1)
      end

      value
    end

    def check_signed_overflow(value, incr, size, overflow)
      max = size == 64 ? (2**63 - 1) : (1 << (size - 1)) - 1
      min = -max - 1

      max_incr = max - value
      min_incr = min - value

      if value > max || (size != 64 && incr > max_incr) || (value >= 0 && incr > 0 && incr > max_incr)
        if overflow == :sat
          max
        elsif overflow == :wrap
          wrap_around_for_signed_integers(value, incr, size)
        end
      elsif value < min || (size != 64 && incr < min_incr) || (value < 0 && incr < 0 && incr < min_incr)
        if overflow == :sat
          min
        elsif overflow == :wrap
          wrap_around_for_signed_integers(value, incr, size)
        end
      else
        nil
      end
    end

    def wrap_around_for_signed_integers(value, incr, size)
      msb = 1 << (size - 1)
      c = (value + incr) # TODO: Careful, this will never overflow in Ruby
      if size < 64
        mask = -1 << size
        if c & msb != 0
          c |= mask
        else
          c &= ~mask
        end
      end

      c
    end

    def check_unsigned_overflow(value, incr, size, overflow)
      max = size == 64 ? (2**64 - 1) : (1 << size) - 1
      max_incr = max - value
      min_incr = -value

      if value > max || (incr > 0 && incr > max_incr)
        if overflow == :sat
          max
        elsif overflow == :wrap
          wrap_around_for_unsigned_integers(value, incr, size)
        end
      elsif incr < 0 && incr < min_incr
        if overflow == :sat
          0
        elsif overflow == :wrap
          wrap_around_for_unsigned_integers(value, incr, size)
        end
      else
        nil
      end
    end

    def wrap_around_for_unsigned_integers(value, incr, size)
      mask = -1 << size
      res = value + incr
      res & ~mask
    end

    def set_op(offset, size, type, new_value, overflow)
      old_value = get_op(offset, size, type)

      if type == :unsigned && new_value < 0
        # If the value is negative and we're dealing with an unsigned format (prefixed with u), then we
        # need to convert it to a positive integer, which we do with a mask of 64 1s
        # This is equivalent to casting an int64_t to an uint64_t in C
        new_value = new_value & (2**64 - 1)
      end

      with_overflow_check(new_value, size, type, 0, overflow) do |new_value|
        set_value(offset, size, new_value)
        old_value
      end
    end

    def incrby_op(offset, size, type, incr, overflow)
      old_value = get_op(offset, size, type)

      with_overflow_check(old_value, size, type, incr, overflow) do |new_value|
        set_value(offset, size, new_value)
        new_value
      end
    end

    def with_overflow_check(value, size, type, incr, overflow)
      value_after_overflow =
        case type
        when :signed then check_signed_overflow(value, incr, size, overflow)
        when :unsigned then check_unsigned_overflow(value, incr, size, overflow)
        else raise "Unknow type: #{ type }"
        end

      if value_after_overflow != nil
        # new_value = value_after_overflow & (2**size - 1)
        yield value_after_overflow
      elsif value_after_overflow == nil && overflow == :fail
        return nil
      else
        yield value + incr
      end
    end

    def set_value(offset, size, new_value)
      size.times do |bit_index|
        bit_value = (new_value & (1 << (size - 1 - bit_index))) != 0 ? 1 : 0
        byte = offset >> 3
        bit = 7 - (offset & 0x7)
        byte_value = @string[byte]&.ord || 0
        byte_value &= ~(1 << bit)
        byte_value |= (bit_value << bit) # & 0xff
        @string[byte] = (byte_value & 0xff).chr
        offset += 1
      end

      new_value
    end
  end
end
