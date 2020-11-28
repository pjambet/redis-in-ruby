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
      p @string.unpack("B*")[0].chars.each_slice(8).to_a.map(&:join).join(' ')

      size.times do |bit_index|
        p "bit_offset=#{bit_offset}"
        byte = bit_offset >> 3 # divide by 8, get byte index
        p "byte=#{byte}"
        # 0x7 is 0111, so we essentially do bit_offset % 8
        bit = 7 - (bit_offset & 0x7) # get bit index from the left
        p "bit=#{bit}"
        byteval = bytes[byte]
        p "byteval=#{byteval}"
        bitval = (byteval >> bit) & 1
        p "bitval=#{bitval}"
        value = (value<<1) | bitval
        p "value=#{value}"
        bit_offset += 1
        p '---'
      end

      if type == :signed && (value & (1 << (size - 1))) != 0
        value = -1 * ((~value & (2**(size) - 1)) + 1)
      end

      value
    end

    def check_signed_overflow(new_value, size, overflow)
      p '--------'
      max = (1 << (size - 1)) - 1
      min = -max -1

      max_incr = max - new_value
      min_incr = min - new_value

      if new_value > max # || incr ...
        if overflow == :sat
          max
        end
      elsif new_value < min # || incr ...
        if overflow == :sat
          min
        end
      else
        nil
      end
    end

    def check_unsigned_overflow(new_value, size, overflow)
      max = (1 << size) - 1

      if new_value > max # incr
        if overflow == :sat
          max
        end
      elsif false # incr stuff with neg shit
      end
    end

    def set_op(offset, size, type, new_value, overflow)
      old_value = get_op(offset, size, type)
      p "old_value=#{ old_value }"

      res = set_value(offset, size, type, new_value, overflow)

      if res.nil?
        nil
      else
        old_value
      end
    end

    def incrby_op(offset, size, type, incr, overflow)
      p @string
      p @string.object_id
      old_value = get_op(offset, size, type)
      p "OLD VALUE: #{ old_value }"
      1
      new_value = old_value + incr
      res = set_value(offset, size, type, new_value, overflow)

      if res.nil?
        nil
      else
        new_value
      end
    end

    def set_value(offset, size, type, new_value, overflow)
      p '===='
      p type
      p type == :signed
      p @string

      # if type == :signed
      #   p "Converting #{ new_value } to #{ new_value & (2**size - 1) }"
      #   format = "%0#{size}b"
      #   p format % new_value
      #   new_value = new_value & (2**size - 1)
      #   p format % new_value
      # end
      if type == :unsigned && new_value < 0
        # Conversion from long long to uint64_t
        new_value = new_value & (2**64 - 1)
      end

      p type
      value_after_overflow =
        case type
        when :signed then check_signed_overflow(new_value, size, overflow)
        when :unsigned then check_unsigned_overflow(new_value, size, overflow)
        else raise "Unknow type: #{ type }"
        end

      p "NEW VALUE: #{ value_after_overflow }"
      if value_after_overflow != nil
        new_value = value_after_overflow & (2**size - 1)
      elsif value_after_overflow == nil && overflow == :fail
        return nil
      end

      # set field
      size.times do |bit_index|
        p 'playing with bits'
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
