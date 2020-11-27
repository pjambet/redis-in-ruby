module BYORedis
  class BitOps
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

    private

    # say byte is 111, so 0110 1111 in binary
    # with offset 0, we do 0110 1111 >> 7, 0000 0000 & 1 => 0
    # with offset 1, we do 0110 1111 >> 6, 0000 0001 & 1 => 1
    # with offset 2, we do 0110 1111 >> 5, 0000 0011 & 1 => 1
    # with offset 3, we do 0110 1111 >> 4, 0000 0110 & 1 => 0
    # etc ...
    def bit_at_offset(byte, bit_shift_offset)
      (byte >> bit_shift_offset) & 1
    end
  end
end
