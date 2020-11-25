module BYORedis
  class BitOps
    def initialize(string)
      @string = string
    end

    def self.initialize_string_for_offset(string, offset)
      size = if offset == 0
               1
             elsif offset & 7 == 0
               offset / 8
             else
               (offset / 8) + 1
             end

      string << "\x00" * size
      string
    end

    def self.and(strings)
      result = nil

      strings.each do |other_string|
        next if other_string.nil?

        if result.nil?
          result = other_string
          next
        end

        i = 0
        while i < result.length || i < other_string.length
          res_byte = result[i]&.ord || 0
          other_byte = other_string[i]&.ord || 0

          result[i] = (res_byte & other_byte).chr
          i += 1
        end
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
