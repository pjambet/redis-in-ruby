module BYORedis
  class BitOps
    def initialize(string)
      @string = string
    end

    def self.initialize_string_for_offset(offset)
      size = if offset == 0
               1
             elsif offset & 7 == 0
               offset / 8
             else
               (offset / 8) + 1
             end

      string = String.new('', capacity: size)
      string << "\x00" * size
      string
    end

    def get_bit(offset)
      byte_position = offset / 8
      p @string
      return 0 if @string && byte_position >= @string.length

      bit_shift_offset = 7 - (offset & 7) # Equivalent to 7 - offset % 8
      bit_at_offset(@string[byte_position].ord, bit_shift_offset)
    end

    def set_bit(offset, bit)
      byte_position = offset / 8
      bit_shift_offset = 7 - (offset & 7) # Equivalent to 7 - offset % 8

      if @string.empty?
        byte = @string[byte_position].ord
        old_value = 0
      elsif byte_position >= @string.length
        byte = ''
        old_value = 0
        raise "not done yet"
      else
        byte = @string[byte_position].ord
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
