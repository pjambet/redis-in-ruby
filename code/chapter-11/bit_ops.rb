module BYORedis
  class BitOps
    def initialize(string)
      @string = string
    end

    def get_bit(offset)
      byte_position = offset / 8
      return 0 if byte_position >= @string.length

      bit_offset = offset & 7 # Equivalent to offset % 8
      (@string[byte_position].ord >> (7 - bit_offset)) & 1
    end
  end
end
