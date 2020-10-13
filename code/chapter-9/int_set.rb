module BYORedis
  class IntSet

    INT16_MIN = -2**15 # -32,768
    INT16_MAX = 2**15 - 1 # 32,767
    INT32_MIN = -2**31 # -2,147,483,648
    INT32_MAX = 2**31 - 1 # 2,147,483,647
    INT64_MIN = -2**63 # -9,223,372,036,854,775,808
    INT64_MAX = 2**63 - 1 # 9,223,372,036,854,775,807

    # Each of the constant value represents the number of bytes used to store an integer
    ENCODING_16_BITS = 2
    ENCODING_32_BITS = 4
    ENCODING_64_BITS = 8

    def initialize
      @underlying_array = []
      @encoding = ENCODING_16_BITS
    end

    def empty?
      @underlying_array.empty?
    end

    def each(&block)
      members.each(&block)
    end

    def members
      size.times.map do |index|
        get(index)
      end
    end

    def size
      @underlying_array.size / @encoding
    end
    alias cardinality size
    alias card cardinality

    def add(member)
      raise "Member is not an int: #{ member }" unless member.is_a?(Integer)

      # Ruby's Integer can go over 64 bits, but this class can only store signed 64 bit integers
      # so we use this to reject out of range integers
      raise "Out of range integer: #{ member }" if member < INT64_MIN || member > INT64_MAX

      encoding = encoding_for_member(member)

      return upgrade_and_add(member) if encoding > @encoding

      # search always returns a value, either the position of the item or the position where it
      # should be inserted
      position = search(member)
      return false if get(position) == member

      move_tail(position, position + 1) if position < size

      set(position, member)

      true
    end

    def include?(member)
      return false if member.nil?

      index = search(member)
      get(index) == member
    end
    alias member? include?

    def pop
      rand_index = rand(size)
      value = get(rand_index)
      @underlying_array.slice!(rand_index * @encoding, @encoding)
      value
    end

    def random_member
      rand_index = rand(size)
      get(rand_index)
    end

    def remove(member)
      index = search(member)
      if get(index) == member
        @underlying_array.slice!(index * @encoding, @encoding)
        true
      else
        false
      end
    end

    private

    def set(position, member)
      @encoding.times do |i|
        index = (position * @encoding) + i
        @underlying_array[index] = ((member >> (i * 8)) & 0xff).chr
      end
    end

    def move_tail(from, to)
      @underlying_array[(to * @encoding)..-1] = @underlying_array[(from * @encoding)..-1]
    end

    def search(member)
      min = 0
      max = size - 1
      mid = -1
      current = -1

      # the index is always 0 for an empty array
      return 0 if empty?

      if member > get(max)
        return size
      elsif member < get(min)
        return 0
      end

      while max >= min
        mid = (min + max) >> 1
        current = get(mid)

        if member > current
          min = mid + 1
        elsif member < current
          max = mid - 1
        else
          break
        end
      end

      if member == current
        mid
      else
        min
      end
    end

    def get(position)
      get_with_encoding(position, @encoding)
    end

    def get_with_encoding(position, encoding)
      return nil if position >= size

      bytes = @underlying_array[position * encoding, encoding]

      # bytes is an array of bytes, in little endian, so with the small bytes first
      # We could iterate over the array and "assemble" the bytes into in a single integer,
      # by performing the opposite we did in set, that is with the following
      #
      # bytes.lazy.with_index.reduce(0) do |sum, (byte, index)|
      #   sum | (byte << (index * 8))
      # end
      #
      # But doing do would only work if the final result was positive, if the first bit of the
      # last byte was a 1, then the number we're re-assembling needs to be a negative number, we
      # could do so with the following:
      #
      # negative = (bytes[-1] >> 7) & 1 == 1
      #
      # And at the end of the method, we could apply the following logic to obtain the value,
      # get the 1 complement, with `~` and add 1. We also need to apply a mask to make sure that
      # the 1 complement result stays within the bounds of the current encoding
      # For instance, with encoding set to 2, the mask would be 0xffff, which is 65,535
      #
      # if negative
      #   mask = (2**(encoding * 8) - 1)
      #   v = -1 * ((~v & mask) + 1)
      # end
      #
      # Anyway, we can use the pack/unpack methods to let Ruby do that for us, calling
      # bytes.pack('C*') will return a string of bytes, for instance, the number -128 is stored
      # in the intset as [ 128, 255 ], calling, `.pack('C*')` returns "\x80\xFF". Next up, we
      # pick the right format, 's' for 16-bit integers, 'l' for 32 and 'q' for 64 and we let
      # Ruby put together the bytes into the final number.
      # The result of unpack is an array, but we use unpack1 here, which is a shortcut to
      # calling unpack() followed by [0]
      #
      # What this whole thing tells us is that we could have used `.pack('s').chars` in the
      # set method, but using >> 8 is more interesting to understand actually what happens!
      format = case encoding
               when ENCODING_16_BITS then 's'
               when ENCODING_32_BITS then 'l'
               when ENCODING_64_BITS then 'q'
               end

      bytes.join.unpack1(format)
    end

    def encoding_for_member(member)
      if member < INT32_MIN || member > INT32_MAX
        ENCODING_64_BITS
      elsif member < INT16_MIN || member > INT16_MAX
        ENCODING_32_BITS
      else
        ENCODING_16_BITS
      end
    end

    def upgrade_and_add(member)
      current_encoding = @encoding
      current_size = size
      new_size = current_size + 1
      @encoding = encoding_for_member(member)

      prepend = member < 0 ? 1 : 0
      @underlying_array[(new_size * @encoding) - 1] = nil # Allocate a bunch of nils

      # Upgrade back to front
      while (current_size -= 1) >= 0
        value = get_with_encoding(current_size, current_encoding)
        # Note the use of the prepend variable to shift all elements one cell to the right in
        # the case where we need to add the new member as the first element in the array
        set(current_size + prepend, value)
      end

      if prepend == 1
        set(0, member)
      else
        set(size - 1, member)
      end

      true
    end
  end
end
