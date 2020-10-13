# Credit to https://github.com/emboss/siphash-ruby/blob/master/lib/siphash.rb
class SipHash

  # Ruby's Integer class allows numbers by going passed the max value of a 64 bit integer,
  # by encoding the value across multiple 64-bit integers under the hood. In order to make
  # sure that the values we deal with stay within the 64-bit range, we use the following
  # constant as the right operand of an AND bitwise operation
  MASK_64 = 0xffffffffffffffff

  def self.digest(key, msg, compress_rounds: 1, finalize_rounds: 2)
    new(key, msg, compress_rounds, finalize_rounds).digest
  end

  def initialize(key, msg, compress_rounds, finalize_rounds)
    @msg = msg
    @compress_rounds = compress_rounds
    @finalize_rounds = finalize_rounds

    # These are the four 64-bit integers of internal state. The initial values are based on the
    # arbitrary string: "somepseudorandomlygeneratedbytes"
    # "somepseu".split('').map(&:ord).map { |b| b.to_s(16) }.join # => "736f6d6570736575"
    # The 64-bit value is 8317987319222330741, its binary represenation is:
    # 0111 0011 0110 1111 0110 1101 0110 0101 0111 0000 0111 0011 0110 0101 0111 0101
    # Which we can obtain with
    # "736f6d6570736575".scan(/../).map { |h| h.hex } # => [115, 111, 109, 101, 112, 115, 101, 117]
    # [115, 111, 109, 101, 112, 115, 101, 117].pack('c8') # => "somepseu"
    # "somepseu".unpack('Q>') # => 8317987319222330741
    # '%064b' % 8317987319222330741 # =>
    # 0111 0011 0110 1111 0110 1101 0110 0101 0111 0000 0111 0011 0110 0101 0111 0101
    #
    # Note that we used 'Q>' which tells unpack to assume big-endianness. Using the default of
    # Q< would have returned the same 8 bytes but in the opposite order, from right to left:
    # "somepseu".unpack('Q<') # => 8459294401660546931
    # '%064b' % 8459294401660546931 # =>
    # 0111 0101 0110 0101 0111 0011 0111 0000 0110 0101 0110 1101 0110 1111 0111 0011
    # These are the same bytes but in different order, the character s is the following 8 bits:
    # ('%08b' % 's'.ord).scan(/..../).join(' ') # => "0111 0011"
    # And the character o is:
    # ('%08b' % 'o'.ord).scan(/..../).join(' ') # => "0110 1111"
    #
    # We can see that in the big endian version, these two bytes are on the left side, and
    # they're on the right side with the little endian version
    #
    # "dorandom".split('').map(&:ord).map { |b| b.to_s(16) }.join # => "646f72616e646f6d"
    # "lygenera".split('').map(&:ord).map { |b| b.to_s(16) }.join # => "6c7967656e657261"
    # "tedbytes".split('').map(&:ord).map { |b| b.to_s(16) }.join # => "7465646279746573"
    @v0 = 0x736f6d6570736575
    @v1 = 0x646f72616e646f6d
    @v2 = 0x6c7967656e657261
    @v3 = 0x7465646279746573

    # The key argument is a 16 byte string, which we want to unpack to two 64-bit integers.
    # A byte contains 8 bits, so one 64-bit integer is composed of 8 bytes, and one 16 byte
    # string can be unpacked to two 64-bit integers.
    # The first line grabs the first 8 bytes with the slice method, and calls unpack with the
    # Q< argument. Q means that Ruby will attempt to unpack as a long, and < is the explicit
    # way of telling it to unpack it as little endian, which is the default on many modern CPUs
    k0 = key.slice(0, 8).unpack('Q<')[0]
    # This line does the same thing with the last 8 bytes of the key
    k1 = key.slice(8, 8).unpack('Q<')[0]

    # The ^ Ruby operator is the XOR bitwise operation, a ^= b is equivalent to a = a ^ b
    # These four lines initialize the four 64-bit integers of internal state with the two
    # parts of the secret, k0 and k1
    @v0 ^= k0
    @v1 ^= k1
    @v2 ^= k0
    @v3 ^= k1
  end

  def digest
    iter = @msg.size / 8

    # Compression step
    iter.times do |i|
      m = @msg.slice(i * 8, 8).unpack('Q<')[0]
      compress(m)
    end

    # Compression of the last characters
    m = last_block(iter)
    compress(m)

    # Finalization step
    finalize

    # Digest result
    @v0 ^ @v1 ^ @v2 ^ @v3
  end

  private

  # This function might look a little bit complicated at first, it implements this section of
  # the compression part of paper, 2.2:
  # where m w-1 includes the last 0 through 7 bytes of m followed by null bytes and endingwith a
  # byte encoding the positive integer b mod 256
  def last_block(iter)
    # We initialize last as a 64-bit integer where the leftmost byte, the 8 bits to the left are
    # set to the length of the input. 8 bits can only encode a value up to 255, so if the length
    # is more than 255, the extra bites are discarded, for a length of 11, last would look like
    # 0000 1011 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 000 00000
    # 11 is encoded as 1011 as binary, calling << 56 on it pushes it 56 bits to the left
    last = (@msg.size << 56) & MASK_64

    left = @msg.size % 8
    off = iter * 8

    # At this point, we've applied the compress step to all the 8 byte chunks in the input
    # string, but if the length was not a multiple of 8, there might be between 1 & 7 more bytes
    # we have not compressed yet.
    # For instance, if the string was 'hello world', the length is 11. The digest method would
    # have computed an iter value of 1, because 11 / 8 => 1, and called compress once, with the
    # first 8 bytes, obtained with the slice method, 'hello world'.slice(0, 8) => 'hello wo'
    # We still need to compress the last 3 bytes, 'rld'
    # In this example, left will be 3, and off 8
    # Note that this case/when statement is written to optimize for readability, there are ways
    # to express the same instructions with less code
    case left
    when 7
      last |= @msg[off + 6].ord << 48
      last |= @msg[off + 5].ord << 40
      last |= @msg[off + 4].ord << 32
      last |= @msg[off + 3].ord << 24
      last |= @msg[off + 2].ord << 16
      last |= @msg[off + 1].ord << 8
      last |= @msg[off].ord
    when 6
      last |= @msg[off + 5].ord << 40
      last |= @msg[off + 4].ord << 32
      last |= @msg[off + 3].ord << 24
      last |= @msg[off + 2].ord << 16
      last |= @msg[off + 1].ord << 8
      last |= @msg[off].ord
    when 5
      last |= @msg[off + 4].ord << 32
      last |= @msg[off + 3].ord << 24
      last |= @msg[off + 2].ord << 16
      last |= @msg[off + 1].ord << 8
      last |= @msg[off].ord
    when 4
      last |= @msg[off + 3].ord << 24
      last |= @msg[off + 2].ord << 16
      last |= @msg[off + 1].ord << 8
      last |= @msg[off].ord
    when 3
      # In the example documented above, this is the branch of the case/when we would end up in.
      # last is initially set to:
      # 0000 1011 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 000 00000
      # @msg[off + 2] is the character d, calling ord returns its integer value, 100. We shift
      # it 16 bits to the left, effectively inserting as the 6th byte starting from the left, or
      # third from the right:
      # 0000 1011 0000 0000 0000 0000 0000 0000 0000 0000 0110 0100 0000 0000 0000 0000
      last |= @msg[off + 2].ord << 16
      # @msg[off + 1]is the character l, ord returns 108, and we insert it as the 7th byte from
      # the left, or second from the right:
      # 0000 1011 0000 0000 0000 0000 0000 0000 0000 0000 0110 0100 0110 1100 0000 0000
      last |= @msg[off + 1].ord << 8
      # Finally, @msg[off] is the character r, with the ord value 100, and we insert it as the
      # byte from the left, or first from the right:
      # 0000 1011 0000 0000 0000 0000 0000 0000 0000 0000 0110 0100 0110 1100 0111 0010
      last |= @msg[off].ord
    when 2
      last |= @msg[off + 1].ord << 8
      last |= @msg[off].ord
    when 1
      last |= @msg[off].ord
    when 0
      last
    else
      raise "Something unexpected happened with the r value: #{ r }, should be between 0 & 7"
    end

    # Last is now a 64 bit integer containing the length of the input and the last bytes, if any:
    last
  end

  # rotl64 is the left rotation, also called circular shift:
  # https://en.wikipedia.org/wiki/Circular_shift
  def rotl64(num, shift)
    ((num << shift) & MASK_64) | (num >> (64 - shift))
  end

  # This is the main step of the siphash algorithm. A big difference with the C implementation
  # in the Redis codebase is the use of & MASK_64. As explained above, it is used to apply an
  # upper bounds to the results as Ruby would allow them to go past the max value of 64-bit
  # integer: 2^64 - 1
  def sip_round
    @v0 = (@v0 + @v1) & MASK_64
    @v2 = (@v2 + @v3) & MASK_64
    @v1 = rotl64(@v1, 13)
    @v3 = rotl64(@v3, 16)
    @v1 ^= @v0
    @v3 ^= @v2
    @v0 = rotl64(@v0, 32)
    @v2 = (@v2 + @v1) & MASK_64
    @v0 = (@v0 + @v3) & MASK_64
    @v1 = rotl64(@v1, 17)
    @v3 = rotl64(@v3, 21)
    @v1 ^= @v2
    @v3 ^= @v0
    @v2 = rotl64(@v2, 32)
  end

  def compress(m)
    @v3 ^= m
    @compress_rounds.times { sip_round }
    @v0 ^= m
  end

  def finalize
    @v2 ^= 0xff
    @finalize_rounds.times { sip_round }
  end
end
