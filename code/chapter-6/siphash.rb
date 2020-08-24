# Credit to https://github.com/emboss/siphash-ruby/blob/master/lib/siphash.rb
module SipHash

  def self.digest(key, msg)
    s = State.new(key)
    len = msg.size
    iter = len / 8

    iter.times do |i|
      m = msg.slice(i * 8, 8).unpack("Q<")[0]
      s.apply_block(m)
    end

    m = last_block(msg, len, iter)

    s.apply_block(m)
    s.finalize
    s.digest
  end

  private

  def self.last_block(msg, len, iter)
    last = (len << 56) & State::MASK_64;

    r = len % 8
    off = iter * 8

    last |= msg[off + 6].ord << 48 if r >= 7
    last |= msg[off + 5].ord << 40 if r >= 6
    last |= msg[off + 4].ord << 32 if r >= 5
    last |= msg[off + 3].ord << 24 if r >= 4
    last |= msg[off + 2].ord << 16 if r >= 3
    last |= msg[off + 1].ord << 8 if r >= 2
    last |= msg[off].ord if r >= 1
    last
  end

  class State

    MASK_64 = 0xffffffffffffffff

    def initialize(key)
      @v0 = 0x736f6d6570736575
      @v1 = 0x646f72616e646f6d
      @v2 = 0x6c7967656e657261
      @v3 = 0x7465646279746573

      k0 = key.slice(0, 8).unpack("Q<")[0]
      k1 = key.slice(8, 8).unpack("Q<")[0]

      @v0 ^= k0
      @v1 ^= k1
      @v2 ^= k0
      @v3 ^= k1
    end

    def apply_block(m)
      @v3 ^= m
      1.times { compress }
      @v0 ^= m
    end

    def rotl64(num, shift)
      ((num << shift) & MASK_64) | (num >> (64 - shift))
    end

    def compress
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

    def finalize
      @v2 ^= 0xff
      3.times { compress }
    end

    def digest
      @v0 ^ @v1 ^ @v2 ^ @v3
    end

  end
end
