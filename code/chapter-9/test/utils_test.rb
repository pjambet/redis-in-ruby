require_relative './test_helper'

describe BYORedis::Utils do
  describe 'string_to_integer' do
    it 'returns an error with an empty string' do
      assert_raises BYORedis::InvalidIntegerString do
        BYORedis::Utils.string_to_integer('')
      end
    end

    it 'returns an error for just a - sign' do
      assert_raises BYORedis::InvalidIntegerString do
        BYORedis::Utils.string_to_integer('-')
      end
    end

    it 'returns an error with a float' do
      error = assert_raises BYORedis::InvalidIntegerString do
        BYORedis::Utils.string_to_integer('1.0')
      end
      assert_equal("Not a number: '46' / '.'", error.message)
    end

    it 'returns an error with a leading zero' do
      assert_raises BYORedis::InvalidIntegerString do
        BYORedis::Utils.string_to_integer('01')
      end
      assert_raises BYORedis::InvalidIntegerString do
        BYORedis::Utils.string_to_integer('-01')
      end
    end

    it 'returns 0 with "0"' do
      assert_equal(0, BYORedis::Utils.string_to_integer('0'))
    end

    it 'works with a string representing a non overflowing integer' do
      assert_equal(256, BYORedis::Utils.string_to_integer('256'))
      assert_equal(9_223_372_036_854_775_807, BYORedis::Utils.string_to_integer('9223372036854775807')) # 2^63 - 1
    end

    it 'raises an overflow' do
      error = assert_raises BYORedis::IntegerOverflow do
        BYORedis::Utils.string_to_integer('18446744073709551616') # 2^64
      end
      assert_equal('Overflow before +', error.message)

      error = assert_raises BYORedis::IntegerOverflow do
        BYORedis::Utils.string_to_integer('9223372036854775808') # 2^63
      end
      assert_equal('Too big for a long long', error.message)

      error = assert_raises BYORedis::IntegerOverflow do
        BYORedis::Utils.string_to_integer('-9223372036854775809') # 2^63
      end
      assert_equal('Too small for a long long', error.message)

      error = assert_raises BYORedis::IntegerOverflow do
        BYORedis::Utils.string_to_integer('18446744073709551620') # 2^64 + 2
      end
      assert_equal('Overflow before *', error.message)
    end

    it 'handles negative numbers' do
      assert_equal(-1, BYORedis::Utils.string_to_integer('-1'))
      assert_equal(-9_223_372_036_854_775_808, BYORedis::Utils.string_to_integer('-9223372036854775808')) # -1 * 2^63
    end
  end

  describe 'integer_to_string' do
    it 'works with 0' do
      assert_equal('0', BYORedis::Utils.integer_to_string(0))
    end

    it 'works with positive numbers' do
      assert_equal('1', BYORedis::Utils.integer_to_string(1))
      assert_equal('9223372036854775807', BYORedis::Utils.integer_to_string(9_223_372_036_854_775_807))
    end

    it 'works with negative numbers' do
      assert_equal('-1', BYORedis::Utils.integer_to_string(-1))
      assert_equal('-9223372036854775808', BYORedis::Utils.integer_to_string(-9_223_372_036_854_775_808))
    end
  end
end
