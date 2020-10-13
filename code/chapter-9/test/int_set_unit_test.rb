require 'minitest/autorun'

require_relative './test_helper'
require_relative '../int_set'

describe BYORedis::IntSet do
  describe 'add' do
    it 'returns true if the element is added' do
      set = new_set

      assert_equal(true, set.add(10))
      assert_equal(true, set.add(256))
      assert_equal(true, set.add(5))
      assert_equal(true, set.add(20))
      assert_equal(true, set.add(15))
      assert_equal(5, set.card)
    end

    it 'returns false when the element already exists' do
      set = new_set

      assert_equal(true, set.add(10))
      assert_equal(true, set.add(-10))
      assert_equal(false, set.add(10))
      assert_equal(true, set.add(32_768))
      assert_equal(false, set.add(32_768))
      assert_equal(false, set.add(10))
    end

    it 'handles encoding updates' do
      set = new_set

      assert_equal(true, set.add(100))
      assert_equal(true, set.add(32_768))
      assert_equal(true, set.add(-2_147_483_649))
      assert(set.include?(-2_147_483_649))
      assert(set.include?(32_768))
      assert(set.include?(100))
    end
  end

  def new_set
    BYORedis::IntSet.new
  end
end
