require 'minitest/autorun'

require_relative './test_helper'
require_relative '../int_set'

describe BYORedis::IntSet do
  describe 'add' do
    it 'returns true if the element is added' do
      set = new_set

      assert_equal(true, set.add(10))
      assert_equal(true, set.add(5))
      assert_equal(true, set.add(20))
      assert_equal(true, set.add(15))
      assert_equal(4, set.card)
    end

    it 'returns false if the element was already in the set' do
      set = new_set
      set.add(10)

      assert_equal(false, set.add(10))
      assert_equal(1, set.card)
    end
  end

  def new_set
    BYORedis::IntSet.new
  end
end
