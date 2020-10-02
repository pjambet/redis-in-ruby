# coding: utf-8

require_relative './test_helper'
require_relative '../sorted_array'

describe BYORedis::SortedArray do
  TestStruct = Struct.new(:a, :timeout)

  describe 'push/<<' do
    it 'appends elements while keeping the array sorted' do
      sorted_array = new_array(:timeout)

      sorted_array << TestStruct.new('a', 1)
      sorted_array << TestStruct.new('b', 2)
      sorted_array << TestStruct.new('c', 10)
      sorted_array << TestStruct.new('d', 20)
      sorted_array << TestStruct.new('e', 15)
      sorted_array << TestStruct.new('f', 8)

      assert_equal(6, sorted_array.size)
      assert_equal(1, sorted_array[0].timeout)
      assert_equal(2, sorted_array[1].timeout)
      assert_equal(8, sorted_array[2].timeout)
      assert_equal(10, sorted_array[3].timeout)
      assert_equal(15, sorted_array[4].timeout)
      assert_equal(20, sorted_array[5].timeout)
    end
  end

  describe 'delete' do
    it 'deletes the element from the array' do
      sorted_array = new_array(:timeout)

      sorted_array << TestStruct.new('a', 10)
      sorted_array << TestStruct.new('b1', 20)
      sorted_array << TestStruct.new('b2', 20)
      sorted_array << TestStruct.new('b3', 20)
      sorted_array << TestStruct.new('c', 30) # array is now a, b3, b2, b1, c

      sorted_array.delete(TestStruct.new('d', 40)) # no-op
      sorted_array.delete(TestStruct.new('b1', 20))

      assert_equal(4, sorted_array.size)
      assert_equal(10, sorted_array[0].timeout)
      assert_equal(TestStruct.new('b3', 20), sorted_array[1])
      assert_equal(TestStruct.new('b2', 20), sorted_array[2])
      assert_equal(30, sorted_array[3].timeout)
    end
  end

  def new_array(field)
    BYORedis::SortedArray.new(field)
  end
end
