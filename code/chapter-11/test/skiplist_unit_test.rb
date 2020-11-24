require_relative './test_helper'
require_relative '../skiplist'

describe BYORedis::SkipList do
  it 'can create an empty list' do
    list = new_list

    assert(list)
  end

  it 'can add elements to a list' do
    list = new_list

    list.insert(10, '10')
    list.insert(5, '5')
    list.insert(20, '20')
    list.insert(30, '30')
    list.insert(40, '40')
    list.insert(50, '50')
    list.insert(60, '60')
    list.insert(70, '70')
    list.insert(80, '80')
    list.insert(90, '90')
    list.insert(90, '90-2')
    list.insert(-1, 'neg one')

    assert_equal(12, list.length)
  end

  def new_list
    BYORedis::SkipList.new
  end
end
