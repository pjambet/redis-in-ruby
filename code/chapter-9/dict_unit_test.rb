require_relative './test_helper'
require_relative '../dict'

describe 'Dict' do
  describe 'set' do
    it 'adds a new pair if the key is not already present' do
      dict = new_dict

      assert_equal('1', dict['a'] = '1')
      assert_equal('1', dict['a'])
      assert_equal(1, dict.used)
    end

    it 'overrides the existing value if the key is already present' do
      dict = new_dict([ 'a', '1' ])

      assert_equal('2', dict['a'] = '2')
      assert_equal('2', dict['a'])
      assert_equal(1, dict.used)
    end

    it 'works and prevents duplicates, even while rehashing' do
      dict = new_dict([ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ])
      p dict
      # assert(false)
    end

  describe 'get' do
    it 'returns nil if the key is not present' do
      dict = new_dict

      assert_nil(dict['a'])
    end

    it 'returns the value if the key is present' do
      dict = new_dict([ 'a', '1' ])

      dict['b'] = '2'

      assert_equal('2', dict['b'])
    end
  end

  describe 'delete' do
    it 'returns nil if the key is not present' do
      dict = new_dict([ 'a', '1' ])

      assert_nil(dict.delete('b'))
    end

    it 'removes the key/value pair is the key is present' do
      dict = new_dict([ 'a', '1' ])

      assert_equal('1', dict.delete('a'))
      assert_nil(dict['a'])
      assert_equal(0, dict.used)
    end
  end

  describe 'each' do
    it 'iterates over all elements in the dict' do
      dict = new_dict([ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ])

      pairs = []
      dict.each do |key, value|
        pairs << [ key, value ]
      end

      assert_equal([ [ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ] ], pairs.sort)
    end

    it 'iterates over all elements in the dict while rehashing' do
      dict = new_dict([ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ], [ 'd', '4' ])
      dict['e'] = '5'

      pairs = []
      dict.each do |key, value|
        pairs << [ key, value ]
      end

      assert_equal([ [ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ], [ 'd', '4' ], [ 'e', '5' ] ], pairs.sort)
    end
  end

  def new_dict(*pairs)
    dict = BYORedis::Dict.new

    pairs.each do |pair|
      dict[pair[0]] = pair[1]
    end

    dict
  end
end
