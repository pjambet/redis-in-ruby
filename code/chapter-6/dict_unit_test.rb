require_relative './test_helper'
require_relative './dict'

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

    it 'prevents duplicates even while rehashing' do
      dict = new_dict([ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ], [ 'd', '4' ], [ 'e', '5' ],
                      [ 'f', '6' ], [ 'g', '7' ], [ 'h', '8' ])
      dict['i'] = '9' # Trigger rehashing with a 9th element
      # Find an element that has not been rehashed
      not_yet_rehashed_entry = dict.hash_tables[0].table.reject(&:nil?)[-1]

      # Override that entry, and make sure it does not incorrectly add it to the rehashing table
      # instead
      dict[not_yet_rehashed_entry.key] = 'something else'

      pairs = []
      dict.each do |key, value|
        pairs << [ key, value ]
      end

      # We only know at runtime which key we changed, so compute the expected result then
      expected_pairs = [ [ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ], [ 'd', '4' ], [ 'e', '5' ],
                         [ 'f', '6' ], [ 'g', '7' ], [ 'h', '8' ], [ 'i', '9' ] ]
      updated_entry = expected_pairs.find { |p| p[0] == not_yet_rehashed_entry.key }
      updated_entry[1] = 'something else'

      assert_equal(expected_pairs, pairs.sort)
    end
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
