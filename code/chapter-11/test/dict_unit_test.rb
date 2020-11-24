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

  describe 'random_entry' do
    it 'returns a random entry' do
      dict = new_dict([ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ])

      random_entry = dict.send(:random_entry)

      assert([ 'a', 'b', 'c' ].include?(random_entry.key))
    end

    it 'returns a random entry even while rehashing' do
      dict = new_dict([ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ], [ 'd', '4' ], [ 'e', '5' ],
                      [ 'f', '6' ], [ 'g', '7' ], [ 'h', '8' ])
      dict['i'] = '9' # Trigger rehashing with a 9th element
      dict.rehash_milliseconds(100)

      distribution = Hash.new { |h, k| h[k] = 0 }

      10_000.times do
        random_entry = dict.send(:random_entry)
        distribution[random_entry.key] += 1
      end

      distribution.each do |key, count|
        assert([ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i' ].include?(key))
        # 10,000 / 9 = 1,111.11
        # The delta is huge, because the default algorithm doesn't take into account that
        # different buckets have different sizes and the distribution is all over the place
        # Still, it test that all keys are returned at least some non zero times
        assert_in_delta(count, 1111, 1000)
      end
    end
  end

  describe 'get_some_entries' do
    it 'returns up to count random keys' do
      dict = new_dict([ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ], [ 'd', '4' ], [ 'e', '5' ],
                      [ 'f', '6' ], [ 'g', '7' ], [ 'h', '8' ])

      entries = dict.send(:get_some_entries, 2)
      assert_equal(2, entries.size)

      entries = dict.send(:get_some_entries, 20)
      assert_equal(8, entries.size)
    end
  end

  describe 'fair_random_entry' do
    it 'returns a random entry' do
      dict = new_dict([ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ])

      random_entry = dict.fair_random_entry

      assert([ 'a', 'b', 'c' ].include?(random_entry.key))
    end

    it 'returns a random entry even while rehashing' do
      dict = new_dict([ 'a', '1' ], [ 'b', '2' ], [ 'c', '3' ], [ 'd', '4' ], [ 'e', '5' ],
                      [ 'f', '6' ], [ 'g', '7' ], [ 'h', '8' ])
      dict['i'] = '9' # Trigger rehashing with a 9th element
      dict.rehash_milliseconds(100)

      distribution = Hash.new { |h, k| h[k] = 0 }

      10_000.times do
        random_entry = dict.fair_random_entry
        distribution[random_entry.key] += 1
      end

      distribution.each do |key, count|
        assert([ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i' ].include?(key))
        # 10,000 / 9 = 1,111.11
        # 100 is an arbitrary that seems to cover 90+% of the outcomes based on a non scientific
        # experiment I ran locally, just rant this "many" times (about 42 times)
        assert_in_delta(count, 1111, 100)
      end
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
