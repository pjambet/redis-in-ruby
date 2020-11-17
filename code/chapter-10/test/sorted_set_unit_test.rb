require_relative './test_helper'
require_relative '../config'

describe BYORedis::RedisSortedSet do
  before do
    BYORedis::Config.reset_defaults!
  end

  it 'can create an empty set' do
    sorted_set = new_sorted_set

    assert(sorted_set)
  end

  describe 'when using a list under the hood' do
    it 'can add elements to a set' do
      sorted_set = new_sorted_set

      sorted_set.add(BigDecimal(10), '10')
      sorted_set.add(BigDecimal(20), 'twenty')
      sorted_set.add(BigDecimal(5.001, 4), 'five-ish')
      sorted_set.add(BigDecimal(5.001, 4), 'five')

      assert_equal(4, sorted_set.cardinality)
      assert_equal(
        [
          [ 'five', '5.001' ],
          [ 'five-ish', '5.001' ],
          [ '10', '10' ],
          [ 'twenty', '20' ],
        ], sorted_set_to_a(sorted_set)
      )
    end

    it 'reorders the set after a score update' do
      sorted_set = new_sorted_set
      sorted_set.add(10, '10')
      sorted_set.add(20, 'twenty')
      sorted_set.add(5, 'five')
      assert_equal(3, sorted_set.cardinality)
      assert_equal(
        [
          [ 'five', '5' ],
          [ '10', '10' ],
          [ 'twenty', '20' ],
        ], sorted_set_to_a(sorted_set)
      )

      sorted_set.add(21, '10') # Yeah, it's confusing. The member '10' now has the score 21

      assert_equal(3, sorted_set.cardinality)
      assert_equal(
        [
          [ 'five', '5' ],
          [ 'twenty', '20' ],
          [ '10', '21' ],
        ], sorted_set_to_a(sorted_set)
      )

      sorted_set.add(20, '10') # Yeah, it's confusing. The member '10' now has the score 20

      assert_equal(3, sorted_set.cardinality)
      assert_equal(
        [
          [ 'five', '5' ],
          [ '10', '20' ],
          [ 'twenty', '20' ],
        ], sorted_set_to_a(sorted_set)
      )

      sorted_set.add(100, 'five')
      assert_equal(3, sorted_set.cardinality)
      assert_equal(
        [
          [ '10', '20' ],
          [ 'twenty', '20' ],
          [ 'five', '100' ],
        ], sorted_set_to_a(sorted_set)
      )
    end

    it 'can remove elements from a set' do
      sorted_set = new_sorted_set

      sorted_set.add(10, 'ten')
      sorted_set.add(11, 'eleven')
      sorted_set.add(0, 'zero')

      sorted_set.remove('ten')

      assert_equal(2, sorted_set.cardinality)
      assert_equal(
        [
          [ 'zero', '0' ],
          [ 'eleven', '11' ],
        ], sorted_set_to_a(sorted_set)
      )
    end

    it 'can return a sub range of elements, sorted by score' do
      sorted_set = new_sorted_set

      sorted_set.add(10, 'ten')
      sorted_set.add(11, 'eleven')
      sorted_set.add(0, 'zero')

      assert_equal(3, sorted_set.cardinality)
      range_spec =
        BYORedis::RedisSortedSet::GenericRangeSpec.rank_range_spec(0, 1, sorted_set.cardinality)
      serializer = BYORedis::SortedSetRankSerializer.new(sorted_set, range_spec, withscores: true)
      assert_equal(
        BYORedis::RESPArray.new([ 'zero', '0', 'ten', '10' ]).serialize, serializer.serialize)

      range_spec =
        BYORedis::RedisSortedSet::GenericRangeSpec.rank_range_spec(2, 2, sorted_set.cardinality)
      serializer = BYORedis::SortedSetRankSerializer.new(sorted_set, range_spec, withscores: true)
      assert_equal(
        BYORedis::RESPArray.new([ 'eleven', '11' ]).serialize, serializer.serialize)
    end
  end

  describe 'when using a sorted_set, a dict and a sorted array, under the hood' do
    before do
      BYORedis::Config.set_config(:zset_max_ziplist_entries, '2')
    end

    it 'can add elements' do
      sorted_set = new_sorted_set

      sorted_set.add(10, 'ten')
      sorted_set.add(11, 'eleven')
      sorted_set.add(0, 'zero')

      assert_equal(3, sorted_set.cardinality)
      assert_equal(
        [
          [ 'zero', '0' ],
          [ 'ten', '10' ],
          [ 'eleven', '11' ],
        ], sorted_set_to_a(sorted_set)
      )
    end

    it 'maintains the order when updating the score of existing elements' do
      sorted_set = new_sorted_set

      sorted_set.add(10, 'ten')
      sorted_set.add(11, 'eleven')
      sorted_set.add(0, 'zero')

      sorted_set.add(-1, 'ten')

      assert_equal(3, sorted_set.cardinality)
      assert_equal(
        [
          [ 'ten', '-1' ],
          [ 'zero', '0' ],
          [ 'eleven', '11' ],
        ], sorted_set_to_a(sorted_set)
      )
    end

    it 'can delete elements' do
      sorted_set = new_sorted_set

      sorted_set.add(10, 'ten')
      sorted_set.add(11, 'eleven')
      sorted_set.add(0, 'zero')

      sorted_set.remove('ten')

      assert_equal(2, sorted_set.cardinality)
      assert_equal(
        [
          [ 'zero', '0' ],
          [ 'eleven', '11' ],
        ], sorted_set_to_a(sorted_set)
      )
    end

    it 'can return a sub range of element, sorted by score' do
      sorted_set = new_sorted_set

      sorted_set.add(10, 'ten')
      sorted_set.add(11, 'eleven')
      sorted_set.add(0, 'zero')

      assert_equal(3, sorted_set.cardinality)
      range_spec =
        BYORedis::RedisSortedSet::GenericRangeSpec.rank_range_spec(0, 1, sorted_set.cardinality)
      serializer = BYORedis::SortedSetRankSerializer.new(sorted_set, range_spec, withscores: true)
      assert_equal(
        BYORedis::RESPArray.new([ 'zero', '0', 'ten', '10' ]).serialize, serializer.serialize)

      range_spec =
        BYORedis::RedisSortedSet::GenericRangeSpec.rank_range_spec(2, 2, sorted_set.cardinality)
      serializer = BYORedis::SortedSetRankSerializer.new(sorted_set, range_spec, withscores: true)
      assert_equal(
        BYORedis::RESPArray.new([ 'eleven', '11' ]).serialize, serializer.serialize)
    end
  end


  it 'switches to a ZSet if the members are too long' do
    BYORedis::Config.set_config(:zset_max_ziplist_value, '2')
    sorted_set = new_sorted_set

    sorted_set.add(10, 't')
    assert_equal(BYORedis::List, sorted_set.underlying.class)

    sorted_set.add(10, 'more than two')
    assert_equal(BYORedis::ZSet, sorted_set.underlying.class)
  end

  def new_sorted_set
    BYORedis::RedisSortedSet.new
  end

  def sorted_set_to_a(sorted_set)
    members = []
    sorted_set.each do |pair|
      members << [ pair.member, BYORedis::Utils.float_to_string(pair.score) ]
    end
    members
  end
end
