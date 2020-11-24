# coding: utf-8

require_relative './test_helper'

describe 'Sorted Set Commands' do
  describe 'ZADD' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZADD', '-ERR wrong number of arguments for \'ZADD\' command' ],
        [ 'ZADD 2.0', '-ERR wrong number of arguments for \'ZADD\' command' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZADD z a a', '-ERR value is not a valid float' ],
        # Weirdly enough, if the score is a valid option, it returns a syntax error
        # and not a "not a valid float" error
        [ 'ZADD z nx a', '-ERR syntax error' ],
        [ 'ZADD z xx a', '-ERR syntax error' ],
        [ 'ZADD z ch a', '-ERR syntax error' ],
        [ 'ZADD z incr a', '-ERR syntax error' ],
        [ 'ZADD z 1.0 a 2.0', '-ERR syntax error' ],
        [ 'ZADD z 1.0 a incr b', '-ERR value is not a valid float' ],
        [ 'ZADD z 1.0 a inc b', '-ERR value is not a valid float' ],
        [ 'ZADD z NX inc 1.0 a 2.0 a', '-ERR syntax error' ], # Typo, inc instead of incr
        [ 'ZADD z NX XX 1.0 a 2.0 a', '-ERR XX and NX options at the same time are not compatible' ],
        [ 'ZADD z INCR 1.0 a 2.0 a', '-ERR INCR option supports a single increment-element pair' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZADD not-a-set 2.0 a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'creates a zset if needed' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '2' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 a', ':1' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'a', '2' ] ],
          [ 'TYPE z', '+zset' ],
        ]
      end
    end

    it 'adds or updates to the set/list' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 a 3.0 b', ':2' ],
          [ 'ZADD z 2.0 a 4.0 e 0.0 z', ':2' ],
          [ 'ZADD z 10.0 a 0.1 x 28e1 e', ':1' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'z', '0', 'x', '0.1', 'b', '3', 'a', '10', 'e', '280' ] ],
        ]
      end
    end

    it 'adds or updates the given elements' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 a 3.0 b', ':2' ],
          [ 'ZADD z 2.0 a 4.0 e 0.0 z', ':2' ],
          [ 'ZADD z 10.0 a 0.1 x 28e1 e', ':1' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'z', '0', 'x', '0.1', 'b', '3', 'a', '10', 'e', '280' ] ],
        ]
      end
    end

    it 'handles the NX option by not updating existing elements' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 a 3.0 b', ':2' ],
          [ 'ZADD z NX 1.0 a', ':0' ],
          [ 'ZADD z NX NX 1.0 a', ':0' ], # doesn't matter if NX is duplicated
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'a', '2', 'b', '3' ] ],
        ]
      end
    end

    it 'handles the XX option by only updating and never adding elements' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 a 3.0 b', ':2' ],
          [ 'ZADD z XX 1.0 a 10.0 z', ':0' ],
          [ 'ZADD z XX XX 1.0 a 10.0 z', ':0' ], # doesn't matter if XX is duplicated
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'a', '1', 'b', '3' ] ],
        ]
      end
    end

    it 'handles the CH option by returning the count of added and CHanged elements' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 a 3.0 b', ':2' ],
          [ 'ZADD z CH XX 1.0 a 10.0 z', ':1' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'a', '1', 'b', '3' ] ],
          [ 'ZADD z CH 11.0 a 10.0 z', ':2' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'b', '3', 'z', '10', 'a', '11' ] ],
        ]
      end
    end

    it 'handles the INCR option by incrementing the existing score' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 a 3.0 b', ':2' ],
          [ 'ZADD z INCR 1.0 a', '3' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'a', '3', 'b', '3' ] ],
          [ 'ZADD z INCR 1.1 a', '4.1' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'b', '3', 'a', '4.1' ] ],
          [ 'ZADD z INCR CH 1.1 a', '5.2' ], # CH is ignored with INCR
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'b', '3', 'a', '5.2' ] ],
          # There was a bug at some point where this would incorrectly add a new a instead of
          # updating it
          [ 'ZADD z 1 a', ':0' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'a', '1', 'b', '3' ] ],
          [ 'ZADD z INCR 1 1', '1' ],
          [ 'ZADD z INCR 1 1', '2' ],
          [ 'ZADD z INCR XX 1 1', '3' ],
          [ 'ZADD z INCR NX 1 1', BYORedis::NULL_BULK_STRING ],
          [ 'ZADD z INCR inf a', 'inf' ],
          [ 'ZADD z INCR -inf a', '-ERR resulting score is not a number (NaN)' ],
        ]
      end
    end

    it 'handles the INCR option by defaulting a non existing member to a 0 score' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z INCR 2.0 a', '2' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'a', '2' ] ],
          [ 'ZADD z INCR 1.1 a', '3.1' ],
          # There was a bug at some point where the Dict was not getting updated
          [ 'ZSCORE z a', '3.1' ],
          [ 'ZRANGE z 0 -1 WITHSCORES', [ 'a', '3.1' ] ],
        ]
      end
    end
  end

  describe 'ZCARD' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZCARD', '-ERR wrong number of arguments for \'ZCARD\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZCARD not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns 0 for a non existing zset' do
      assert_command_results [
        [ 'ZCARD z', ':0' ],
      ]
    end

    it 'returns the size of the zset' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 32 john 45.1 z', ':4' ],
          [ 'ZCARD z', ':4' ],
        ]
      end
    end
  end

  describe 'ZRANGE' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZRANGE', '-ERR wrong number of arguments for \'ZRANGE\' command' ],
        [ 'ZRANGE z', '-ERR wrong number of arguments for \'ZRANGE\' command' ],
        [ 'ZRANGE z 0', '-ERR wrong number of arguments for \'ZRANGE\' command' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZRANGE z 0 1 WITHSCORE', '-ERR syntax error' ],
        [ 'ZRANGE z 0 1 WITHSCORES a', '-ERR syntax error' ],
      ]
    end

    it 'validates that start and stop are integers' do
      assert_command_results [
        [ 'ZRANGE z a 1 WITHSCORE', '-ERR value is not an integer or out of range' ],
        [ 'ZRANGE z 0 a WITHSCORES a', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZRANGE not-a-set 0 -1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the whole sorted set with 0 -1' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e 4.0 d', ':4' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'd', 'e' ] ],
        ]
      end
    end

    it 'returns the whole sorted including set with 0 -1 and withscores' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e 4.0 d', ':4' ],
          [ 'ZRANGE z 0 -1 withscores', [ 'a', '1', 'b', '2', 'd', '4', 'e', '5.1' ] ],
        ]
      end
    end

    it 'handles negative indexes as starting from the right side' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e', ':3' ],
          [ 'ZRANGE z -3 2', [ 'a', 'b', 'e' ] ],
          [ 'ZRANGE z -2 1', [ 'b' ] ],
          [ 'ZRANGE z -2 2', [ 'b', 'e' ] ],
          [ 'ZRANGE z -1 2', [ 'e' ] ],
        ]
      end
    end

    it 'works with out of bounds indices' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e', ':3' ],
          [ 'ZRANGE z 2 22', [ 'e' ] ],
          [ 'ZRANGE z -6 0', [ 'a' ] ],
        ]
      end
    end

    it 'returns an empty array for out of order boundaries' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b', ':1' ],
          [ 'ZRANGE z 2 1', [] ],
          [ 'ZRANGE z -1 -2', [] ],
        ]
      end
    end

    it 'returns subsets' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e 10.1 f 200.1 z', ':5' ],
          [ 'ZRANGE z 1 1', [ 'b' ] ],
          [ 'ZRANGE z 1 3', [ 'b', 'e', 'f' ] ],
          [ 'ZRANGE z 3 4', [ 'f', 'z' ] ],
          [ 'ZRANGE z 3 100', [ 'f', 'z' ] ],
        ]
      end
    end
  end

  describe 'ZRANGEBYLEX' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZRANGEBYLEX', '-ERR wrong number of arguments for \'ZRANGEBYLEX\' command' ],
        [ 'ZRANGEBYLEX z', '-ERR wrong number of arguments for \'ZRANGEBYLEX\' command' ],
        [ 'ZRANGEBYLEX z a', '-ERR wrong number of arguments for \'ZRANGEBYLEX\' command' ],
        [ 'ZRANGEBYLEX z a', '-ERR wrong number of arguments for \'ZRANGEBYLEX\' command' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZRANGEBYLEX z a b LIMIT', '-ERR min or max not valid string range item' ],
        [ 'ZRANGEBYLEX z [a [b LIMIT', '-ERR syntax error' ],
        [ 'ZRANGEBYLEX z [a [b LIMIT a', '-ERR syntax error' ],
        [ 'ZRANGEBYLEX z [a [b LIMIT 0', '-ERR syntax error' ],
        [ 'ZRANGEBYLEX z [a [b LIMIT 0 a', '-ERR value is not an integer or out of range' ],
        [ 'ZRANGEBYLEX z [a [b LIMIT a 1', '-ERR value is not an integer or out of range' ],
        [ 'ZRANGEBYLEX z [a [b LIMIT 0 1 a', '-ERR syntax error' ],
      ]
    end

    it 'validates the format of min and max' do
      assert_command_results [
        [ 'ZRANGEBYLEX z a b', '-ERR min or max not valid string range item' ],
        [ 'ZRANGEBYLEX z [a b', '-ERR min or max not valid string range item' ],
        [ 'ZRANGEBYLEX z a (b', '-ERR min or max not valid string range item' ],
        [ 'ZRANGEBYLEX z - b', '-ERR min or max not valid string range item' ],
        [ 'ZRANGEBYLEX z a +', '-ERR min or max not valid string range item' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZRANGEBYLEX not-a-set [a [b', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns all elements with - +' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z', ':5' ],
          [ 'ZRANGEBYLEX z - +', [ 'a', 'b', 'e', 'f', 'z' ] ],
        ]
      end
    end

    it 'returns an empty array with + -' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z', ':5' ],
          [ 'ZRANGEBYLEX z + -', [] ],
        ]
      end
    end

    it 'returns all elements in the range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':7' ],
          [ 'ZRANGEBYLEX z - [e', [ 'a', 'b', 'bb', 'bbb', 'e' ] ],
          [ 'ZRANGEBYLEX z - (e', [ 'a', 'b', 'bb', 'bbb' ] ],
          [ 'ZRANGEBYLEX z [b [e', [ 'b', 'bb', 'bbb', 'e' ] ],
          [ 'ZRANGEBYLEX z (b [e', [ 'bb', 'bbb', 'e' ] ],
          [ 'ZRANGEBYLEX z (bb [e', [ 'bbb', 'e' ] ],
          [ 'ZRANGEBYLEX z (b +', [ 'bb', 'bbb', 'e', 'f', 'z' ] ],
          [ 'ZRANGEBYLEX z [b +', [ 'b', 'bb', 'bbb', 'e', 'f', 'z' ] ],
          [ 'ZRANGEBYLEX z [bbb +', [ 'bbb', 'e', 'f', 'z' ] ],
        ]
      end
    end

    it 'handles the limit offset count options' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':7' ],
          [ 'ZRANGEBYLEX z - [e LIMIT 0 2', [ 'a', 'b' ] ],
          [ 'ZRANGEBYLEX z - [e LIMIT 1 2', [ 'b', 'bb' ] ],
          [ 'ZRANGEBYLEX z - [e LIMIT 2 2', [ 'bb', 'bbb' ] ],
          [ 'ZRANGEBYLEX z - [e LIMIT 3 2', [ 'bbb', 'e' ] ],
          [ 'ZRANGEBYLEX z - [e LIMIT 4 2', [ 'e' ] ],
          [ 'ZRANGEBYLEX z - [e LIMIT 5 2', [] ],
        ]
      end
    end

    it 'handles the limit offset count options with a negative count' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':7' ],
          [ 'ZRANGEBYLEX z - (e LIMIT 0 -1', [ 'a', 'b', 'bb', 'bbb' ] ],
          [ 'ZRANGEBYLEX z - (e LIMIT 0 -2', [ 'a', 'b', 'bb', 'bbb' ] ],
        ]
      end
    end
  end

  describe 'ZRANGEBYSCORE' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZRANGEBYSCORE', '-ERR wrong number of arguments for \'ZRANGEBYSCORE\' command' ],
        [ 'ZRANGEBYSCORE z', '-ERR wrong number of arguments for \'ZRANGEBYSCORE\' command' ],
        [ 'ZRANGEBYSCORE z m', '-ERR wrong number of arguments for \'ZRANGEBYSCORE\' command' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZRANGEBYSCORE z a b', '-ERR min or max is not a float' ],
        [ 'ZRANGEBYSCORE z a b withscore', '-ERR min or max is not a float' ],
        [ 'ZRANGEBYSCORE z a b limit', '-ERR min or max is not a float' ],
        [ 'ZRANGEBYSCORE z a b limit a', '-ERR min or max is not a float' ],
        [ 'ZRANGEBYSCORE z a b limit a b', '-ERR min or max is not a float' ],
        [ 'ZRANGEBYSCORE z 0 1 limi', '-ERR syntax error' ],
        [ 'ZRANGEBYSCORE z 0 1 limit', '-ERR syntax error' ],
        [ 'ZRANGEBYSCORE z 0 1 limit a', '-ERR syntax error' ],
        [ 'ZRANGEBYSCORE z 0 1 limit 0', '-ERR syntax error' ],
        [ 'ZRANGEBYSCORE z 0 1 limit a b', '-ERR value is not an integer or out of range' ],
        [ 'ZRANGEBYSCORE z 0 1 limit 0 b', '-ERR value is not an integer or out of range' ],
        [ 'ZRANGEBYSCORE z 0 1 limit a 0', '-ERR value is not an integer or out of range' ],
        [ 'ZRANGEBYSCORE z 0 1 withscor', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZRANGEBYSCORE not-a-set 0 -1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an empty array for nonsensical ranges' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZRANGEBYSCORE z 1 0', [] ],
        ]
      end
    end

    it 'returns all the elements with a score in the range (inclusive), ordered low to high' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZRANGEBYSCORE z 0 1', [] ],
          [ 'ZRANGEBYSCORE z 1 4', [ 'a', 'b', 'bb', 'bbb' ] ],
          [ 'ZRANGEBYSCORE z 5 100', [ 'e', 'f', 'z' ] ],
        ]
      end
    end

    it 'includes the scores with the withscores options' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZRANGEBYSCORE z 0 1 WITHSCORES', [] ],
          [ 'ZRANGEBYSCORE z 1 4 WITHSCORES', [ 'a', '1.1', 'b', '2.2', 'bb', '2.22', 'bbb', '2.222' ] ],
          [ 'ZRANGEBYSCORE z 5 100 WITHSCORES', [ 'e', '5.97', 'f', '6.12345', 'z', '26.2' ] ],
        ]
      end
    end

    it 'supports -inf and +inf as min/max values' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZRANGEBYSCORE z -inf 1', [] ],
          [ 'ZRANGEBYSCORE z -infinity 1', [] ],
          [ 'ZRANGEBYSCORE z -inf 4', [ 'a', 'b', 'bb', 'bbb' ] ],
          [ 'ZRANGEBYSCORE z -infinity 4', [ 'a', 'b', 'bb', 'bbb' ] ],
          [ 'ZRANGEBYSCORE z -infinity infinity', [ 'a', 'b', 'bb', 'bbb', 'e', 'f', 'z' ] ],
          [ 'ZRANGEBYSCORE z 5 inf', [ 'e', 'f', 'z' ] ],
          [ 'ZRANGEBYSCORE z 5 +inf', [ 'e', 'f', 'z' ] ],
          [ 'ZRANGEBYSCORE z 5 infinity', [ 'e', 'f', 'z' ] ],
          [ 'ZRANGEBYSCORE z 5 +infinity', [ 'e', 'f', 'z' ] ],
        ]
      end
    end

    it 'filters the result with the limit (offset/count) options' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZRANGEBYSCORE z 1 4 LIMIT 0 1', [ 'a' ] ],
          [ 'ZRANGEBYSCORE z 1 4 LIMIT 1 2', [ 'b', 'bb' ] ],
        ]
      end
    end

    it 'handles both limit and withscores options' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZRANGEBYSCORE z 1 4 WITHSCORES LIMIT 0 1', [ 'a', '1.1' ] ],
          [ 'ZRANGEBYSCORE z 1 4 LIMIT 1 2 WITHSCORES', [ 'b', '2.2', 'bb', '2.22' ] ],
        ]
      end
    end

    it 'accepts exclusive intervals' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZRANGEBYSCORE z 1 (2.2', [ 'a' ] ],
          [ 'ZRANGEBYSCORE z 1 (2.22', [ 'a', 'b' ] ],
        ]
      end
    end
  end

  describe 'ZINTER' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZINTER', '-ERR wrong number of arguments for \'ZINTER\' command' ],
        [ 'ZINTER a', '-ERR wrong number of arguments for \'ZINTER\' command' ],
        [ 'ZINTER 1', '-ERR wrong number of arguments for \'ZINTER\' command' ],
        [ 'ZINTER 2 z', '-ERR syntax error' ],
        [ 'ZINTER 2 z1 z2 z3', '-ERR syntax error' ],
        [ 'ZINTER 0', '-ERR wrong number of arguments for \'ZINTER\' command' ],
        [ 'ZINTER 0 z', '-ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE' ],
        [ 'ZINTER -1 z1', '-ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZINTER a z1', '-ERR value is not an integer or out of range' ],
        [ 'ZINTER 2 z1 z2 WEIGHTS', '-ERR syntax error' ],
        [ 'ZINTER 2 z1 z2 WEIGHTS 0', '-ERR syntax error' ],
        [ 'ZINTER 2 z1 z2 WEIGHTS 0 0 0', '-ERR syntax error' ],
        [ 'ZINTER 2 z1 z2 AGGREGATE', '-ERR syntax error' ],
        [ 'ZINTER 2 z1 z2 AGGREGATE A', '-ERR syntax error' ],
        [ 'ZINTER 2 z1 z2 AGGREGATE SUM A', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZINTER 1 not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an empty array if any of the sets do not exist' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZINTER 2 z1 z2', [] ],
        ]
      end
    end

    it 'returns the full set with a single input' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZINTER 1 z1', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    it 'returns the intersection of all the sets' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTER 2 z1 z2', [ 'a', 'b' ] ],
          [ 'ZINTER 2 z1 z1', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    it 'includes the scores with withscores' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTER 2 z1 z2 WITHSCORES', [ 'a', '101.1', 'b', '202.2' ] ],
        ]
      end
    end

    it 'returns the intersection summing scores with aggregate sum' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTER 2 z1 z2 WITHSCORES AGGREGATE SUM', [ 'a', '101.1', 'b', '202.2' ] ],
        ]
      end
    end

    it 'returns the intersection with the min score with aggregate min' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTER 2 z1 z2 WITHSCORES AGGREGATE MIN', [ 'a', '1.1', 'b', '2.2' ] ],
        ]
      end
    end

    it 'returns the intersection with the max score with aggregate max' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTER 2 z1 z2 WITHSCORES AGGREGATE MAX', [ 'a', '100', 'b', '200' ] ],
        ]
      end
    end

    it 'uses the weights multiplier with the weights option' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTER 2 z1 z2 WITHSCORES AGGREGATE SUM WEIGHTS 2 2', [ 'a', '202.2', 'b', '404.4' ] ],
          [ 'ZINTER 2 z1 z2 WITHSCORES AGGREGATE MIN WEIGHTS 2 2', [ 'a', '2.2', 'b', '4.4' ] ],
          [ 'ZINTER 2 z1 z2 WITHSCORES AGGREGATE MAX WEIGHTS 2 2', [ 'a', '200', 'b', '400' ] ],
          [ 'ZINTER 2 z1 z2 WITHSCORES AGGREGATE MAX WEIGHTS 2 inf', [ 'a', 'inf', 'b', 'inf' ] ],
        ]
      end
    end

    it 'handles infinity * 0 with the weights option' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 0 b 0 a 1 c', ':3' ],
          [ 'ZADD z2 1 b 2 a 3 c', ':3' ],
          [ 'ZINTER 2 z1 z2 WITHSCORES WEIGHTS inf 1', [ 'b', '1', 'a', '2', 'c', 'inf' ] ],
          [ 'ZINTER 2 z1 z2 WITHSCORES WEIGHTS inf 1 AGGREGATE MIN', [ 'a', '0', 'b', '0', 'c', '3' ] ],
          [ 'ZINTER 2 z1 z2 WITHSCORES WEIGHTS inf 1 AGGREGATE MAX', [ 'b', '1', 'a', '2', 'c', 'inf' ] ],
        ]
      end
    end

    it 'accepts regular sets as inputs with a default score of 1.0' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'SADD z2 bb b a', ':3' ],
          [ 'ZINTER 2 z1 z2 WITHSCORES', [ 'a', '2.1', 'b', '3.2' ] ],
        ]
      end
    end

    it 'converts NaNs to 0 when aggregating' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 inf a -inf b', ':2' ],
          [ 'ZADD z2 -inf a inf b', ':2' ],
          [ 'ZINTER 2 z1 z2 WITHSCORES', [ 'a', '0', 'b', '0' ] ],
        ]
      end
    end
  end

  describe 'ZINTERSTORE' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZINTERSTORE', '-ERR wrong number of arguments for \'ZINTERSTORE\' command' ],
        [ 'ZINTERSTORE d', '-ERR wrong number of arguments for \'ZINTERSTORE\' command' ],
        [ 'ZINTERSTORE 1', '-ERR wrong number of arguments for \'ZINTERSTORE\' command' ],
        [ 'ZINTERSTORE d 1', '-ERR wrong number of arguments for \'ZINTERSTORE\' command' ],
        [ 'ZINTERSTORE d 2 z', '-ERR syntax error' ],
        [ 'ZINTERSTORE d 2 z1 z2 z3', '-ERR syntax error' ],
        [ 'ZINTERSTORE d 0', '-ERR wrong number of arguments for \'ZINTERSTORE\' command' ],
        [ 'ZINTERSTORE d 0 z', '-ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE' ],
        [ 'ZINTERSTORE d -1 z1', '-ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZINTERSTORE d a z1', '-ERR value is not an integer or out of range' ],
        [ 'ZINTERSTORE d 2 z1 z2 WEIGHTS', '-ERR syntax error' ],
        [ 'ZINTERSTORE d 2 z1 z2 WEIGHTS 0', '-ERR syntax error' ],
        [ 'ZINTERSTORE d 2 z1 z2 WEIGHTS 0 0 0', '-ERR syntax error' ],
        [ 'ZINTERSTORE d 2 z1 z2 AGGREGATE', '-ERR syntax error' ],
        [ 'ZINTERSTORE d 2 z1 z2 AGGREGATE A', '-ERR syntax error' ],
        [ 'ZINTERSTORE d 2 z1 z2 AGGREGATE SUM A', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZINTERSTORE dest 1 not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns 0 and does not create a new set if any of the sets do not exist' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZINTERSTORE dest 2 z1 z2', ':0' ],
          [ 'TYPE dest', '+none' ],
        ]
      end
    end

    it 'returns the full set with a single input' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZINTERSTORE dest 1 z1', ':4' ],
          [ 'ZRANGE dest 0 -1', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    # Weird, but hey, that's how it is
    it 'accepts but ignores the WITHSCORES option' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZINTERSTORE dest 1 z1 WITHSCORES', ':4' ],
          [ 'ZRANGE dest 0 -1', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    it 'returns the intersection of all the sets' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTERSTORE dest 2 z1 z2', ':2' ],
          [ 'ZRANGE dest 0 -1', [ 'a', 'b' ] ],
        ]
      end
    end

    it 'returns the intersection summing scores with aggregate sum' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTERSTORE dest 2 z1 z2 AGGREGATE SUM', ':2' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'a', '101.1', 'b', '202.2' ] ],
        ]
      end
    end

    it 'returns the intersection with the min score with aggregate min' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTERSTORE dest 2 z1 z2 AGGREGATE MIN', ':2' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'a', '1.1', 'b', '2.2' ] ],
        ]
      end
    end

    it 'returns the intersection with the max score with aggregate max' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTERSTORE dest 2 z1 z2 AGGREGATE MAX', ':2' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'a', '100', 'b', '200' ] ],
        ]
      end
    end

    it 'uses the weights multiplier with the weights option' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZINTERSTORE dest 2 z1 z2 AGGREGATE SUM WEIGHTS 2 2', ':2' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'a', '202.2', 'b', '404.4' ] ],
          [ 'ZINTERSTORE dest 2 z1 z2 AGGREGATE MIN WEIGHTS 2 2', ':2' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'a', '2.2', 'b', '4.4' ] ],
          [ 'ZINTERSTORE dest 2 z1 z2 AGGREGATE MAX WEIGHTS 2 2', ':2' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'a', '200', 'b', '400' ] ],
        ]
      end
    end

    it 'accepts regular sets as inputs with a default score of 1.0' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'SADD z2 bb b a', ':3' ],
          [ 'ZINTERSTORE dest 2 z1 z2', ':2' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'a', '2.1', 'b', '3.2' ] ],
        ]
      end
    end
  end

  describe 'ZUNION' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZUNION', '-ERR wrong number of arguments for \'ZUNION\' command' ],
        [ 'ZUNION a', '-ERR wrong number of arguments for \'ZUNION\' command' ],
        [ 'ZUNION 1', '-ERR wrong number of arguments for \'ZUNION\' command' ],
        [ 'ZUNION 2 z', '-ERR syntax error' ],
        [ 'ZUNION 2 z1 z2 z3', '-ERR syntax error' ],
        [ 'ZUNION 0', '-ERR wrong number of arguments for \'ZUNION\' command' ],
        [ 'ZUNION 0 z', '-ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE' ],
        [ 'ZUNION -1 z1', '-ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZUNION a z1', '-ERR value is not an integer or out of range' ],
        [ 'ZUNION 2 z1 z2 WEIGHTS', '-ERR syntax error' ],
        [ 'ZUNION 2 z1 z2 WEIGHTS 0', '-ERR syntax error' ],
        [ 'ZUNION 2 z1 z2 WEIGHTS 0 0 0', '-ERR syntax error' ],
        [ 'ZUNION 2 z1 z2 AGGREGATE', '-ERR syntax error' ],
        [ 'ZUNION 2 z1 z2 AGGREGATE A', '-ERR syntax error' ],
        [ 'ZUNION 2 z1 z2 AGGREGATE SUM A', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZUNION 1 not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'ignores empty inputs' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZUNION 2 z1 z2', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    it 'returns the full set with a single input' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZUNION 1 z1', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    it 'returns the union of all the sets' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNION 2 z1 z2', [ 'bb', 'bbb', 'e', 'f', 'z', 'a', 'b' ] ],
          [ 'ZUNION 2 z1 z1', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    it 'includes the scores with withscores' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNION 2 z1 z2 WITHSCORES', [ 'bb', '2.22', 'bbb', '2.222', 'e', '5.97', 'f', '6.12345', 'z', '26.2', 'a', '101.1', 'b', '202.2' ] ],
        ]
      end
    end

    it 'returns the union summing scores with aggregate sum' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNION 2 z1 z2 WITHSCORES AGGREGATE SUM', [ 'bb', '2.22', 'bbb', '2.222', 'e', '5.97', 'f', '6.12345', 'z', '26.2', 'a', '101.1', 'b', '202.2' ] ],
        ]
      end
    end

    it 'returns the union with the min score with aggregate min' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNION 2 z1 z2 WITHSCORES AGGREGATE MIN', [ 'a', '1.1', 'b', '2.2', 'bb', '2.22', 'bbb', '2.222', 'e', '5.97', 'f', '6.12345', 'z', '26.2' ] ],
        ]
      end
    end

    it 'returns the union with the max score with aggregate max' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNION 2 z1 z2 WITHSCORES AGGREGATE MAX', [ 'bb', '2.22', 'bbb', '2.222', 'e', '5.97', 'f', '6.12345', 'z', '26.2', 'a', '100', 'b', '200' ] ],
        ]
      end
    end

    it 'uses the weights multiplier with the weights option' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNION 2 z1 z2 WITHSCORES AGGREGATE SUM WEIGHTS 2 2', [ 'bb', '4.44', 'bbb', '4.444', 'e', '11.94', 'f', '12.2469', 'z', '52.4', 'a', '202.2', 'b', '404.4' ] ],
          [ 'ZUNION 2 z1 z2 WITHSCORES AGGREGATE MIN WEIGHTS 2 2', [ 'a', '2.2', 'b', '4.4', 'bb', '4.44', 'bbb', '4.444', 'e', '11.94', 'f', '12.2469', 'z', '52.4' ] ],
          [ 'ZUNION 2 z1 z2 WITHSCORES AGGREGATE MAX WEIGHTS 2 2', [ 'bb', '4.44', 'bbb', '4.444', 'e', '11.94', 'f', '12.2469', 'z', '52.4', 'a', '200', 'b', '400' ] ],
        ]
      end
    end

    it 'accepts regular sets as inputs with a default score of 1.0' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'SADD z2 z bb bbb b a', ':5' ],
          [ 'ZUNION 2 z1 z2 WITHSCORES', [ 'bb', '1', 'bbb', '1', 'z', '1', 'a', '2.1', 'b', '3.2', 'e', '5.97', 'f', '6.12345' ] ],
        ]
      end
    end

    it 'handles infinity * 0 with the weights option' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 0 b 0 a 1 c', ':3' ],
          [ 'ZADD z2 1 b 2 a 3 c', ':3' ],
          [ 'ZUNION 2 z1 z2 WITHSCORES WEIGHTS inf 1', [ 'b', '1', 'a', '2', 'c', 'inf' ] ],
          [ 'ZUNION 2 z1 z2 WITHSCORES WEIGHTS inf 1 AGGREGATE MIN', [ 'a', '0', 'b', '0', 'c', '3' ] ],
          [ 'ZUNION 2 z1 z2 WITHSCORES WEIGHTS inf 1 AGGREGATE MAX', [ 'b', '1', 'a', '2', 'c', 'inf' ] ],
        ]
      end
    end

    it 'converts NaNs to 0 when aggregating' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 inf a -inf b', ':2' ],
          [ 'ZADD z2 -inf a inf b', ':2' ],
          [ 'ZUNION 2 z1 z2 WITHSCORES', [ 'a', '0', 'b', '0' ] ],
        ]
      end
    end
  end

  describe 'ZUNIONSTORE' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZUNIONSTORE', '-ERR wrong number of arguments for \'ZUNIONSTORE\' command' ],
        [ 'ZUNIONSTORE d', '-ERR wrong number of arguments for \'ZUNIONSTORE\' command' ],
        [ 'ZUNIONSTORE 1', '-ERR wrong number of arguments for \'ZUNIONSTORE\' command' ],
        [ 'ZUNIONSTORE d 1', '-ERR wrong number of arguments for \'ZUNIONSTORE\' command' ],
        [ 'ZUNIONSTORE d 2 z', '-ERR syntax error' ],
        [ 'ZUNIONSTORE d 2 z1 z2 z3', '-ERR syntax error' ],
        [ 'ZUNIONSTORE d 0', '-ERR wrong number of arguments for \'ZUNIONSTORE\' command' ],
        [ 'ZUNIONSTORE d 0 z', '-ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE' ],
        [ 'ZUNIONSTORE d -1 z1', '-ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZUNIONSTORE d a z1', '-ERR value is not an integer or out of range' ],
        [ 'ZUNIONSTORE d 2 z1 z2 WEIGHTS', '-ERR syntax error' ],
        [ 'ZUNIONSTORE d 2 z1 z2 WEIGHTS 0', '-ERR syntax error' ],
        [ 'ZUNIONSTORE d 2 z1 z2 WEIGHTS 0 0 0', '-ERR syntax error' ],
        [ 'ZUNIONSTORE d 2 z1 z2 AGGREGATE', '-ERR syntax error' ],
        [ 'ZUNIONSTORE d 2 z1 z2 AGGREGATE A', '-ERR syntax error' ],
        [ 'ZUNIONSTORE d 2 z1 z2 AGGREGATE SUM A', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZUNIONSTORE dest 1 not-a-set', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'ignores non existing sorted sets' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZUNIONSTORE dest 2 z1 z2', ':4' ],
          [ 'ZRANGE dest 0 -1', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    it 'returns the full set with a single input' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZUNIONSTORE dest 1 z1', ':4' ],
          [ 'ZRANGE dest 0 -1', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    # Weird, but hey, that's how it is
    it 'accepts but ignores the WITHSCORES option' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZUNIONSTORE dest 1 z1 WITHSCORES', ':4' ],
          [ 'ZRANGE dest 0 -1', [ 'a', 'b', 'e', 'f' ] ],
        ]
      end
    end

    it 'returns the union of all the sets' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNIONSTORE dest 2 z1 z2', ':7' ],
          [ 'ZRANGE dest 0 -1', [ 'bb', 'bbb', 'e', 'f', 'z', 'a', 'b' ] ],
        ]
      end
    end

    it 'returns the union summing scores with aggregate sum' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNIONSTORE dest 2 z1 z2 AGGREGATE SUM', ':7' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'bb', '2.22', 'bbb', '2.222', 'e', '5.97', 'f', '6.12345', 'z', '26.2', 'a', '101.1', 'b', '202.2' ] ],
        ]
      end
    end

    it 'returns the union with the min score with aggregate min' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNIONSTORE dest 2 z1 z2 AGGREGATE MIN', ':7' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'a', '1.1', 'b', '2.2', 'bb', '2.22', 'bbb', '2.222', 'e', '5.97', 'f', '6.12345', 'z', '26.2' ] ],
        ]
      end
    end

    it 'returns the union with the max score with aggregate max' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNIONSTORE dest 2 z1 z2 AGGREGATE MAX', ':7' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'bb', '2.22', 'bbb', '2.222', 'e', '5.97', 'f', '6.12345', 'z', '26.2', 'a', '100', 'b', '200' ] ],
        ]
      end
    end

    it 'uses the weights multiplier with the weights option' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'ZADD z2 26.2 z 2.22 bb 2.222 bbb 200.0 b 100.0 a', ':5' ],
          [ 'ZUNIONSTORE dest 2 z1 z2 AGGREGATE SUM WEIGHTS 2 2', ':7' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'bb', '4.44', 'bbb', '4.444', 'e', '11.94', 'f', '12.2469', 'z', '52.4', 'a', '202.2', 'b', '404.4' ] ],
          [ 'ZUNIONSTORE dest 2 z1 z2 AGGREGATE MIN WEIGHTS 2 2', ':7' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'a', '2.2', 'b', '4.4', 'bb', '4.44', 'bbb', '4.444', 'e', '11.94', 'f', '12.2469', 'z', '52.4' ] ],
          [ 'ZUNIONSTORE dest 2 z1 z2 AGGREGATE MAX WEIGHTS 2 2', ':7' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'bb', '4.44', 'bbb', '4.444', 'e', '11.94', 'f', '12.2469', 'z', '52.4', 'a', '200', 'b', '400' ] ],
        ]
      end
    end

    it 'accepts regular sets as inputs with a default score of 1.0' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z1 2.2 b 1.1 a 5.97 e 6.12345 f', ':4' ],
          [ 'SADD z2 z bb bbb b a', ':5' ],
          [ 'ZUNIONSTORE dest 2 z1 z2', ':7' ],
          [ 'ZRANGE dest 0 -1 WITHSCORES', [ 'bb', '1', 'bbb', '1', 'z', '1', 'a', '2.1', 'b', '3.2', 'e', '5.97', 'f', '6.12345' ] ],
        ]
      end
    end
  end

  describe 'ZRANK' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZRANK', '-ERR wrong number of arguments for \'ZRANK\' command' ],
        [ 'ZRANK z', '-ERR wrong number of arguments for \'ZRANK\' command' ],
        [ 'ZRANK z m1 m2', '-ERR wrong number of arguments for \'ZRANK\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZRANK not-a-set m', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns a nil string if the zset does not exist' do
      assert_command_results [
        [ 'ZRANK not-a-set a', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns a nil string if the zset does not contain the member' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a', ':2' ],
          [ 'ZRANK z c', BYORedis::NULL_BULK_STRING ],
        ]
      end
    end

    it 'returns the rank (0-based index) of the member' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a', ':2' ],
          [ 'ZRANK z a', ':0' ],
          [ 'ZRANK z b', ':1' ],
        ]
      end
    end
  end

  describe 'ZSCORE' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZSCORE', '-ERR wrong number of arguments for \'ZSCORE\' command' ],
        [ 'ZSCORE z', '-ERR wrong number of arguments for \'ZSCORE\' command' ],
        [ 'ZSCORE z m1 m2', '-ERR wrong number of arguments for \'ZSCORE\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZSCORE not-a-set m', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns a nil string if the zset does not exist' do
      assert_command_results [
        [ 'ZSCORE not-a-set a', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns a nil string if the zset does not contain the member' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a', one_of([ ':0', ':2' ]) ],
          [ 'ZSCORE z c', BYORedis::NULL_BULK_STRING ],
        ]
      end
    end

    it 'returns the score of the member as a string' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a', ':2' ],
          [ 'ZSCORE z a', '1.1' ],
          [ 'ZSCORE z b', '2.2' ],
        ]
      end
    end
  end

  describe 'ZMSCORE' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZMSCORE', '-ERR wrong number of arguments for \'ZMSCORE\' command' ],
        [ 'ZMSCORE z', '-ERR wrong number of arguments for \'ZMSCORE\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZMSCORE not-a-set m', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an array of nil strings if the zset does not exist' do
      assert_command_results [
        [ 'ZMSCORE z a b c', [ nil, nil, nil ] ],
      ]
    end

    it 'returns a nil string for each member not present in the set' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a', ':2' ],
          [ 'ZMSCORE z c', [ nil ] ],
        ]
      end
    end

    it 'returns the score of the member as a string for each member present in the set' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a', ':2' ],
          [ 'ZMSCORE z a', [ '1.1' ] ],
          [ 'ZMSCORE z b', [ '2.2' ] ],
          [ 'ZMSCORE z c', [ nil ] ],
          [ 'ZMSCORE z b a', [ '2.2', '1.1' ] ],
          [ 'ZMSCORE z a b', [ '1.1', '2.2' ] ],
          [ 'ZMSCORE z a c b', [ '1.1', nil, '2.2' ] ],
        ]
      end
    end
  end

  describe 'ZREM' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZREM', '-ERR wrong number of arguments for \'ZREM\' command' ],
        [ 'ZREM z', '-ERR wrong number of arguments for \'ZREM\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZREM not-a-set m', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns 0 if the zset does not exist' do
      assert_command_results [
        [ 'ZREM not-a-set a', ':0' ],
      ]
    end

    it 'returns 0 for non existing keys' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 26.2 z', ':3' ],
          [ 'ZREM z c d e', ':0' ],
        ]
      end
    end

    it 'returns the number of deleted members' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 26.2 z', ':3' ],
          [ 'ZREM z z b e', ':2' ],
        ]
      end
    end
  end

  describe 'ZREMRANGEBYLEX' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZREMRANGEBYLEX', '-ERR wrong number of arguments for \'ZREMRANGEBYLEX\' command' ],
        [ 'ZREMRANGEBYLEX z', '-ERR wrong number of arguments for \'ZREMRANGEBYLEX\' command' ],
        [ 'ZREMRANGEBYLEX z min', '-ERR wrong number of arguments for \'ZREMRANGEBYLEX\' command' ],
        [ 'ZREMRANGEBYLEX z min max a', '-ERR wrong number of arguments for \'ZREMRANGEBYLEX\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZREMRANGEBYLEX not-a-set [a [a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'validates the format of min and max' do
      assert_command_results [
        [ 'ZREMRANGEBYLEX z a b', '-ERR min or max not valid string range item' ],
        [ 'ZREMRANGEBYLEX z [a b', '-ERR min or max not valid string range item' ],
        [ 'ZREMRANGEBYLEX z a (b', '-ERR min or max not valid string range item' ],
        [ 'ZREMRANGEBYLEX z - b', '-ERR min or max not valid string range item' ],
        [ 'ZREMRANGEBYLEX z a +', '-ERR min or max not valid string range item' ],
      ]
    end

    it 'returns 0 if the zset does not exist' do
      assert_command_results [
        [ 'ZREMRANGEBYLEX not-a-set [a [a', ':0' ],
      ]
    end

    it 'returns 0 if no keys are in range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 13.1 f', ':3' ],
          [ 'ZREMRANGEBYLEX z [v [z', ':0' ],
        ]
      end
    end

    it 'removes all the items in the lex range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':7' ],
          [ 'ZREMRANGEBYLEX z - [e', ':5' ],
          [ 'ZRANGE z 0 -1', [ 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':5' ],
          [ 'ZREMRANGEBYLEX z - (e', ':4' ],
          [ 'ZRANGE z 0 -1', [ 'e', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':4' ],
          [ 'ZREMRANGEBYLEX z [b [e', ':4' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':4' ],
          [ 'ZREMRANGEBYLEX z (b [e', ':3' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':3' ],
          [ 'ZREMRANGEBYLEX z (bb [e', ':2' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'bb', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':2' ],
          [ 'ZREMRANGEBYLEX z (b +', ':5' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':5' ],
          [ 'ZREMRANGEBYLEX z [b +', ':6' ],
          [ 'ZRANGE z 0 -1', [ 'a' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':6' ],
          [ 'ZREMRANGEBYLEX z [bbb +', ':4' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'bb' ] ],
        ]
      end
    end
  end

  describe 'ZREMRANGEBYRANK' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZREMRANGEBYRANK', '-ERR wrong number of arguments for \'ZREMRANGEBYRANK\' command' ],
        [ 'ZREMRANGEBYRANK z', '-ERR wrong number of arguments for \'ZREMRANGEBYRANK\' command' ],
        [ 'ZREMRANGEBYRANK z 0', '-ERR wrong number of arguments for \'ZREMRANGEBYRANK\' command' ],
        [ 'ZREMRANGEBYRANK z 0 1 a', '-ERR wrong number of arguments for \'ZREMRANGEBYRANK\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZREMRANGEBYRANK not-a-set 0 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'validates the format of min and max' do
      assert_command_results [
        [ 'ZREMRANGEBYRANK z a b', '-ERR value is not an integer or out of range' ],
        [ 'ZREMRANGEBYRANK z 0 b', '-ERR value is not an integer or out of range' ],
        [ 'ZREMRANGEBYRANK z a 1', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns 0 if the zset does not exist' do
      assert_command_results [
        [ 'ZREMRANGEBYRANK not-a-set 0 1', ':0' ],
      ]
    end

    it 'returns 0 if no keys are in range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 13.1 f', ':3' ],
          [ 'ZREMRANGEBYRANK z 30 40', ':0' ],
          [ 'ZREMRANGEBYRANK z 2 0', ':0' ],
        ]
      end
    end

    it 'removes all the items in the lex range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':7' ],
          [ 'ZREMRANGEBYRANK z 0 4', ':5' ],
          [ 'ZRANGE z 0 -1', [ 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':5' ],
          [ 'ZREMRANGEBYRANK z 0 3', ':4' ],
          [ 'ZRANGE z 0 -1', [ 'e', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':4' ],
          [ 'ZREMRANGEBYRANK z 1 4', ':4' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':4' ],
          [ 'ZREMRANGEBYRANK z 2 4', ':3' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':3' ],
          [ 'ZREMRANGEBYRANK z 2 3', ':2' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'e', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':2' ],
          [ 'ZREMRANGEBYRANK z 1 -1', ':6' ],
          [ 'ZRANGE z 0 -1', [ 'a' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':6' ],
          [ 'ZREMRANGEBYRANK z 2 -1', ':5' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':5' ],
          [ 'ZREMRANGEBYRANK z 3 -1', ':4' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'bb' ] ],
        ]
      end
    end
  end

  describe 'ZREMRANGEBYSCORE' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZREMRANGEBYSCORE', '-ERR wrong number of arguments for \'ZREMRANGEBYSCORE\' command' ],
        [ 'ZREMRANGEBYSCORE z', '-ERR wrong number of arguments for \'ZREMRANGEBYSCORE\' command' ],
        [ 'ZREMRANGEBYSCORE z 0', '-ERR wrong number of arguments for \'ZREMRANGEBYSCORE\' command' ],
        [ 'ZREMRANGEBYSCORE z 0 1 a', '-ERR wrong number of arguments for \'ZREMRANGEBYSCORE\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZREMRANGEBYSCORE not-a-set 0 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'validates the format of min and max' do
      assert_command_results [
        [ 'ZREMRANGEBYSCORE z a b', '-ERR min or max is not a float' ],
        [ 'ZREMRANGEBYSCORE z 0 b', '-ERR min or max is not a float' ],
        [ 'ZREMRANGEBYSCORE z a 1', '-ERR min or max is not a float' ],
      ]
    end

    it 'returns 0 if the zset does not exist' do
      assert_command_results [
        [ 'ZREMRANGEBYSCORE not-a-set 0 1', ':0' ],
      ]
    end

    it 'returns 0 if no keys are in range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 13.1 f', ':3' ],
          [ 'ZREMRANGEBYSCORE z 30 40', ':0' ],
          [ 'ZREMRANGEBYSCORE z 2 0', ':0' ],
        ]
      end
    end

    it 'removes all the items in the score range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZREMRANGEBYSCORE z 0 4', ':4' ],
          [ 'ZRANGE z 0 -1', [ 'e', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':4' ],
          [ 'ZREMRANGEBYSCORE z 0 2.22', ':3' ],
          [ 'ZRANGE z 0 -1', [ 'bbb', 'e', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':3' ],
          [ 'ZREMRANGEBYSCORE z 2 4', ':3' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'e', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':3' ],
          [ 'ZREMRANGEBYSCORE z 0 100', ':7' ],
          [ 'ZRANGE z 0 -1', [] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZREMRANGEBYSCORE z 3 +inf', ':3' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'bb', 'bbb' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':3' ],
          [ 'ZREMRANGEBYSCORE z 3 inf', ':3' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'bb', 'bbb' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':3' ],
          [ 'ZREMRANGEBYSCORE z 3 infinity', ':3' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'bb', 'bbb' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':3' ],
          [ 'ZREMRANGEBYSCORE z 3 +infinity', ':3' ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'bb', 'bbb' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':3' ],
          [ 'ZREMRANGEBYSCORE z -inf 7', ':6' ],
          [ 'ZRANGE z 0 -1', [ 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.5 e 6.6 f 26.2 z 2.22 bb 2.222 bbb', ':6' ],
          [ 'ZREMRANGEBYSCORE z -infinity 7', ':6' ],
          [ 'ZRANGE z 0 -1', [ 'z' ] ],
        ]
      end
    end

    it 'accepts exclusive intervals' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZREMRANGEBYSCORE z 1 (2.2', ':1' ],
          [ 'ZRANGE z 0 -1', [ 'b', 'bb', 'bbb', 'e', 'f', 'z' ] ],
        ]
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':1' ],
          [ 'ZREMRANGEBYSCORE z 1 (26.2', ':6' ],
          [ 'ZRANGE z 0 -1', [ 'z' ] ],
        ]
      end
    end
  end

  describe 'ZREVRANGE' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZREVRANGE', '-ERR wrong number of arguments for \'ZREVRANGE\' command' ],
        [ 'ZREVRANGE z', '-ERR wrong number of arguments for \'ZREVRANGE\' command' ],
        [ 'ZREVRANGE z 0', '-ERR wrong number of arguments for \'ZREVRANGE\' command' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZREVRANGE z 0 1 WITHSCORE', '-ERR syntax error' ],
        [ 'ZREVRANGE z 0 1 WITHSCORES a', '-ERR syntax error' ],
      ]
    end

    it 'validates that start and stop are integers' do
      assert_command_results [
        [ 'ZREVRANGE z a 1 WITHSCORE', '-ERR value is not an integer or out of range' ],
        [ 'ZREVRANGE z 0 a WITHSCORES a', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZREVRANGE not-a-set 0 -1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the whole sorted set with 0 -1' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e 4.0 d', ':4' ],
          [ 'ZREVRANGE z 0 -1', [ 'e', 'd', 'b', 'a' ] ],
        ]
      end
    end

    it 'returns the whole sorted including set with 0 -1 and withscores' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e 4.0 d', ':4' ],
          [ 'ZREVRANGE z 0 -1 withscores', [ 'e', '5.1', 'd', '4', 'b', '2', 'a', '1' ] ],
          [ 'ZREVRANGE z 0 1 withscores', [ 'e', '5.1', 'd', '4' ] ],
        ]
      end
    end

    it 'handles negative indexes as starting from the right side' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e', ':3' ],
          [ 'ZREVRANGE z -3 2', [ 'e', 'b', 'a' ] ],
          [ 'ZREVRANGE z -2 1', [ 'b' ] ],
          [ 'ZREVRANGE z -2 2', [ 'b', 'a' ] ],
          [ 'ZREVRANGE z -1 2', [ 'a' ] ],
        ]
      end
    end

    it 'works with out of bounds indices' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e', ':3' ],
          [ 'ZREVRANGE z 2 22', [ 'a' ] ],
          [ 'ZREVRANGE z -6 0', [ 'e' ] ],
        ]
      end
    end

    it 'returns an empty array for out of order boundaries' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b', ':1' ],
          [ 'ZREVRANGE z 2 1', [] ],
          [ 'ZREVRANGE z -1 -2', [] ],
        ]
      end
    end

    it 'returns subsets' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.0 b 1.0 a 5.1 e 10.1 f 200.1 z', ':5' ],
          [ 'ZREVRANGE z 1 1', [ 'f' ] ],
          [ 'ZREVRANGE z 1 3', [ 'f', 'e', 'b' ] ],
          [ 'ZREVRANGE z 3 4', [ 'b', 'a' ] ],
          [ 'ZREVRANGE z 3 100', [ 'b', 'a' ] ],
        ]
      end
    end
  end

  describe 'ZREVRANGEBYLEX' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZREVRANGEBYLEX', '-ERR wrong number of arguments for \'ZREVRANGEBYLEX\' command' ],
        [ 'ZREVRANGEBYLEX z', '-ERR wrong number of arguments for \'ZREVRANGEBYLEX\' command' ],
        [ 'ZREVRANGEBYLEX z a', '-ERR wrong number of arguments for \'ZREVRANGEBYLEX\' command' ],
        [ 'ZREVRANGEBYLEX z a', '-ERR wrong number of arguments for \'ZREVRANGEBYLEX\' command' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZREVRANGEBYLEX z a b LIMIT', '-ERR min or max not valid string range item' ],
        [ 'ZREVRANGEBYLEX z [a [b LIMIT', '-ERR syntax error' ],
        [ 'ZREVRANGEBYLEX z [a [b LIMIT a', '-ERR syntax error' ],
        [ 'ZREVRANGEBYLEX z [a [b LIMIT 0', '-ERR syntax error' ],
        [ 'ZREVRANGEBYLEX z [a [b LIMIT 0 a', '-ERR value is not an integer or out of range' ],
        [ 'ZREVRANGEBYLEX z [a [b LIMIT a 1', '-ERR value is not an integer or out of range' ],
        [ 'ZREVRANGEBYLEX z [a [b LIMIT 0 1 a', '-ERR syntax error' ],
      ]
    end

    it 'validates the format of min and max' do
      assert_command_results [
        [ 'ZREVRANGEBYLEX z a b', '-ERR min or max not valid string range item' ],
        [ 'ZREVRANGEBYLEX z [a b', '-ERR min or max not valid string range item' ],
        [ 'ZREVRANGEBYLEX z a (b', '-ERR min or max not valid string range item' ],
        [ 'ZREVRANGEBYLEX z - b', '-ERR min or max not valid string range item' ],
        [ 'ZREVRANGEBYLEX z a +', '-ERR min or max not valid string range item' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZREVRANGEBYLEX not-a-set [a [b', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns all elements with + -' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z', ':5' ],
          [ 'ZREVRANGEBYLEX z + -', [ 'z', 'f', 'e', 'b', 'a' ] ],
        ]
      end
    end

    it 'returns an empty array with - +' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z', ':5' ],
          [ 'ZREVRANGEBYLEX z - +', [] ],
        ]
      end
    end

    it 'returns all elements in the range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':7' ],
          [ 'ZREVRANGEBYLEX z [e  -', [ 'e', 'bbb', 'bb', 'b', 'a' ] ],
          [ 'ZREVRANGEBYLEX z (e -', [ 'bbb', 'bb', 'b', 'a' ] ],
          [ 'ZREVRANGEBYLEX z [e [b', [ 'e', 'bbb', 'bb', 'b' ] ],
          [ 'ZREVRANGEBYLEX z [e (b', [ 'e', 'bbb', 'bb' ] ],
          [ 'ZREVRANGEBYLEX z [e (bb', [ 'e', 'bbb' ] ],
          [ 'ZREVRANGEBYLEX z + (b', [ 'z', 'f', 'e', 'bbb', 'bb' ] ],
          [ 'ZREVRANGEBYLEX z + [b', [ 'z', 'f', 'e', 'bbb', 'bb', 'b' ] ],
          [ 'ZREVRANGEBYLEX z + [bbb', [ 'z', 'f', 'e', 'bbb' ] ],
        ]
      end
    end

    it 'handles the limit offset count options' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':7' ],
          [ 'ZREVRANGEBYLEX z [e - LIMIT 0 2', [ 'e', 'bbb' ] ],
          [ 'ZREVRANGEBYLEX z [e - LIMIT 1 2', [ 'bbb', 'bb' ] ],
          [ 'ZREVRANGEBYLEX z [e - LIMIT 2 2', [ 'bb', 'b' ] ],
          [ 'ZREVRANGEBYLEX z [e - LIMIT 3 2', [ 'b', 'a' ] ],
          [ 'ZREVRANGEBYLEX z [e - LIMIT 4 2', [ 'a' ] ],
          [ 'ZREVRANGEBYLEX z [e - LIMIT 5 2', [] ],
        ]
      end
    end

    it 'handles the limit offset count options with a negative count' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':7' ],
          [ 'ZREVRANGEBYLEX z (e - LIMIT 0 -1', [ 'bbb', 'bb', 'b', 'a' ] ],
          [ 'ZREVRANGEBYLEX z (e - LIMIT 0 -2', [ 'bbb', 'bb', 'b', 'a' ] ],
        ]
      end
    end
  end

  describe 'ZREVRANGEBYSCORE' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZREVRANGEBYSCORE', '-ERR wrong number of arguments for \'ZREVRANGEBYSCORE\' command' ],
        [ 'ZREVRANGEBYSCORE z', '-ERR wrong number of arguments for \'ZREVRANGEBYSCORE\' command' ],
        [ 'ZREVRANGEBYSCORE z m', '-ERR wrong number of arguments for \'ZREVRANGEBYSCORE\' command' ],
      ]
    end

    it 'validates the options' do
      assert_command_results [
        [ 'ZREVRANGEBYSCORE z a b', '-ERR min or max is not a float' ],
        [ 'ZREVRANGEBYSCORE z a b withscore', '-ERR min or max is not a float' ],
        [ 'ZREVRANGEBYSCORE z a b limit', '-ERR min or max is not a float' ],
        [ 'ZREVRANGEBYSCORE z a b limit a', '-ERR min or max is not a float' ],
        [ 'ZREVRANGEBYSCORE z a b limit a b', '-ERR min or max is not a float' ],
        [ 'ZREVRANGEBYSCORE z 0 1 limi', '-ERR syntax error' ],
        [ 'ZREVRANGEBYSCORE z 0 1 limit', '-ERR syntax error' ],
        [ 'ZREVRANGEBYSCORE z 0 1 limit a', '-ERR syntax error' ],
        [ 'ZREVRANGEBYSCORE z 0 1 limit 0', '-ERR syntax error' ],
        [ 'ZREVRANGEBYSCORE z 0 1 limit a b', '-ERR value is not an integer or out of range' ],
        [ 'ZREVRANGEBYSCORE z 0 1 limit 0 b', '-ERR value is not an integer or out of range' ],
        [ 'ZREVRANGEBYSCORE z 0 1 limit a 0', '-ERR value is not an integer or out of range' ],
        [ 'ZREVRANGEBYSCORE z 0 1 withscor', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZREVRANGEBYSCORE not-a-set 0 -1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an empty array for nonsensical ranges' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZREVRANGEBYSCORE z 0 1', [] ],
        ]
      end
    end

    it 'returns all the elements with a score in the range (inclusive), ordered high to low' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZREVRANGEBYSCORE z 1 0', [] ],
          [ 'ZREVRANGEBYSCORE z 4 1', [ 'bbb', 'bb', 'b', 'a' ] ],
          [ 'ZREVRANGEBYSCORE z 100 5', [ 'z', 'f', 'e' ] ],
        ]
      end
    end

    it 'includes the scores with the withscores options' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZREVRANGEBYSCORE z 1 0 WITHSCORES', [] ],
          [ 'ZREVRANGEBYSCORE z 4 1 WITHSCORES', [ 'bbb', '2.222', 'bb', '2.22', 'b', '2.2', 'a', '1.1' ] ],
          [ 'ZREVRANGEBYSCORE z 100 5 WITHSCORES', [ 'z', '26.2', 'f', '6.12345', 'e', '5.97' ] ],
        ]
      end
    end

    it 'supports -inf and +inf as min/max values' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZREVRANGEBYSCORE z 1 -inf', [] ],
          [ 'ZREVRANGEBYSCORE z 1 -infinity', [] ],
          [ 'ZREVRANGEBYSCORE z 4 -inf', [ 'bbb', 'bb', 'b', 'a' ] ],
          [ 'ZREVRANGEBYSCORE z 4 -infinity', [ 'bbb', 'bb', 'b', 'a' ] ],
          [ 'ZREVRANGEBYSCORE z infinity -infinity', [ 'z', 'f', 'e', 'bbb', 'bb', 'b', 'a' ] ],
          [ 'ZREVRANGEBYSCORE z inf 5', [ 'z', 'f', 'e' ] ],
          [ 'ZREVRANGEBYSCORE z +inf 5', [ 'z', 'f', 'e' ] ],
          [ 'ZREVRANGEBYSCORE z infinity 5', [ 'z', 'f', 'e' ] ],
          [ 'ZREVRANGEBYSCORE z +infinity 5', [ 'z', 'f', 'e' ] ],
        ]
      end
    end

    it 'filters the result with the limit (offset/count) options' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZREVRANGEBYSCORE z 4 1 LIMIT 0 1', [ 'bbb' ] ],
          [ 'ZREVRANGEBYSCORE z 4 1 LIMIT 1 2', [ 'bb', 'b' ] ],
        ]
      end
    end

    it 'handles both limit and withscores options' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a 5.97 e 6.12345 f 26.2 z 2.22 bb 2.222 bbb', ':7' ],
          [ 'ZREVRANGEBYSCORE z 4 1 WITHSCORES LIMIT 0 1', [ 'bbb', '2.222' ] ],
          [ 'ZREVRANGEBYSCORE z 4 1 LIMIT 1 2 WITHSCORES', [ 'bb', '2.22', 'b', '2.2' ] ],
        ]
      end
    end
  end

  describe 'ZREVRANK' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZREVRANK', '-ERR wrong number of arguments for \'ZREVRANK\' command' ],
        [ 'ZREVRANK z', '-ERR wrong number of arguments for \'ZREVRANK\' command' ],
        [ 'ZREVRANK z m1 m2', '-ERR wrong number of arguments for \'ZREVRANK\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZREVRANK not-a-set m', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns a nil string if the zset does not exist' do
      assert_command_results [
        [ 'ZREVRANK not-a-set a', BYORedis::NULL_BULK_STRING ],
      ]
    end

    it 'returns a nil string if the zset does not contain the member' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a', ':2' ],
          [ 'ZREVRANK z c', BYORedis::NULL_BULK_STRING ],
        ]
      end
    end

    it 'returns the rank (0-based index) of the member' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 2.2 b 1.1 a', ':2' ],
          [ 'ZREVRANK z a', ':1' ],
          [ 'ZREVRANK z b', ':0' ],
        ]
      end
    end
  end

  describe 'ZPOPMAX' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZPOPMAX', '-ERR wrong number of arguments for \'ZPOPMAX\' command' ],
        [ 'ZPOPMAX z a a', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZPOPMAX not-a-set 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an empty arrat if the zset does not exist' do
      assert_command_results [
        [ 'ZPOPMAX not-a-set', BYORedis::EMPTY_ARRAY ],
      ]
    end

    it 'validates that count is an integer' do
      assert_command_results [
        [ 'ZPOPMAX z a', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'does nothing with a 0 or negative count' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'ZPOPMAX z 0', BYORedis::EMPTY_ARRAY ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'c' ] ],
        ]
      end
    end

    it 'returns the member with the max score, as a 2-element array, and remove it' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'ZPOPMAX z', [ 'c', '3.333' ] ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b' ] ],
          [ 'ZPOPMAX z', [ 'b', '2.2' ] ],
          [ 'ZRANGE z 0 -1', [ 'a' ] ],
          [ 'ZPOPMAX z', [ 'a', '1.1111' ] ],
          [ 'ZRANGE z 0 -1', [] ],
          [ 'TYPE z', '+none' ],
        ]
      end
    end

    it 'removes up to count pairs, sorted from high to low scores' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'ZPOPMAX z 10', [ 'c', '3.333', 'b', '2.2', 'a', '1.1111' ] ],
          [ 'ZRANGE z 0 -1', BYORedis::EMPTY_ARRAY ],
          [ 'TYPE z', '+none' ],
        ]
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'ZPOPMAX z 2', [ 'c', '3.333', 'b', '2.2' ] ],
          [ 'ZRANGE z 0 -1', [ 'a' ] ],
        ]
      end
    end
  end

  describe 'ZPOPMIN' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZPOPMIN', '-ERR wrong number of arguments for \'ZPOPMIN\' command' ],
        [ 'ZPOPMIN z a a', '-ERR syntax error' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZPOPMIN not-a-set 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns an empty arrat if the zset does not exist' do
      assert_command_results [
        [ 'ZPOPMIN not-a-set', BYORedis::EMPTY_ARRAY ],
      ]
    end

    it 'validates that count is an integer' do
      assert_command_results [
        [ 'ZPOPMIN z a', '-ERR value is not an integer or out of range' ],
      ]
    end

    it 'does nothing with a 0 or negative count' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'ZPOPMIN z 0', BYORedis::EMPTY_ARRAY ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b', 'c' ] ],
        ]
      end
    end

    it 'returns the member with the max score, as a 2-element array, and remove it' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'ZPOPMIN z', [ 'a', '1.1111' ] ],
          [ 'ZRANGE z 0 -1', [ 'b', 'c' ] ],
          [ 'ZPOPMIN z', [ 'b', '2.2' ] ],
          [ 'ZRANGE z 0 -1', [ 'c' ] ],
          [ 'ZPOPMIN z', [ 'c', '3.333' ] ],
          [ 'ZRANGE z 0 -1', [] ],
          [ 'TYPE z', '+none' ],
        ]
      end
    end

    it 'removes up to count pairs, sorted from low to high scores' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'ZPOPMIN z 10', [ 'a', '1.1111', 'b', '2.2', 'c', '3.333' ] ],
          [ 'ZRANGE z 0 -1', BYORedis::EMPTY_ARRAY ],
          [ 'TYPE z', '+none' ],
        ]
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'ZPOPMIN z 2', [ 'a', '1.1111', 'b', '2.2' ] ],
          [ 'ZRANGE z 0 -1', [ 'c' ] ],
        ]
      end
    end
  end

  describe 'BZPOPMAX' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'BZPOPMAX', '-ERR wrong number of arguments for \'BZPOPMAX\' command' ],
        [ 'BZPOPMAX z', '-ERR wrong number of arguments for \'BZPOPMAX\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'BZPOPMAX not-a-set 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'validates that timeout is a float' do
      assert_command_results [
        [ 'BZPOPMAX z a', '-ERR timeout is not a float or out of range' ],
        [ 'BZPOPMAX z -inf', '-ERR timeout is negative' ],
        [ 'BZPOPMAX z -infinity', '-ERR timeout is negative' ],
        [ 'BZPOPMAX z infinity', '-ERR timeout is negative' ],
        [ 'BZPOPMAX z +infinity', '-ERR timeout is negative' ],
        [ 'BZPOPMAX z inf', '-ERR timeout is negative' ],
        [ 'BZPOPMAX z +inf', '-ERR timeout is negative' ],
      ]
    end

    it 'returns the member with the max score from the first non empty set, as a 2-element array, and remove it' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'BZPOPMAX z1 z2 z z3 1', [ 'z', 'c', '3.333' ] ],
          [ 'ZRANGE z 0 -1', [ 'a', 'b' ] ],
          [ 'ZADD z2 123 John 456 Jane', ':2' ],
          [ 'BZPOPMAX z1 z2 1', [ 'z2', 'Jane', '456' ] ],
          [ 'BZPOPMAX z1 z 1', [ 'z', 'b', '2.2' ] ],
          [ 'ZRANGE z 0 -1', [ 'a' ] ],
          [ 'BZPOPMAX z3 z 1', [ 'z', 'a', '1.1111' ] ],
          [ 'ZRANGE z 0 -1', [] ],
          [ 'TYPE z', '+none' ],
        ]
      end
    end

    it 'blocks up to timeout and returns a nil array' do
      with_server do |socket|
        socket.write(to_query('BZPOPMAX', 'z', '0.2'))
        assert_nil(read_response(socket, read_timeout: 0.05))
        # Still blocked after 200ms, we give up and call it a success!
        assert_equal(BYORedis::NULL_ARRAY, read_response(socket, read_timeout: 0.3))
      end
    end

    it 'blocks forever with a 0 timeout' do
      with_server do |socket|
        socket.write(to_query('BZPOPMAX', 'z', '0'))
        # Still blocked after 200ms, we give up and call it a success!
        assert_nil(read_response(socket, read_timeout: 0.2))
      end
    end

    it 'returns a pair if any of the sets receives an element before timeout' do
      with_server do |socket|
        socket2 = TCPSocket.new 'localhost', 2000
        th = Thread.new do
          sleep 0.05 # Enough time for the BZPOPMAX to be processed
          socket2.write(to_query('ZADD', 'z',  '4.0', 'd', '1.0', 'a'))
          socket2.close
        end
        socket.write(to_query('BZPOPMAX', 'z', '0.5'))
        th.join
        assert_equal("*3\r\n$1\r\nz\r\n$1\r\nd\r\n$1\r\n4\r\n", read_response(socket))
      end
    end
  end

  describe 'BZPOPMIN' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'BZPOPMIN', '-ERR wrong number of arguments for \'BZPOPMIN\' command' ],
        [ 'BZPOPMIN z', '-ERR wrong number of arguments for \'BZPOPMIN\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'BZPOPMIN not-a-set 1', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'validates that timeout is a float' do
      assert_command_results [
        [ 'BZPOPMIN z a', '-ERR timeout is not a float or out of range' ],
        [ 'BZPOPMIN z -inf', '-ERR timeout is negative' ],
        [ 'BZPOPMIN z -infinity', '-ERR timeout is negative' ],
        [ 'BZPOPMIN z infinity', '-ERR timeout is negative' ],
        [ 'BZPOPMIN z +infinity', '-ERR timeout is negative' ],
        [ 'BZPOPMIN z inf', '-ERR timeout is negative' ],
        [ 'BZPOPMIN z +inf', '-ERR timeout is negative' ],
      ]
    end

    it 'returns the member with the max score from the first non empty set, as a 2-element array, and remove it' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 3.333 c 2.2 b 1.1111 a', ':3' ],
          [ 'BZPOPMIN z1 z2 z z3 1', [ 'z', 'a', '1.1111' ] ],
          [ 'ZRANGE z 0 -1', [ 'b', 'c' ] ],
          [ 'BZPOPMIN z1 z2 z z3 1', [ 'z', 'b', '2.2' ] ],
          [ 'BZPOPMIN z1 z2 z z3 1', [ 'z', 'c', '3.333' ] ],
          [ 'TYPE z', '+none' ],
        ]
      end
    end

    it 'blocks up to timeout and returns a nil array' do
      with_server do |socket|
        socket.write(to_query('BZPOPMIN', 'z', '0.2'))
        assert_nil(read_response(socket, read_timeout: 0.05))

        assert_equal(BYORedis::NULL_ARRAY, read_response(socket, read_timeout: 0.3))
      end
    end

    it 'blocks forever with a 0 timeout' do
      with_server do |socket|
        socket.write(to_query('BZPOPMIN', 'z', '0'))
        # Still blocked after 200ms, we give up and call it a success!
        assert_nil(read_response(socket, read_timeout: 0.2))
      end
    end

    it 'returns a pair if any of the sets receives an element before timeout' do
      with_server do |socket|
        socket2 = TCPSocket.new 'localhost', 2000
        th = Thread.new do
          sleep 0.05 # Enough time for the BZPOPMIN to be processed
          socket2.write(to_query('ZADD', 'z',  '4.0', 'd', '1.0', 'a'))
          socket2.close
        end
        socket.write(to_query('BZPOPMIN', 'z', '0.5'))
        th.join
        assert_equal("*3\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\n1\r\n", read_response(socket))
      end
    end
  end

  describe 'ZCOUNT' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZCOUNT', '-ERR wrong number of arguments for \'ZCOUNT\' command' ],
        [ 'ZCOUNT z', '-ERR wrong number of arguments for \'ZCOUNT\' command' ],
        [ 'ZCOUNT z a', '-ERR wrong number of arguments for \'ZCOUNT\' command' ],
        [ 'ZCOUNT z a b c', '-ERR wrong number of arguments for \'ZCOUNT\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZCOUNT not-a-set 1 2', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'validates that min and max are floats' do
      assert_command_results [
        [ 'ZCOUNT z a b', '-ERR min or max is not a float' ],
        [ 'ZCOUNT z 1 b', '-ERR min or max is not a float' ],
        [ 'ZCOUNT z a 1', '-ERR min or max is not a float' ],
      ]
    end

    it 'returns 0 if min and max are out of order' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 1.1 a 3.3 c 2.2 b', ':3' ],
          [ 'ZCOUNT z +inf -inf', ':0', ],
        ]
      end
    end

    it 'returns 0 if the sorted set does not exist' do
      assert_command_results [
        [ 'ZCOUNT z -inf +inf', ':0', ],
      ]
    end

    it 'returns the number of members in the range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 1.1 a 3.3 c 2.2 b', ':3' ],
          [ 'ZCOUNT z -inf 2.2', ':2', ],
          [ 'ZCOUNT z 1.1 2.2', ':2', ],
          [ 'ZCOUNT z -inf +inf', ':3', ],
          [ 'ZCOUNT z -inf -inf', ':0', ],
          [ 'ZCOUNT z +inf +inf', ':0', ],
        ]
      end
    end

    it 'accepts exclusive intervals' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 1.1 a 3.3 c 2.2 b', ':3' ],
          [ 'ZCOUNT z (2.2 +inf', ':1', ],
          [ 'ZCOUNT z 1.1 (3.3', ':2', ],
        ]
      end
    end
  end

  describe 'ZLEXCOUNT' do
    it 'handles an unexpected number of arguments' do
      assert_command_results [
        [ 'ZLEXCOUNT', '-ERR wrong number of arguments for \'ZLEXCOUNT\' command' ],
        [ 'ZLEXCOUNT z', '-ERR wrong number of arguments for \'ZLEXCOUNT\' command' ],
        [ 'ZLEXCOUNT z a', '-ERR wrong number of arguments for \'ZLEXCOUNT\' command' ],
        [ 'ZLEXCOUNT z a b c', '-ERR wrong number of arguments for \'ZLEXCOUNT\' command' ],
      ]
    end

    it 'returns an error if the key is not a set' do
      assert_command_results [
        [ 'SET not-a-set 1', '+OK' ],
        [ 'ZLEXCOUNT not-a-set - +', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'validates the format of min and max' do
      assert_command_results [
        [ 'ZLEXCOUNT z a b', '-ERR min or max not valid string range item' ],
        [ 'ZLEXCOUNT z [a b', '-ERR min or max not valid string range item' ],
        [ 'ZLEXCOUNT z a (b', '-ERR min or max not valid string range item' ],
        [ 'ZLEXCOUNT z - b', '-ERR min or max not valid string range item' ],
        [ 'ZLEXCOUNT z a +', '-ERR min or max not valid string range item' ],
      ]
    end

    it 'returns 0 if min and max are out of order' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 1.1 a 3.3 c 2.2 b', ':3' ],
          [ 'ZLEXCOUNT z + -', ':0', ],
        ]
      end
    end

    it 'returns 0 if the sorted set does not exist' do
      assert_command_results [
        [ 'ZLEXCOUNT z - +', ':0', ],
      ]
    end

    it 'returns the number of members in the range' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z 0 bb 0 bbb', ':7' ],
          [ 'ZLEXCOUNT z - [e', ':5' ],
          [ 'ZLEXCOUNT z - (e', ':4' ],
          [ 'ZLEXCOUNT z [b [e', ':4' ],
          [ 'ZLEXCOUNT z (b [e', ':3' ],
          [ 'ZLEXCOUNT z (bb [e', ':2' ],
          [ 'ZLEXCOUNT z (b +', ':5' ],
          [ 'ZLEXCOUNT z [b +', ':6' ],
          [ 'ZLEXCOUNT z [bbb +', ':4' ],
          [ 'ZLEXCOUNT z - +', ':7', ],
          [ 'ZLEXCOUNT z - -', ':0', ],
          [ 'ZLEXCOUNT z + +', ':0', ],
        ]
      end
    end

    it 'returns all elements with - +' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z', ':5' ],
          [ 'ZLEXCOUNT z - +', ':5' ],
        ]
      end
    end

    it 'returns an empty array with + -' do
      test_with_config_values(zset_max_ziplist_entries: [ '128', '1' ]) do
        assert_command_results [
          [ 'ZADD z 0 b 0 a 0 e 0 f 0 z', ':5' ],
          [ 'ZLEXCOUNT z + -', ':0' ],
        ]
      end
    end
  end

  describe 'ZINCRBY' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'ZINCRBY', '-ERR wrong number of arguments for \'ZINCRBY\' command' ],
        [ 'ZINCRBY z', '-ERR wrong number of arguments for \'ZINCRBY\' command' ],
        [ 'ZINCRBY z a', '-ERR wrong number of arguments for \'ZINCRBY\' command' ],
        [ 'ZINCRBY z a b c', '-ERR wrong number of arguments for \'ZINCRBY\' command' ],
      ]
    end

    it 'fails if the key is not a zset' do
      assert_command_results [
        [ 'SET not-a-zset 1', '+OK' ],
        [ 'ZINCRBY not-a-zset 1 a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the new value, as a RESP string of the value for the field, after the incr' do
      assert_command_results [
        [ 'ZADD z 1.0 a 3.0 c 2.0 b', ':3' ],
        [ 'ZINCRBY z 0.34 a', '1.34' ],
      ]
    end

    it 'returns an error if the increment is not a number (float or int)' do
      assert_command_results [
        [ 'ZADD z 1.0 a 3.0 c 2.0 b', ':3' ],
        [ 'ZINCRBY z a a', '-ERR value is not a valid float' ],
      ]
    end

    it 'creates a new field/value pair with a value of 0 if the field does not exist' do
      assert_command_results [
        [ 'ZADD z 1.0 a 3.0 c 2.0 b', ':3' ],
        [ 'ZINCRBY z 5.2 d', '5.2' ],
        [ 'ZRANGE z 0 -1', [ 'a', 'b', 'c', 'd' ]],
      ]
    end

    it 'creates a new hash and a new field/value pair with a value of 0 if the hash does not exist' do
      assert_command_results [
        [ 'TYPE z', '+none' ],
        [ 'ZINCRBY z 1.2 a', '1.2' ],
        [ 'TYPE z', '+zset' ],
      ]
    end

    it 'allows adding inf to inf' do
      assert_command_results [
        [ 'ZINCRBY z inf a', 'inf' ],
        [ 'ZINCRBY z +inf a', 'inf' ],
      ]
    end

    it 'fails to subtract inf from inf' do
      assert_command_results [
        [ 'ZINCRBY z inf a', 'inf' ],
        [ 'ZINCRBY z -inf a', '-ERR resulting score is not a number (NaN)' ],
      ]
    end
  end

  describe 'blocking commands for lists and sets' do
    it 'handles being blocked for a key that is then created with a different type' do
      begin
        socket2 = nil
        with_server do |socket|
          socket2 = TCPSocket.new 'localhost', 2000
          thread = Thread.new do
            sleep 0.1
            socket2.write(to_query('RPUSH', 'z', '1', '2'))
            read_response(socket2)

            socket2.write(to_query('TYPE', 'z'))
            assert_equal('+list', read_response(socket2).strip)

            socket2.write(to_query('DEL', 'z'))
            assert_equal(':1', read_response(socket2).strip)

            sleep 0.2

            socket2.write(to_query('ZADD', 'z', '0', 'a'))
            assert_equal(':1', read_response(socket2).strip)
          end
          socket.write(to_query('BZPOPMAX', 'z', '0'))
          assert_nil(read_response(socket))

          sleep 0.1

          assert_equal(BYORedis::RESPArray.new([ 'z', 'a', '0' ]).serialize, read_response(socket))

          thread.join
        end
      ensure
        socket2&.close
      end
    end
  end
end
