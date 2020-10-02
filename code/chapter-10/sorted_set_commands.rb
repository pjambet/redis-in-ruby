require_relative './redis_sorted_set'

module BYORedis
  module SortedSetUtils
    def self.generic_zpop(db, args)
      Utils.assert_args_length_greater_than(0, args)
      key = args.shift
      count = args.shift
      raise RESPSyntaxError unless args.empty?

      count = if count.nil?
                1
              else
                Utils.validate_integer(count)
              end

      sorted_set = db.lookup_sorted_set(key)
      popped = []

      if sorted_set
        popped = yield sorted_set, count
        db.data_store.delete(key) if sorted_set.empty?
      end

      RESPArray.new(popped)
    end

    def self.generic_bzpop(db, args, operation)
      Utils.assert_args_length_greater_than(1, args)
      timeout = Utils.validate_timeout(args.pop)
      args.each do |set_name|
        sorted_set = db.lookup_sorted_set(set_name)
        next if sorted_set.nil?

        popped = yield sorted_set
        db.data_store.delete(set_name) if sorted_set.empty?

        return RESPArray.new([ set_name ] + popped)
      end

      Server::BlockedState.new(
        BlockedClientHandler.timeout_timestamp_or_nil(timeout), args, operation)
    end

    def self.intersection(db, args)
      set_operation(db, args) do |sets_with_weight, aggregate|
        RedisSortedSet.intersection(sets_with_weight, aggregate: aggregate)
      end
    end

    def self.union(db, args)
      set_operation(db, args) do |sets_with_weight, aggregate|
        RedisSortedSet.union(sets_with_weight, aggregate: aggregate)
      end
    end

    def self.set_operation(db, args)
      options = { aggregate: :sum, withscores: false }
      sets = SortedSetUtils.validate_number_of_sets(db, args)
      options.merge!(SortedSetUtils.parse_union_or_inter_options(args, sets.size))

      sets_with_weight = sets.zip(options[:weights])
      new_set = yield sets_with_weight, options[:aggregate]

      return new_set, options[:withscores]
    end

    def self.set_operation_command(args)
      Utils.assert_args_length_greater_than(1, args)
      set_result, withscores = yield

      SortedSetRankSerializer.new(
        set_result,
        RedisSortedSet::GenericRangeSpec.rank_range_spec(0, -1, set_result.cardinality),
        withscores: withscores,
      )
    end

    def self.set_operation_store_command(db, args)
      Utils.assert_args_length_greater_than(2, args)
      destination_key = args.shift

      result_set, _ = yield

      if result_set.empty?
        db.data_store.delete(destination_key)
      else
        db.data_store[destination_key] = result_set
      end

      RESPInteger.new(result_set.cardinality)
    end

    def self.parse_union_or_inter_options(args, number_of_sets)
      options = { weights: Array.new(number_of_sets, 1) }
      while arg = args.shift
        case arg.downcase
        when 'weights'
          options[:weights] = validate_weights(number_of_sets, args)
        when 'aggregate'
          aggregate_mode = args.shift
          case aggregate_mode&.downcase
          when 'min' then options[:aggregate] = :min
          when 'max' then options[:aggregate] = :max
          when 'sum' then options[:aggregate] = :sum
          else raise RESPSyntaxError
          end
        when 'withscores' then options[:withscores] = true
        else raise RESPSyntaxError
        end
      end

      options
    end

    def self.validate_number_of_sets(db, args)
      number_of_sets = Utils.validate_integer(args.shift)
      number_of_sets.times.map do
        set_key = args.shift
        raise RESPSyntaxError if set_key.nil?

        db.lookup_sorted_set_or_set(set_key)
      end
    end

    def self.validate_weights(number_of_sets, args)
      number_of_sets.times.map do
        weight = args.shift
        raise RESPSyntaxError if weight.nil?

        Utils.validate_float(weight, 'ERR weight value is not a float')
      end
    end

    def self.parse_limit_option(args, options)
      offset = args.shift
      count = args.shift
      raise RESPSyntaxError if offset.nil? || count.nil?

      offset = Utils.validate_integer(offset)
      count = Utils.validate_integer(count)
      options[:offset] = offset
      options[:count] = count
    end

    def self.generic_range_by_score(db, args, reverse: false)
      # A negative count means "all of them"
      options = { offset: 0, count: -1, withscores: false }
      Utils.assert_args_length_greater_than(2, args)
      key = args.shift
      if reverse
        max = args.shift
        min = args.shift
      else
        min = args.shift
        max = args.shift
      end
      range_spec = Utils.validate_score_range_spec(min, max)
      parse_range_by_score_options(args, options) unless args.empty?

      sorted_set = db.lookup_sorted_set(key)
      if options[:offset] < 0
        EmptyArrayInstance
      elsif sorted_set
        options[:reverse] = reverse

        SortedSetSerializerBy.new(sorted_set, range_spec, **options, &:score)
      else
        EmptyArrayInstance
      end
    end

    def self.parse_range_by_score_options(args, options)
      while arg = args.shift
        case arg.downcase
        when 'withscores' then options[:withscores] = true
        when 'limit' then SortedSetUtils.parse_limit_option(args, options)
        else raise RESPSyntaxError
        end
      end
    end

    def self.generic_range_by_lex(db, args, reverse: false)
      # A negative count means "all of them"
      options = { offset: 0, count: -1 }
      Utils.assert_args_length_greater_than(2, args)
      key = args.shift
      if reverse
        max = args.shift
        min = args.shift
      else
        min = args.shift
        max = args.shift
      end
      range_spec = Utils.validate_lex_range_spec(min, max)
      parse_range_by_lex_options(args, options) unless args.empty?

      sorted_set = db.lookup_sorted_set(key)
      if options[:offset] < 0
        EmptyArrayInstance
      elsif sorted_set
        options[:withscores] = false
        options[:reverse] = reverse
        SortedSetSerializerBy.new(sorted_set, range_spec, **options, &:member)
      else
        EmptyArrayInstance
      end
    end

    def self.parse_range_by_lex_options(args, options)
      raise RESPSyntaxError unless args.length == 3

      if args.shift.downcase == 'limit'
        SortedSetUtils.parse_limit_option(args, options)
      else
        raise RESPSyntaxError
      end
    end

    def self.reverse_range_index(index, max)
      if index >= 0
        max - index
      elsif index < 0
        max - (index + max + 1)
      end
    end

    def self.generic_range(db, args, reverse: false)
      Utils.assert_args_length_greater_than(2, args)
      start = Utils.validate_integer(args[1])
      stop = Utils.validate_integer(args[2])
      raise RESPSyntaxError if args.length > 4

      if args[3]
        if args[3].downcase == 'withscores'
          withscores = true
        else
          raise RESPSyntaxError
        end
      end

      sorted_set = db.lookup_sorted_set(args[0])

      if reverse
        tmp = reverse_range_index(start, sorted_set.cardinality - 1)
        start = reverse_range_index(stop, sorted_set.cardinality - 1)
        stop = tmp
      end

      if sorted_set
        range_spec =
          RedisSortedSet::GenericRangeSpec.rank_range_spec(start, stop, sorted_set.cardinality)
        SortedSetRankSerializer.new(
          sorted_set,
          range_spec,
          withscores: withscores,
          reverse: reverse,
        )
      else
        EmptyArrayInstance
      end
    end

    def self.generic_count(db, args)
      Utils.assert_args_length(3, args)
      key = args[0]
      min = args[1]
      max = args[2]
      sorted_set = db.lookup_sorted_set(key)

      count = yield(sorted_set, min, max) || 0

      RESPInteger.new(count)
    end
  end

  class ZAddCommand < BaseCommand
    def call
      @options = {
        presence: nil,
        ch: false,
        incr: false,
      }
      Utils.assert_args_length_greater_than(1, @args)
      key = @args.shift
      parse_options
      raise RESPSyntaxError unless @args.length.even?

      if @options[:incr] && @args.length > 2
        raise ValidationError, 'ERR INCR option supports a single increment-element pair'
      end

      pairs = @args.each_slice(2).map do |pair|
        score = Utils.validate_float(pair[0], 'ERR value is not a valid float')
        member = pair[1]
        [ score, member ]
      end

      sorted_set = @db.lookup_sorted_set_for_write(key)
      return_count = 0
      pairs.each do |pair|
        sorted_set_add_result = sorted_set.add(pair[0], pair[1], options: @options)

        if @options[:incr]
          if sorted_set_add_result
            return_count = Utils.float_to_string(sorted_set_add_result)
          else
            return_count = nil
          end
        elsif sorted_set_add_result
          return_count += 1
        end
      end

      RESPSerializer.serialize(return_count)
    rescue FloatNaN
      RESPError.new('ERR resulting score is not a number (NaN)')
    end

    def self.describe
      Describe.new('zadd', -4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@sortedset', '@fast' ])
    end

    private

    def parse_options
      @options = {}
      loop do
        # We peek at the first arg to see if it is an option
        arg = @args[0]
        case arg.downcase
        when 'nx', 'xx' then set_presence_option(arg.downcase)
        when 'ch' then @options[:ch] = true
        when 'incr' then @options[:incr] = true
        else
          # We found none of the known options, so let's stop here
          break
        end
        # Since we didn't break, we consume the head of @args
        @args.shift
      end
    end

    def set_presence_option(option_value)
      if @options[:presence] && @options[:presence] != option_value
        raise ValidationError, 'ERR XX and NX options at the same time are not compatible'
      else
        @options[:presence] = option_value
      end
    end
  end

  class ZCardCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)
      sorted_set = @db.lookup_sorted_set(@args[0])
      cardinality = sorted_set&.cardinality || 0

      RESPInteger.new(cardinality)
    end

    def self.describe
      Describe.new('zcard', 2, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@fast' ])
    end
  end

  class ZRangeCommand < BaseCommand
    def call
      SortedSetUtils.generic_range(@db, @args)
    end

    def self.describe
      Describe.new('zrange', -4, [ 'readonly' ], 1, 1, 1, [ '@read', '@sortedset', '@slow' ])
    end
  end

  class ZRangeByLexCommand < BaseCommand
    def call
      SortedSetUtils.generic_range_by_lex(@db, @args, reverse: false)
    end

    def self.describe
      Describe.new('zrangebylex', -4, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@slow' ])
    end
  end

  class ZRangeByScoreCommand < BaseCommand
    def call
      SortedSetUtils.generic_range_by_score(@db, @args, reverse: false)
    end

    def self.describe
      Describe.new('zrangebyscore', -4, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@slow' ])
    end
  end

  class ZInterCommand < BaseCommand
    def call
      SortedSetUtils.set_operation_command(@args) do
        SortedSetUtils.intersection(@db, @args)
      end
    end

    def self.describe
      Describe.new('zinter', -3, [ 'readonly', 'movablekeys' ], 0, 0, 0,
                   [ '@read', '@sortedset', '@slow' ])
    end
  end

  class ZInterStoreCommand < BaseCommand
    def call
      SortedSetUtils.set_operation_store_command(@db, @args) do
        SortedSetUtils.intersection(@db, @args)
      end
    end

    def self.describe
      Describe.new('zinterstore', -4, [ 'write', 'denyoom', 'movablekeys' ], 0, 0, 0,
                   [ '@write', '@sortedset', '@slow' ])
    end
  end

  class ZUnionCommand < BaseCommand
    def call
      SortedSetUtils.set_operation_command(@args) do
        SortedSetUtils.union(@db, @args)
      end
    end

    def self.describe
      Describe.new('zunion', -3, [ 'readonly', 'movablekeys' ], 0, 0, 0,
                   [ '@read', '@sortedset', '@slow' ])
    end
  end

  class ZUnionStoreCommand < BaseCommand
    def call
      SortedSetUtils.set_operation_store_command(@db, @args) do
        SortedSetUtils.union(@db, @args)
      end
    end

    def self.describe
      Describe.new('zunionstore', -4, [ 'write', 'denyoom', 'movablekeys' ], 0, 0, 0,
                   [ '@write', '@sortedset', '@slow' ])
    end
  end

  class ZRankCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      sorted_set = @db.lookup_sorted_set(@args[0])

      if sorted_set
        RESPSerializer.serialize(sorted_set.rank(@args[1]))
      else
        NullBulkStringInstance
      end
    end

    def self.describe
      Describe.new('zrank', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@fast' ])
    end
  end

  class ZScoreCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      sorted_set = @db.lookup_sorted_set(@args[0])

      if sorted_set
        RESPSerializer.serialize(sorted_set.score(@args[1]))
      else
        NullBulkStringInstance
      end
    end

    def self.describe
      Describe.new('zscore', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@fast' ])
    end
  end

  class ZMScoreCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      sorted_set = @db.lookup_sorted_set(@args[0])

      scores = @args[1..-1].map do |member|
        sorted_set.score(member) if sorted_set
      end

      RESPArray.new(scores)
    end

    def self.describe
      Describe.new('zmscore', -3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@fast' ])
    end
  end

  class ZRemCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      sorted_set = @db.lookup_sorted_set(@args.shift)
      removed_count = 0

      if sorted_set
        @args.each do |member|
          removed_count += 1 if sorted_set.remove(member)
        end
      end

      RESPInteger.new(removed_count)
    end

    def self.describe
      Describe.new('zrem', -3, [ 'write', 'fast' ], 1, 1, 1,
                   [ '@write', '@sortedset', '@fast' ])
    end
  end

  class ZRemRangeByLexCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      range_spec = Utils.validate_lex_range_spec(@args[1], @args[2])
      sorted_set = @db.lookup_sorted_set(@args[0])
      removed_count = 0

      if sorted_set
        removed_count = sorted_set.remove_lex_range(range_spec)
      end

      RESPInteger.new(removed_count)
    end

    def self.describe
      Describe.new('zremrangebylex', 4, [ 'write' ], 1, 1, 1,
                   [ '@write', '@sortedset', '@slow' ])
    end
  end

  class ZRemRangeByRankCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      start = Utils.validate_integer(@args[1])
      stop = Utils.validate_integer(@args[2])
      sorted_set = @db.lookup_sorted_set(@args[0])
      removed_count = 0

      if sorted_set
        range_spec =
          RedisSortedSet::GenericRangeSpec.rank_range_spec(start, stop, sorted_set.cardinality)
        removed_count = sorted_set.remove_rank_range(range_spec)
      end

      RESPInteger.new(removed_count)
    end

    def self.describe
      Describe.new('zremrangebyrank', 4, [ 'write' ], 1, 1, 1,
                   [ '@write', '@sortedset', '@slow' ])
    end
  end

  class ZRemRangeByScoreCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      range_spec = Utils.validate_score_range_spec(@args[1], @args[2])
      sorted_set = @db.lookup_sorted_set(@args[0])
      removed_count = 0

      removed_count = sorted_set.remove_score_range(range_spec) if sorted_set

      RESPInteger.new(removed_count)
    end

    def self.describe
      Describe.new('zremrangebyscore', 4, [ 'write' ], 1, 1, 1,
                   [ '@write', '@sortedset', '@slow' ])
    end
  end

  class ZRevRangeCommand < BaseCommand
    def call
      SortedSetUtils.generic_range(@db, @args, reverse: true)
    end

    def self.describe
      Describe.new('zrevrange', -4, [ 'readonly' ], 1, 1, 1, [ '@read', '@sortedset', '@slow' ])
    end
  end

  class ZRevRangeByLexCommand < BaseCommand
    def call
      SortedSetUtils.generic_range_by_lex(@db, @args, reverse: true)
    end

    def self.describe
      Describe.new('zrevrangebylex', -4, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@slow' ])
    end
  end

  class ZRevRangeByScoreCommand < BaseCommand
    def call
      SortedSetUtils.generic_range_by_score(@db, @args, reverse: true)
    end

    def self.describe
      Describe.new('zrevrangebyscore', -4, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@slow' ])
    end
  end

  class ZRevRankCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      sorted_set = @db.lookup_sorted_set(@args[0])

      if sorted_set
        RESPSerializer.serialize(sorted_set.rev_rank(@args[1]))
      else
        NullBulkStringInstance
      end
    end

    def self.describe
      Describe.new('zrevrank', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@fast' ])
    end
  end

  class ZPopMaxCommand < BaseCommand
    def call
      SortedSetUtils.generic_zpop(@db, @args) do |sorted_set, count|
        sorted_set.pop_max(count)
      end
    end

    def self.describe
      Describe.new('zpopmax', -2, [ 'write', 'fast' ], 1, 1, 1,
                   [ '@write', '@sortedset', '@fast' ])
    end
  end

  class ZPopMinCommand < BaseCommand
    def call
      SortedSetUtils.generic_zpop(@db, @args) do |sorted_set, count|
        sorted_set.pop_min(count)
      end
    end

    def self.describe
      Describe.new('zpopmin', -2, [ 'write', 'fast' ], 1, 1, 1,
                   [ '@write', '@sortedset', '@fast' ])
    end
  end

  class BZPopMaxCommand < BaseCommand
    def call
      SortedSetUtils.generic_bzpop(@db, @args, :zpopmax) do |sorted_set|
        sorted_set.pop_max(1)
      end
    end

    def self.describe
      Describe.new('bzpopmax', -3, [ 'write', 'noscript', 'fast' ], 1, -2, 1,
                   [ '@write', '@sortedset', '@fast', '@blocking' ])
    end
  end
  class BZPopMinCommand < BaseCommand
    def call
      SortedSetUtils.generic_bzpop(@db, @args, :zpopmin) do |sorted_set|
        sorted_set.pop_min(1)
      end
    end

    def self.describe
      Describe.new('bzpopmin', -3, [ 'write', 'noscript', 'fast' ], 1, -2, 1,
                   [ '@write', '@sortedset', '@fast', '@blocking' ])
    end
  end

  class ZCountCommand < BaseCommand
    def call
      SortedSetUtils.generic_count(@db, @args) do |sorted_set, min, max|
        range_spec = Utils.validate_score_range_spec(min, max)
        sorted_set&.count_in_rank_range(range_spec)
      end
    end

    def self.describe
      Describe.new('zcount', 4, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@fast' ])
    end
  end

  class ZLexCountCommand < BaseCommand
    def call
      SortedSetUtils.generic_count(@db, @args) do |sorted_set, min, max|
        range_spec = Utils.validate_lex_range_spec(min, max)
        sorted_set&.count_in_lex_range(range_spec)
      end
    end

    def self.describe
      Describe.new('zlexcount', 4, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@sortedset', '@fast' ])
    end
  end

  class ZIncrByCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      incr = Utils.validate_float(@args[1], 'ERR value is not a valid float')

      key = @args[0]
      member = @args[2]

      sorted_set = @db.lookup_sorted_set_for_write(key)
      new_score = sorted_set.increment_score_by(member, incr)

      RESPBulkString.new(Utils.float_to_string(new_score))
    rescue InvalidFloatString
      RESPError.new('ERR hash value is not a float')
    rescue FloatNaN
      RESPError.new('ERR resulting score is not a number (NaN)')
    end

    def self.describe
      Describe.new('zincrby', 4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@sortedset', '@fast' ])
    end
  end
end
