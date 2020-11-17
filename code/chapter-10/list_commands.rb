require_relative './list'

module BYORedis

  ZeroRankError = Class.new(StandardError) do
    def message
      'ERR RANK can\'t be zero: use 1 to start from the first match, 2 from the second, ...'
    end
  end
  NegativeOptionError = Class.new(StandardError) do
    def initialize(field_name)
      @field_name = field_name
    end

    def message
      "ERR #{ @field_name } can\'t be negative"
    end
  end

  module ListUtils
    def self.common_lpush(list, elements)
      elements.each { |element| list.left_push(element) }
      RESPInteger.new(list.size)
    end

    def self.common_rpush(list, elements)
      elements.each { |element| list.right_push(element) }
      RESPInteger.new(list.size)
    end

    def self.common_find(args)
      Utils.assert_args_length_greater_than(1, args)

      yield args[0]
    end

    def self.find_or_create_list(db, args)
      common_find(args) do |key|
        db.lookup_list_for_write(key)
      end
    end

    def self.find_list(db, args)
      common_find(args) do |key|
        db.lookup_list(key)
      end
    end

    def self.common_xpush(list)
      if list.nil?
        RESPInteger.new(0)
      else
        yield
      end
    end

    def self.common_pop(db, args)
      Utils.assert_args_length(1, args)
      key = args[0]
      list = db.lookup_list(key)

      if list.nil?
        NullBulkStringInstance
      else
        value = yield key, list
        RESPBulkString.new(value)
      end
    end

    def self.common_bpop(db, args, operation)
      Utils.assert_args_length_greater_than(1, args)

      timeout = Utils.validate_timeout(args.pop)
      list_names = args

      list_names.each do |list_name|
        list = db.lookup_list(list_name)

        next if list.nil?

        popped = yield list_name, list
        return RESPArray.new([ list_name, popped ])
      end

      Server::BlockedState.new(BlockedClientHandler.timeout_timestamp_or_nil(timeout),
                               list_names, operation)
    end

    def self.common_rpoplpush(db, source_key, destination_key, source)
      if source_key == destination_key && source.size == 1
        source_tail = source.head.value
      else
        destination = db.lookup_list_for_write(destination_key)
        source_tail = db.right_pop_from(source_key, source)
        destination.left_push(source_tail)
      end

      RESPBulkString.new(source_tail)
    end
  end

  class LRangeCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      key = @args[0]
      start = Utils.validate_integer(@args[1])
      stop = Utils.validate_integer(@args[2])
      list = @db.lookup_list(key)

      if list.nil?
        EmptyArrayInstance
      else
        ListSerializer.new(list, start, stop)
      end
    end

    def self.describe
      Describe.new('lrange', 4, [ 'readonly' ], 1, 1, 1, [ '@read', '@list', '@slow' ])
    end
  end

  class LLenCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)
      key = @args[0]
      list = @db.lookup_list(key)

      if list.nil?
        RESPInteger.new(0)
      else
        RESPInteger.new(list.size)
      end
    end

    def self.describe
      Describe.new('llen', 2, [ 'readonly', 'fast' ], 1, 1, 1, [ '@read', '@list', '@fast' ])
    end
  end

  class LPopCommand < BaseCommand
    def call
      ListUtils.common_pop(@db, @args) do |key, list|
        @db.left_pop_from(key, list)
      end
    end

    def self.describe
      Describe.new('lpop', 2, [ 'write', 'fast' ], 1, 1, 1, [ '@write', '@list', '@fast' ])
    end
  end

  class LPushCommand < BaseCommand
    def call
      list = ListUtils.find_or_create_list(@db, @args)
      values = @args[1..-1]
      ListUtils.common_lpush(list, values)
    end

    def self.describe
      Describe.new('lpush', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@list', '@fast' ])
    end
  end

  class LPushXCommand < BaseCommand
    def call
      list = ListUtils.find_list(@db, @args)
      values = @args[1..-1]
      ListUtils.common_xpush(list) do
        ListUtils.common_lpush(list, values)
      end
    end

    def self.describe
      Describe.new('lpushx', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@list', '@fast' ])
    end
  end

  class RPushCommand < BaseCommand
    def call
      list = ListUtils.find_or_create_list(@db, @args)
      values = @args[1..-1]
      ListUtils.common_rpush(list, values)
    end

    def self.describe
      Describe.new('rpush', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@list', '@fast' ])
    end
  end

  class RPushXCommand < BaseCommand
    def call
      list = ListUtils.find_list(@db, @args)
      values = @args[1..-1]
      ListUtils.common_xpush(list) do
        ListUtils.common_rpush(list, values)
      end
    end

    def self.describe
      Describe.new('rpushx', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@list', '@fast' ])
    end
  end

  class RPopCommand < BaseCommand
    def call
      ListUtils.common_pop(@db, @args) do |key, list|
        @db.right_pop_from(key, list)
      end
    end

    def self.describe
      Describe.new('rpop', 2, [ 'write', 'fast' ], 1, 1, 1, [ '@write', '@list', '@fast' ])
    end
  end

  class RPopLPushCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)

      source_key = @args[0]
      source = @db.lookup_list(source_key)

      if source.nil?
        NullBulkStringInstance
      else
        destination_key = @args[1]
        ListUtils.common_rpoplpush(@db, source_key, destination_key, source)
      end
    end

    def self.describe
      Describe.new('rpoplpush', 3, [ 'write', 'denyoom' ], 1, 2, 1,
                   [ '@write', '@list', '@slow' ])
    end
  end

  class LTrimCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      key = @args[0]
      start = Utils.validate_integer(@args[1])
      stop = Utils.validate_integer(@args[2])
      list = @db.lookup_list(key)

      if list
        @db.trim(key, list, start, stop)
      end
      OKSimpleStringInstance
    end

    def self.describe
      Describe.new('ltrim', 4, [ 'write' ], 1, 1, 1, [ '@write', '@list', '@slow' ])
    end
  end

  class LSetCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      key = @args[0]
      index = Utils.validate_integer(@args[1])
      new_value = @args[2]
      list = @db.lookup_list(key)

      if list.nil?
        RESPError.new('ERR no such key')
      elsif list.set(index, new_value)
        OKSimpleStringInstance
      else
        RESPError.new('ERR index out of range')
      end
    end

    def self.describe
      Describe.new('lset', 4, [ 'write', 'denyoom' ], 1, 1, 1, [ '@write', '@list', '@slow' ])
    end
  end

  class LRemCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      key = @args[0]
      count = Utils.validate_integer(@args[1])
      element = @args[2]
      list = @db.lookup_list(key)

      if list.nil?
        RESPInteger.new(0)
      else
        RESPInteger.new(list.remove(count, element))
      end
    end

    def self.describe
      Describe.new('lrem', 4, [ 'write' ], 1, 1, 1, [ '@write', '@list', '@slow' ])
    end
  end

  class LPosCommand < BaseCommand

    def initialize(db, args)
      super
      @count = nil
      @maxlen = nil
      @rank = nil
    end

    def call
      Utils.assert_args_length_greater_than(1, @args)

      key = @args.shift
      element = @args.shift
      list = @db.lookup_list(key)

      parse_arguments unless @args.empty?

      if list.nil?
        NullBulkStringInstance
      else
        position = list.position(element, @count, @maxlen, @rank)
        if position.nil?
          NullBulkStringInstance
        elsif position.is_a?(Array)
          RESPArray.new(position)
        else
          RESPInteger.new(position)
        end
      end
    rescue ZeroRankError, NegativeOptionError => e
      RESPError.new(e.message)
    end

    def self.describe
      Describe.new('lpos', -3, [ 'readonly' ], 1, 1, 1, [ '@read', '@list', '@slow' ])
    end

    private

    def parse_arguments
      until @args.empty?
        option_name = @args.shift
        option_value = @args.shift
        raise RESPSyntaxError if option_value.nil?

        case option_name.downcase
        when 'rank'
          rank = Utils.validate_integer(option_value)
          raise ZeroRankError if rank == 0

          @rank = rank
        when 'count'
          count = Utils.validate_integer(option_value)
          raise NegativeOptionError, 'COUNT' if count < 0

          @count = count
        when 'maxlen'
          maxlen = Utils.validate_integer(option_value)
          raise NegativeOptionError, 'MAXLEN' if maxlen < 0

          @maxlen = maxlen
        else
          raise RESPSyntaxError
        end
      end
    end
  end

  class LInsertCommand < BaseCommand
    def call
      Utils.assert_args_length(4, @args)

      if ![ 'before', 'after' ].include?(@args[1].downcase)
        raise RESPSyntaxError
      else
        position = @args[1].downcase == 'before' ? :before : :after
      end

      pivot = @args[2]
      element = @args[3]
      list = @db.lookup_list(@args[0])

      return RESPInteger.new(0) if list.nil?

      new_size =
        if position == :before
          list.insert_before(pivot, element)
        else
          list.insert_after(pivot, element)
        end
      RESPInteger.new(new_size)
    end

    def self.describe
      Describe.new('linsert', 5, [ 'write', 'denyoom' ], 1, 1, 1,
                   [ '@write', '@list', '@slow' ])
    end
  end

  class LIndexCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)

      key = @args[0]
      index = Utils.validate_integer(@args[1])
      list = @db.lookup_list(key)

      if list.nil?
        NullBulkStringInstance
      else
        value_at_index = list.at_index(index)
        if value_at_index
          RESPBulkString.new(value_at_index)
        else
          NullBulkStringInstance
        end
      end
    end

    def self.describe
      Describe.new('lindex', 3, [ 'readonly' ], 1, 1, 1, [ '@read', '@list', '@slow' ])
    end
  end

  ###
  # Blocking
  ###

  class BLPopCommand < BaseCommand
    def call
      ListUtils.common_bpop(@db, @args, :lpop) do |list_name, list|
        @db.left_pop_from(list_name, list)
      end
    end

    def self.describe
      Describe.new('blpop', -3, [ 'write', 'noscript' ], 1, -2, 1,
                   [ '@write', '@list', '@slow', '@blocking' ])
    end
  end

  class BRPopCommand < BaseCommand
    def call
      ListUtils.common_bpop(@db, @args, :rpop) do |list_name, list|
        @db.right_pop_from(list_name, list)
      end
    end

    def self.describe
      Describe.new('brpop', -3, [ 'write', 'noscript' ], 1, -2, 1,
                   [ '@write', '@list', '@slow', '@blocking' ])
    end
  end

  class BRPopLPushCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)

      source_key = @args[0]
      source = @db.lookup_list(source_key)
      timeout = Utils.validate_timeout(@args[2])
      destination_key = @args[1]

      if source.nil?
        Server::BlockedState.new(BlockedClientHandler.timeout_timestamp_or_nil(timeout),
                                 [ source_key ], :rpoplpush, destination_key)
      else
        ListUtils.common_rpoplpush(@db, source_key, destination_key, source)
      end
    end

    def self.describe
      Describe.new('brpoplpush', 4, [ 'write', 'denyoom', 'noscript' ], 1, 2, 1,
                   [ '@write', '@list', '@slow', '@blocking' ])
    end
  end
end
