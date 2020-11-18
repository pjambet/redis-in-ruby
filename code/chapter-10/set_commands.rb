require_relative './redis_set'

module BYORedis
  module SetUtils
    def self.generic_sinter(db, args)
      sets = args.map do |set_key|
        set = db.lookup_set(set_key)

        return RedisSet.new if set.nil?

        set
      end

      RedisSet.intersection(sets)
    end

    def self.generic_set_store_operation(db, args)
      Utils.assert_args_length_greater_than(1, args)
      destination_key = args.shift
      sets = args.map { |other_set| db.lookup_set(other_set) }
      new_set = yield sets

      if new_set.empty?
        db.data_store.delete(destination_key)
      else
        db.data_store[destination_key] = new_set
      end

      RESPInteger.new(new_set.cardinality)
    end
  end

  class SAddCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      key = @args.shift
      new_member_count = 0

      set = @db.lookup_set_for_write(key)

      @args.each do |member|
        added = set.add(member)
        new_member_count += 1 if added
      end

      RESPInteger.new(new_member_count)
    end

    def self.describe
      Describe.new('sadd', -3, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end

  class SCardCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)
      set = @db.lookup_set(@args[0])

      cardinality = set.nil? ? 0 : set.cardinality
      RESPInteger.new(cardinality)
    end

    def self.describe
      Describe.new('scard', 2, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@set', '@fast' ])
    end
  end

  class SDiffCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      sets = @args.map { |other_set| @db.lookup_set(other_set) }

      RESPArray.new(RedisSet.difference(sets).members)
    end

    def self.describe
      Describe.new('sdiff', -2, [ 'readonly', 'sort_for_script' ], 1, -1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end

  class SDiffStoreCommand < BaseCommand
    def call
      SetUtils.generic_set_store_operation(@db, @args) do |sets|
        RedisSet.difference(sets)
      end
    end

    def self.describe
      Describe.new('sdiffstore', -3, [ 'write', 'denyoom' ], 1, -1, 1,
                   [ '@write', '@set', '@slow' ])
    end
  end

  class SInterCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      intersection = SetUtils.generic_sinter(@db, @args)

      RESPArray.new(intersection.members)
    end

    def self.describe
      Describe.new('sinter', -2, [ 'readonly', 'sort_for_script' ], 1, -1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end

  class SInterStoreCommand < BaseCommand
    def call
      SetUtils.generic_set_store_operation(@db, @args) do
        SetUtils.generic_sinter(@db, @args)
      end
    end

    def self.describe
      Describe.new('sinterstore', -3, [ 'write', 'denyoom' ], 1, -1, 1,
                   [ '@write', '@set', '@slow' ])
    end
  end

  class SUnionCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      sets = @args.map { |set_key| @db.lookup_set(set_key) }.compact

      RESPArray.new(RedisSet.union(sets).members)
    end

    def self.describe
      Describe.new('sunion', -2, [ 'readonly', 'sort_for_script' ], 1, -1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end

  class SUnionStoreCommand < BaseCommand
    def call
      SetUtils.generic_set_store_operation(@db, @args) do |sets|
        RedisSet.union(sets)
      end
    end

    def self.describe
      Describe.new('sunionstore', -3, [ 'write', 'denyoom' ], 1, -1, 1,
                   [ '@write', '@set', '@slow' ])
    end
  end

  class SIsMemberCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      set = @db.lookup_set(@args[0])
      if set
        presence = set.member?(@args[1]) ? 1 : 0
        RESPInteger.new(presence)
      else
        RESPInteger.new(0)
      end
    end

    def self.describe
      Describe.new('sismember', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@set', '@fast' ])
    end
  end

  class SMIsMemberCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      set = @db.lookup_set(@args.shift)
      members = @args

      if set.nil?
        result = Array.new(members.size, 0)
      else
        result = members.map do |member|
          set.member?(member) ? 1 : 0
        end
      end

      RESPArray.new(result)
    end

    def self.describe
      Describe.new('smismember', -3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@set', '@fast' ])
    end
  end

  class SMembersCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)
      set = @db.lookup_set(@args[0])

      RESPArray.new(set.members)
    end

    def self.describe
      Describe.new('smembers', 2, [ 'readonly', 'sort_for_script' ], 1, 1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end

  class SMoveCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      source_key = @args[0]
      source = @db.lookup_set(source_key)
      member = @args[2]
      destination = @db.lookup_set_for_write(@args[1])

      if source.nil?
        result = 0
      else
        removed = @db.remove_from_set(source_key, source, member)
        if removed
          destination.add(member)
          result = 1
        else
          result = 0
        end
      end

      RESPInteger.new(result)
    end

    def self.describe
      Describe.new('smove', 4, [ 'write', 'fast' ], 1, 2, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end

  class SPopCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      raise RESPSyntaxError if @args.length > 2

      if @args[1]
        count = Utils.validate_integer(@args[1])
        return RESPError.new('ERR index out of range') if count < 0
      end
      key = @args[0]
      set = @db.lookup_set(key)

      if set
        popped_members = @db.generic_pop(key, set) do
          if count.nil?
            set.pop
          else
            set.pop_with_count(count)
          end
        end

        RESPSerializer.serialize(popped_members)
      elsif count.nil?
        NullBulkStringInstance
      else
        EmptyArrayInstance
      end
    end

    def self.describe
      Describe.new('spop', -2, [ 'write', 'random', 'fast' ], 1, 1, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end

  class SRandMemberCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      raise RESPSyntaxError if @args.length > 2

      count = Utils.validate_integer(@args[1]) if @args[1]
      set = @db.lookup_set(@args[0])

      if set
        if count.nil?
          random_members = set.random_member
        else
          random_members = set.random_members_with_count(count)
        end

        RESPSerializer.serialize(random_members)
      elsif count.nil?
        NullBulkStringInstance
      else
        EmptyArrayInstance
      end
    end

    def self.describe
      Describe.new('srandmember', -2, [ 'readonly', 'random' ], 1, 1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end

  class SRemCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      key = @args.shift
      set = @db.lookup_set(key)
      remove_count = 0

      if set
        @args.each do |member|
          remove_count += 1 if @db.remove_from_set(key, set, member)
        end
      end

      RESPInteger.new(remove_count)
    end

    def self.describe
      Describe.new('srem', -3, [ 'write', 'fast' ], 1, 1, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end
end
