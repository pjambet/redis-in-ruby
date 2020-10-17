require_relative './redis_set'

module BYORedis
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
      Describe.new('sadd', 4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end

  class SCardCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)
      set = @db.lookup_set(@args[0])

      cardinality = if set.nil?
                      0
                    else
                      set.cardinality
                    end
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
      first_set = @db.lookup_set(@args.shift)

      other_sets = @args.map { |other_set| @db.lookup_set(other_set) }

      if first_set
        diff = first_set.diff(other_sets)
      else
        diff = RedisSet.new
      end
      p '---'
      p diff
      SetSerializer.new(diff)
    end

    def self.describe
      Describe.new('sdiff', -2, [ 'readonly', 'sort_for_script' ], 1, -1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end

  class SDiffStoreCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      destination_key = @args.shift
      first_set = @db.lookup_set(@args.shift)

      other_sets = @args.map { |other_set| @db.lookup_set(other_set) }

      if first_set
        diff = first_set.diff(other_sets)
        @db.data_store[destination_key] = diff
        cardinality = diff.cardinality
      else
        cardinality = 0
      end

      RESPInteger.new(cardinality)
    end

    def self.describe
      Describe.new('sdiffstore', -3, [ 'write', 'denyoom' ], 1, -1, 1,
                   [ '@write', '@set', '@slow' ])
    end
  end

  class SInterCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
      sets = @args.map do |set_key|
        set = @db.lookup_set(set_key)

        if set.nil?
          return EmptyArrayInstance
        else
          set
        end
      end

      intersection = []
      sets.sort_by!(&:cardinality)
      sets[0].each do |member|
        present_in_all_other_sets = true
        sets[1..-1].each do |set|
          unless set.contains?(member)
            present_in_other_sets = false
            break
          end
        end
        intersection.append(member) if present_in_all_other_sets
      end

      RESPSerializer.serialize(intersection)

      # Sort the sets smallest to largest
      # ...
      # Iterate over the first set, if we find a set that does not contain it, discard
      # ...
      # Otherwise, keep
    end

    def self.describe
      Describe.new('sinter', -2, [ 'readonly', 'sort_for_script' ], 1, -1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end

  class SInterStoreCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      dest_key = @args.shift
      sets = @args.map do |set_key|
        set = @db.lookup_set(set_key)

        if set.nil?
          @db.data_store.delete(dest_key)
          return RESPInteger.new(0)
        else
          set
        end
      end

      new_set = RedisSet.new
      sets.sort_by!(&:cardinality)
      sets[0].each do |member|
        present_in_all_other_sets = true
        sets[1..-1].each do |set|
          unless set.contains?(member)
            present_in_other_sets = false
            break
          end
        end
        new_set.add(member) if present_in_all_other_sets
      end

      if new_set.cardinality > 0
        @db.data_store[dest_key] = new_set
      end

      cardinality = new_set.cardinality
      RESPInteger.new(cardinality)
    end

    def self.describe
      Describe.new('sinterstore', -3, [ 'write', 'denyoom' ], 1, -1, 1,
                   [ '@write', '@set', '@slow' ])
    end
  end

  class SIsMemberCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      set = @db.lookup_set(@args[0])
      if set
        presence = set.contains?(@args[1]) ? 1 : 0
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
    end

    def self.describe
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
      Utils.assert_args_length_greater_than(3, @args)
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
        count = OptionUtils.validate_integer(@args[1])
        return RESPError.new('ERR index out of range') if count < 0
      end
      set = @db.lookup_set(@args[0])

      if set
        popped = set.pop(count)
        @db.data_store.delete(@args[0]) if set.empty?
        RESPSerializer.serialize(popped)
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

      count = OptionUtils.validate_integer(@args[1]) if @args[1]
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
      set = @db.lookup_set(@args.shift)
      member_keys = @args

    end

    def self.describe
      Describe.new('srem', -3, [ 'write', 'fast' ], 1, 1, 1,
                   [ '@write', '@set', '@fast' ])
    end
  end

  class SUnionCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(0, @args)
    end

    def self.describe
      Describe.new('sunion', -2, [ 'readonly', 'sort_for_script' ], 1, -1, 1,
                   [ '@read', '@set', '@slow' ])
    end
  end

  class SUnionStoreCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
    end

    def self.describe
      Describe.new('sunionstore', -3, [ 'write', 'denyoom' ], 1, -1, 1,
                   [ '@write', '@set', '@slow' ])
    end
  end
end
