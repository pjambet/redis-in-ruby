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
    end

    def self.describe
    end
  end

  class SInterStoreCommand < BaseCommand
    def call
    end

    def self.describe
    end
  end

  class SIsMemberCommand < BaseCommand
    def call
    end

    def self.describe
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
    end

    def self.describe
    end
  end

  class SPopCommand < BaseCommand
    def call
    end

    def self.describe
    end
  end

  class SRandMemberCommand < BaseCommand
    def call
    end

    def self.describe
    end
  end

  class SRemCommand < BaseCommand
    def call
    end

    def self.describe
    end
  end

  class SUnionCommand < BaseCommand
    def call
    end

    def self.describe
    end
  end

  class SUnionStoreCommand < BaseCommand
    def call
    end

    def self.describe
    end
  end
end
