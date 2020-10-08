require_relative './t_hash'

module BYORedis

  class HSetCommand < BaseCommand
    def call
      key = @args.shift
      Utils.assert_args_length(1, [ key ].compact)
      Utils.assert_args_length_multiple_of(2, @args)
      pairs = @args.each_slice(2).to_a

      hash = @db.data_store[key]
      if hash.nil?
        hash = @db.data_store[key] = THash.new
      end

      count = 0

      pairs.each do |pair|
        key = pair[0]
        value = pair[1]

        new_pair_count = hash.set(key, value)
        count += new_pair_count
      end

      RESPInteger.new(count)
    end

    def self.describe
      Describe.new('hset', -4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@hash', '@fast' ])
    end
  end

  class HGetAllCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)

      hash = @db.data_store[@args[0]]

      if hash.nil?
        pairs = []
      else
        pairs = hash.get_all
      end

      RESPArray.new(pairs)
    end

    def self.describe
      Describe.new('hgetall', 2, [ 'readonly', 'random' ], 1, 1, 1,
                   [ '@read', '@hash', '@slow' ])
    end
  end

  class HDelCommand < BaseCommand
    def call
      Utils.assert_args_length_greater_than(1, @args)
      key = @args.shift
      hash = @db.data_store[key]

      delete_count = 0
      if hash
        delete_count += @db.delete_from_hash(key, hash, @args)
      end

      RESPInteger.new(delete_count)
    end

    def self.describe
      Describe.new('hdel', -3, [ 'write', 'fast' ], 1, 1, 1,
                   [ '@write', '@hash', '@fast' ])
    end
  end

  class HExistsCommand < BaseCommand
  end

  class HGetCommand < BaseCommand
  end

  class HIncrByCommand < BaseCommand
  end

  class HIncrByFloatCommand < BaseCommand
  end

  class HKeysCommand < BaseCommand
  end

  class HLenCommand < BaseCommand
  end

  class HMGetCommand < BaseCommand
  end

  class HSetNxCommand < BaseCommand
  end

  class HStrLenCommand < BaseCommand
  end

  class HValsCommand < BaseCommand
  end
end
