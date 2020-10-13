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
    def call
      Utils.assert_args_length(2, @args)

      hash = @db.data_store[@args[0]]

      if hash.nil?
        RESPInteger.new(0)
      else
        value = hash[@args[1]]
        if value.nil?
          RESPInteger.new(0)
        else
          RESPInteger.new(1)
        end
      end
    end

    def self.describe
      Describe.new('hexists', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@hash', '@fast' ])
    end
  end

  class HGetCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)

      hash = @db.data_store[@args[0]]

      if hash.nil?
        NullBulkStringInstance
      else
        key = @args[1]
        value = hash[key]
        if value.nil?
          NullBulkStringInstance
        else
          RESPBulkString.new(value)
        end
      end
    end

    def self.describe
      Describe.new('hget', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@hash', '@fast' ])
    end
  end

  class HIncrByCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      OptionUtils.validate_integer(@args[2])

      key = @args[0]
      field = @args[1]
      incr = Utils.string_to_integer(@args[2])

      hash = @db.data_store[key]

      if hash.nil?
        hash = THash.new
        @db.data_store[key] = hash
      end

      value = hash[field]
      if value.nil?
        value = 0
      else
        p "BEFORE"
        p value
        value = Utils.string_to_integer(value)
        p 'AFTER'
        p value
      end

      new_value = value + incr
      if new_value > BYORedis::LLONG_MAX || new_value < BYORedis::LLONG_MIN
        raise IntegerOverflow
      end

      hash[field] = Utils.integer_to_string(new_value)

      RESPInteger.new(new_value)
    rescue InvalidIntegerString => e
      RESPError.new('ERR hash value is not an integer')
    rescue IntegerOverflow => e
      RESPError.new('ERR increment or decrement would overflow')
    end

    def self.describe
      Describe.new('hincrby', 4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@hash', '@fast' ])
    end
  end

  class HIncrByFloatCommand < BaseCommand
    def call
      Utils.assert_args_length(3, @args)
      OptionUtils.validate_float_with_message(@args[2], 'ERR value is not a valid float')

      key = @args[0]
      field = @args[1]
      incr = Utils.string_to_float(@args[2])

      hash = @db.data_store[key]

      if hash.nil?
        hash = THash.new
        @db.data_store[key] = hash
      end

      value = hash[field]
      if value.nil?
        value = 0.0
      else
        value = Utils.string_to_float(value)
      end

      new_value = value + incr

      if new_value.nan? || new_value.infinite? # or call .finite?
        raise FloatOverflow
      end

      hash[field] = new_value

      RESPBulkString.new(Utils.float_to_string(new_value))
    rescue InvalidFloatString => e
      RESPError.new('ERR hash value is not a float')
    rescue FloatOverflow => e
      # Not sure how to _really_ test that
      RESPError.new('ERR increment would produce NaN or Infinity')
    end

    def self.describe
      Describe.new('hincrbyfloat', 4, [ 'write', 'denyoom', 'fast' ], 1, 1, 1,
                   [ '@write', '@hash', '@fast' ])
    end
  end

  class HKeysCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)

      hash = @db.data_store[@args[0]]

      if hash.nil?
        NullArrayInstance
      else
        RESPArray.new(hash.keys)
      end
    end

    def self.describe
      Describe.new('hkeys', 2, [ 'readonly', 'sort_for_script' ], 1, 1, 1,
                   [ '@read', '@hash', '@slow' ])
    end
  end

  class HLenCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)

      hash = @db.data_store[@args[0]]
      hash_length = 0

      unless hash.nil?
        hash_length = hash.length
      end

      RESPInteger.new(hash_length)
    end

    def self.describe
      Describe.new('hlen', 2, [ 'readonly', 'sort_for_script' ], 1, 1, 1,
                   [ '@read', '@hash', '@slow' ])
    end
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