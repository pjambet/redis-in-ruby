module BYORedis
  class TypeCommand < BaseCommand

    def call
      Utils.assert_args_length(1, @args)

      key = @args[0]
      ExpireHelper.check_if_expired(@db, key)
      value = @db.data_store[key]

      type = case value
             when nil            then 'none'
             when String         then 'string'
             when List           then 'list'
             when RedisHash      then 'hash'
             when RedisSet       then 'set'
             when RedisSortedSet then 'zset'
             else raise "Unknown type for #{ value }"
             end

      RESPSimpleString.new(type)
    end

    def self.describe
      Describe.new('type', 2, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@keyspace', '@read', '@fast' ])
    end
  end
end
