module BYORedis
  class TypeCommand < BaseCommand

    def call
      Utils.assert_args_length(1, @args)

      key = @args[0]
      ExpireHelper.check_if_expired(@db, key)
      value = @db.data_store[key]

      case value
      when nil       then RESPSimpleString.new('none')
      when String    then RESPSimpleString.new('string')
      when List      then RESPSimpleString.new('list')
      when RedisHash then RESPSimpleString.new('hash')
      else raise "Unknown type for #{ value }"
      end
    end

    def self.describe
      Describe.new('type', 2, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@keyspace', '@read', '@fast' ])
    end
  end
end
