module BYORedis
  class GetCommand < BaseCommand

    def call
      Utils.assert_args_length(1, @args)

      key = @args[0]
      ExpireHelper.check_if_expired(@db, key)
      value = @db.lookup_string(key)

      if value.nil?
        NullBulkStringInstance
      else
        RESPBulkString.new(value)
      end
    end

    def self.describe
      Describe.new('get', 2, [ 'readonly', 'fast' ], 1, 1, 1, [ '@read', '@string', '@fast' ])
    end
  end
end
