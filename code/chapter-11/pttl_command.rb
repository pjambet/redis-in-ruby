module BYORedis
  class PttlCommand < BaseCommand

    def call
      Utils.assert_args_length(1, @args)

      key = @args[0]
      ExpireHelper.check_if_expired(@db, key)
      key_exists = @db.data_store.include? key
      value = if key_exists
                entry = @db.expires[key]
                if entry
                  ttl = entry
                  (ttl - (Time.now.to_f * 1000)).round
                else
                  -1
                end
              else
                -2
              end
      RESPInteger.new(value)
    end

    def self.describe
      Describe.new('pttl', 2, [ 'readonly', 'random', 'fast' ], 1, 1, 1,
                   [ '@keyspace', '@read', '@fast' ])
    end
  end
end
