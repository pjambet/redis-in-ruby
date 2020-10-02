module BYORedis
  class DelCommand < BaseCommand

    def initialize(db, args)
      @db = db
      @args = args
    end

    def call
      Utils.assert_args_length_greater_than(0, @args)

      keys = @args
      deleted_count = 0
      keys.each do |key|
        entry = @db.data_store.delete(key)
        if entry != nil
          @db.expires.delete(key)
          deleted_count += 1
        end
      end

      RESPInteger.new(deleted_count)
    end

    def self.describe
      Describe.new('del', -2, [ 'write' ], 1, -1, 1, [ '@keyspace', '@write', '@slow' ])
    end
  end
end
