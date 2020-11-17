module BYORedis
  class FlushDBCommand < BaseCommand

    def initialize(db, args)
      @db = db
      @args = args
    end

    def call
      Utils.assert_args_length(0, @args)
      @db.flush

      OKSimpleStringInstance
    end

    def self.describe
      Describe.new('flushdb', 1, [ 'write' ], 1, -1, 1, [ '@keyspace', '@write', '@slow' ])
    end
  end
end
