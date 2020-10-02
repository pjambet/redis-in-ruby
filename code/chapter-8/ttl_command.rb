module BYORedis
  class TtlCommand < BaseCommand

    def call
      Utils.assert_args_length(1, @args)

      pttl_command = PttlCommand.new(@db, @args)
      result = pttl_command.execute_command.to_i
      if result > 0
        RESPInteger.new((result / 1000.0).round)
      else
        RESPInteger.new(result)
      end
    end

    def self.describe
      Describe.new('ttl', 2, [ 'readonly', 'random', 'fast' ], 1, 1, 1,
                   [ '@keyspace', '@read', '@fast' ])
    end
  end
end
