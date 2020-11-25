module BYORedis
  class StrLenCommand < BaseCommand
    def call
      Utils.assert_args_length(1, @args)
      string = @db.lookup_string(@args[0])

      length = string ? string.bytesize : 0
      RESPInteger.new(length)
    end

    def self.describe
      Describe.new('strlen', 2, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@string', '@fast' ])
    end
  end
end
