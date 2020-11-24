require_relative './bit_ops'

module BYORedis
  class GetBitCommand < BaseCommand
    def call
      Utils.assert_args_length(2, @args)
      string = @db.lookup_string(@args[0])
      offset = Utils.validate_integer_with_message(
        @args[1], 'ERR bit offset is not an integer or out of range')

      if offset < 0
        RESPError.new('ERR bit offset is not an integer or out of range')
      else
        RESPInteger.new(BitOps.new(string).get_bit(offset))
      end
    end

    def self.describe
      Describe.new('getbit', 3, [ 'readonly', 'fast' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@fast' ])
    end
  end

  class SetBitCommand < BaseCommand
    def call
    end

    def self.describe
      Describe.new('setbit', 3, [ 'write', 'denyoom' ], 1, 1, 1,
                   [ '@write', '@bitmap', '@slow' ])
    end
  end

  class BitOpCommand < BaseCommand
    def call
    end

    def self.describe
      Describe.new('bitop', -4, [ 'write', 'denyoom' ], 2, -1, 1,
                   [ '@write', '@bitmap', '@slow' ])
    end
  end

  class BitCountCommand < BaseCommand
    def call
    end

    def self.describe
      Describe.new('bitcount', -2, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@slow' ])
    end
  end

  class BitPosCommand < BaseCommand
    def call
    end

    def self.describe
      Describe.new('bitpos', -3, [ 'readonly' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@slow' ])
    end
  end

  class BitFieldCommand < BaseCommand
    def call
    end

    def self.describe
      Describe.new('bitfield', -2, [ 'write', 'denyoom' ], 1, 1, 1,
                   [ '@read', '@bitmap', '@slow' ])
    end
  end
end
