#!/usr/bin/env ruby

module BYORedis
  class GetBitCommand < BaseCommand
    def call
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
