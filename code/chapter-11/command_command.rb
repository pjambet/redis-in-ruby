module BYORedis
  class CommandCommand < BaseCommand

    def initialize(_db, _args); end

    SORTED_COMMANDS = [
      CommandCommand,
      DelCommand,
      GetCommand,
      SetCommand,
      TtlCommand,
      PttlCommand,
      TypeCommand,
      # Lists
      LRangeCommand,
      LPushCommand,
      LPushXCommand,
      RPushCommand,
      RPushXCommand,
      LLenCommand,
      LPopCommand,
      BLPopCommand,
      RPopCommand,
      BRPopCommand,
      RPopLPushCommand,
      BRPopLPushCommand,
      LTrimCommand,
      LSetCommand,
      LRemCommand,
      LPosCommand,
      LInsertCommand,
      LIndexCommand,
      # Hashes
      HSetCommand,
      HGetAllCommand,
      HDelCommand,
      HExistsCommand,
      HGetCommand,
      HIncrByCommand,
      HIncrByFloatCommand,
      HKeysCommand,
      HLenCommand,
      HMGetCommand,
      HSetNXCommand,
      HStrLenCommand,
      HValsCommand,
      # Sets
      SAddCommand,
      SCardCommand,
      SDiffCommand,
      SDiffStoreCommand,
      SInterCommand,
      SInterStoreCommand,
      SIsMemberCommand,
      SMIsMemberCommand,
      SMembersCommand,
      SMoveCommand,
      SPopCommand,
      SRandMemberCommand,
      SRemCommand,
      SUnionCommand,
      SUnionStoreCommand,
    ]

    def call
      RESPArray.new(SORTED_COMMANDS.map { |command_class| command_class.describe.serialize })
    end

    def self.describe
      Describe.new('command', -1, [ 'random', 'loading', 'stale' ], 0, 0, 0,
                   [ '@slow', '@connection' ])
    end
  end
end
