module BYORedis
  class TtlCommand

    def initialize(data_store, expires, args)
      @data_store = data_store
      @expires = expires
      @args = args
    end

    def call
      if @args.length != 1
        RESPError.new("ERR wrong number of arguments for 'TTL' command")
      else
        pttl_command = PttlCommand.new(@data_store, @expires, @args)
        result = pttl_command.call.to_i
        if result > 0
          RESPInteger.new((result / 1000.0).round)
        else
          RESPInteger.new(result)
        end
      end
    end

    def self.describe
      [
        'ttl',
        2, # arity
        # command flags
        [ 'readonly', 'random', 'fast' ].map { |s| RESPSimpleString.new(s) },
        1, # position of first key in argument list
        1, # position of last key in argument list
        1, # step count for locating repeating keys
        # acl categories: https://github.com/antirez/redis/blob/6.0/src/server.c#L161-L166
        [ '@keyspace', '@read', '@fast' ].map { |s| RESPSimpleString.new(s) },
      ]
    end
  end
end
