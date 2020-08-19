module BYORedis
  class PttlCommand

    def initialize(data_store, expires, args)
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      @data_store = data_store
      @expires = expires
      @args = args
    end

    def call
      if @args.length != 1
        RESPError.new("ERR wrong number of arguments for 'PTTL' command")
      else
        key = @args[0]
        ExpireHelper.check_if_expired(@data_store, @expires, key)
        key_exists = @data_store.include? key
        value = if key_exists
                  entry = @expires[key]
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
    end

    def self.describe
      [
        'pttl',
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
