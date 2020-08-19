module BYORedis
  class DelCommand

    def initialize(data_store, expires, args)
      @data_store = data_store
      @expires = expires
      @args = args
    end

    def call
      if @args.empty?
        RESPError.new("ERR wrong number of arguments for 'GET' command")
      else
        keys = @args
        deleted_count = 0
        keys.each do |key|
          entry = @data_store.delete(key)
          if entry != nil
            @expires.delete(key)
            deleted_count += 1
          end
        end

        RESPInteger.new(deleted_count)
      end
    end

    def self.describe
      [
        'del',
        -2, # arity
        # command flags
        [ RESPSimpleString.new('write') ],
        1, # position of first key in argument list
        -1, # position of last key in argument list
        1, # step count for locating repeating keys
        # acl categories: https://github.com/antirez/redis/blob/6.0/src/server.c#L161-L166
        [ '@keyspace', '@write', '@slow' ].map { |s| RESPSimpleString.new(s) },
      ]
    end
  end
end
