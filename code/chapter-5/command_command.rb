module Redis
  class CommandCommand

    def initialize(data_store, expires, args)
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      @data_store = data_store
      @expires = expires
      @args = args
    end

    def call
      RESPArray.new(Server::COMMANDS.map { |_, command_class| command_class.describe } )
    end

    def self.describe
      [
        'command',
        -1, # arity
        # command flags
        [ 'random', 'loading', 'stale' ].map { |s| RESPSimpleString.new(s) },
        0, # position of first key in argument list
        0, # position of last key in argument list
        0, # step count for locating repeating keys
        # acl categories: https://github.com/antirez/redis/blob/6.0/src/server.c#L161-L166
        [ '@slow', '@connection' ].map { |s| RESPSimpleString.new(s) },
      ]
    end
  end
end
