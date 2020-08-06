module Redis
  class GetCommand

    def initialize(data_store, expires, args)
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      @data_store = data_store
      @expires = expires
      @args = args
    end

    def call
      if @args.length != 1
        RESPError.new("ERR wrong number of arguments for 'GET' command")
      else
        key = @args[0]
        ExpireHelper.check_if_expired(@data_store, @expires, key)
        value = @data_store[key]
        if value.nil?
          NullBulkStringInstance
        else
          RESPBulkString.new(value)
        end
      end
    end
  end
end
