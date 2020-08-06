module Redis
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
                  ttl = @expires[key]
                  if ttl
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
  end
end
