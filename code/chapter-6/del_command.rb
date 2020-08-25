module Redis
  class DelCommand

    def initialize(data_store, expires, args)
      # @logger = Logger.new(STDOUT)
      # @logger.level = LOG_LEVEL
      @data_store = data_store
      @expires = expires
      @args = args

      @options = Dict.new($random_bytes)
    end

    def call
      if @args.empty?
        RESPError.new("ERR wrong number of arguments for 'GET' command")
      else
        keys = @args
        deleted_count = 0
        keys.each do |key|
          p @data_store
          entry = @data_store.delete(key)
          if entry != nil
            @expires.delete(key)
            deleted_count += 1
          end
          p entry
        end

        RESPInteger.new(deleted_count)
      end
    end

    def describe
      []
    end
  end
end
