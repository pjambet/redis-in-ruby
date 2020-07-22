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
      "(error) ERR wrong number of arguments for 'GET' command"
    else
      check_if_expired
      @data_store.fetch(@args[0], '(nil)')
    end
  end

  private

  def check_if_expired
    expires_entry = @expires[@args[0]]
    if expires_entry && expires_entry < Time.now.to_f * 1000
      @logger.debug "evicting #{ @args[0] }"
      @expires.delete(@args[0])
      @data_store.delete(@args[0])
    end
  end
end
