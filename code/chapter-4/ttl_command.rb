class TtlCommand

  def initialize(data_store, expires, args)
    @data_store = data_store
    @expires = expires
    @args = args
  end

  def call
    if @args.length != 1
      "(error) ERR wrong number of arguments for 'TTL' command"
    else
      pttl_command = PttlCommand.new(@data_store, @expires, @args)
      result = pttl_command.call.to_i
      if result > 0
        (result / 1000.0).round
      else
        result
      end
    end
  end
end
