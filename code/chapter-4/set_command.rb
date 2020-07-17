class SetCommand

  def initialize(data_store, expires, args)
    @data_store = data_store
    @expires = expires
    @args = args
  end

  def call
    if @args.length == 2
      @data_store[@args[0]] = @args[1]
      'OK'
    elsif @args.length == 4 && @args[2] == 'EX'
      if !integer?(@args[3])
        '(error) ERR value is not an integer or out of range'
      elsif @args[3].to_i <= 0
        '(error) ERR invalid expire time in set'
      else
        @data_store[@args[0]] = @args[1]
        when_ms = ((Time.now + @args[3].to_i).to_f * 1000).to_i
        puts when_ms
        @expires[@args[0]] = when_ms
        'OK'
      end
    elsif @args.length == 4 && @args[2] == 'PX'
      if !integer?(@args[3])
        '(error) ERR value is not an integer or out of range'
      elsif @args[3].to_i <= 0
        '(error) ERR invalid expire time in set'
      else
        @data_store[@args[0]] = @args[1]
        when_ms = (Time.now.to_f * 1000).to_i + @args[3].to_i
        puts when_ms
        @expires[@args[0]] = when_ms
        'OK'
      end
    else
      "(error) ERR wrong number of arguments for 'SET' command"
    end
  end

  private

  def integer?(str)
    !!Integer(str)
  rescue ArgumentError, TypeError
    false
  end
end
