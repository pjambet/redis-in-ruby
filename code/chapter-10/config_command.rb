module BYORedis
  class ConfigCommand < BaseCommand

    def call
      if @args[0] != 'SET' && @args[0] != 'GET'
        message =
          "ERR Unknown subcommand or wrong number of arguments for '#{ @args[0] }'. Try CONFIG HELP."
        RESPError.new(message)
      elsif @args[0] == 'GET'
        Utils.assert_args_length(2, @args)
        value = Config.get_config(@args[1].to_sym)
        return RESPBulkString.new(Utils.integer_to_string(value))
      elsif @args[0] == 'SET'
        Utils.assert_args_length_greater_than(2, @args)
        @args.shift # SET
        @args.each_slice(2) do |key, value|
          raise RESPSyntaxError if key.nil? || value.nil?

          Config.set_config(key, value)
        end
      end

      OKSimpleStringInstance
    end

    def self.describe
      Describe.new('config', -2, [ 'admin', 'noscript', 'loading', 'stale' ], 0, 0, 0,
                   [ '@admin', '@slow', '@dangerous' ])
    end
  end
end
