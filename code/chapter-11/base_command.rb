module BYORedis
  class BaseCommand

    Describe = Struct.new(:name, :arity, :flags, :first_key_position, :last_key_position,
                          :step_count, :acl_categories) do
      def serialize
        [
          name,
          arity,
          flags.map { |flag| RESPSimpleString.new(flag) },
          first_key_position,
          last_key_position,
          step_count,
          acl_categories.map { |category| RESPSimpleString.new(category) },
        ]
      end
    end

    def initialize(db, args)
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      @db = db
      @args = args
    end

    def execute_command
      call
    rescue InvalidArgsLength => e
      @logger.debug e.message
      command_name = self.class.describe.name.upcase
      e.resp_error(command_name)
    rescue WrongTypeError, RESPSyntaxError, ValidationError => e
      e.resp_error
    end

    def call
      raise NotImplementedError
    end
  end
end
