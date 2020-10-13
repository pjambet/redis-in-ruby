module BYORedis
  module Config

    UnsupportedConfigParameter = Class.new(StandardError)
    UnknownConfigType = Class.new(StandardError)

    DEFAULT = {
      set_max_intset_entries: 512,
      hash_max_ziplist_entries: 512,
      hash_max_ziplist_value: 64,
    }

    @config = DEFAULT.clone

    def self.set_config(key, value)
      key = key.to_sym
      existing_config = @config[key]
      raise UnsupportedConfigParameter, key unless existing_config

      case existing_config
      when Integer
        @config[key] = Utils.string_to_integer(value)
      else
        raise UnknownConfigType, "#{ key }/#{ value }"
      end
    end

    def self.get_config(key)
      @config[key.to_sym]
    end
  end
end
