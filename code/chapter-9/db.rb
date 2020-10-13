module BYORedis
  class DB

    attr_reader :data_store, :expires, :ready_keys, :blocking_keys, :client_timeouts,
                :unblocked_clients
    attr_writer :ready_keys

    def initialize
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      @data_store = Dict.new
      @expires = Dict.new
      @ready_keys = Dict.new
      @blocking_keys = Dict.new
      @client_timeouts = SortedArray.new(:timeout)
      @unblocked_clients = List.new
    end

    def lookup_string(key)
      string_value = @data_store[key]
      raise WrongTypeError if string_value && !string_value.is_a?(String)

      string_value
    end

    def lookup_list(key)
      list = @data_store[key]
      raise WrongTypeError if list && !list.is_a?(List)

      list
    end

    def lookup_set(key)
      set = @data_store[key]
      raise WrongTypeError if set && !set.is_a?(RedisSet)

      set
    end

    def lookup_list_for_write(key)
      list = lookup_list(key)
      if list.nil?
        list = List.new
        @data_store[key] = list

        if @blocking_keys[key]
          @ready_keys[key] = nil
        end
      end

      list
    end

    def left_pop_from(key, list)
      generic_pop_wrapper(key, list) do
        list.left_pop
      end
    end

    def right_pop_from(key, list)
      generic_pop_wrapper(key, list) do
        list.right_pop
      end
    end

    def trim(key, list, start, stop)
      list.trim(start, stop)
      @data_store.delete(key) if list.empty?
    end

    def delete_from_hash(key, hash, fields)
      delete_count = 0
      fields.each do |field|
        delete_count += (hash.delete(field) == true ? 1 : 0)
      end
      @data_store.delete(key) if hash.empty?

      delete_count
    end

    private

    def generic_pop_wrapper(key, list)
      popped = yield
      @data_store.delete(key) if list.empty?

      if popped
        popped.value
      else
        @logger.warn("Unexpectedly popped from an empty list or a nil value: #{ key }")
        nil
      end
    end
  end
end
