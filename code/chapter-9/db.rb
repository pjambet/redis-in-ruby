module BYORedis
  class DB

    attr_reader :data_store, :expires, :ready_keys, :blocking_keys, :client_timeouts,
                :unblocked_clients
    attr_writer :ready_keys

    def initialize
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
      flush
    end

    def flush
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

    def lookup_hash(key)
      hash = @data_store[key]
      raise WrongTypeError if hash && !hash.is_a?(RedisHash)

      hash
    end

    def lookup_hash_for_write(key)
      hash = lookup_hash(key)
      if hash.nil?
        hash = RedisHash.new
        @data_store[key] = hash
      end

      hash
    end

    def lookup_set(key)
      set = @data_store[key]
      raise WrongTypeError if set && !set.is_a?(RedisSet)

      set
    end

    def lookup_set_for_write(key)
      set = lookup_set(key)
      if set.nil?
        set = RedisSet.new
        @data_store[key] = set
      end

      set
    end

    def left_pop_from(key, list)
      generic_pop(key, list) do
        list.left_pop.value
      end
    end

    def right_pop_from(key, list)
      generic_pop(key, list) do
        list.right_pop.value
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

    def remove_from_set(key, set, member)
      removed = set.remove(member)
      @data_store.delete(key) if set.empty?

      removed
    end

    def pop_from_set(key, set, count)
      popped_members = if count.nil?
                         set.pop
                       else
                         set.pop_with_count(count)
                       end

      @data_store.delete(key) if set.empty?

      popped_members
    end

    def generic_pop(key, collection)
      popped = yield
      @data_store.delete(key) if collection.empty?

      if popped
        popped
      else
        @logger.warn(
          "Popped from an empty collection or a nil value: #{ key }/#{ collection.class }")

        nil
      end
    end
  end
end
