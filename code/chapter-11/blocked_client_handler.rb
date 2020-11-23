module BYORedis
  class BlockedClientHandler

    def initialize(server, db)
      @server = server
      @db = db
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
    end

    def self.timeout_timestamp_or_nil(timeout)
      if timeout == 0
        nil
      else
        Time.now + timeout
      end
    end

    def handle(key)
      clients = @db.blocking_keys[key]

      list_or_set = @db.data_store[key]
      raise "Unexpected empty list/sorted set for #{ key }" if list_or_set&.empty?

      unblocked_clients = serve_client_blocked_on(key, list_or_set, clients)

      @db.blocking_keys.delete(key) if clients.empty?

      unblocked_clients
    end

    private

    def pop_operation(key, list_or_set, operation, target)
      case operation
      when :lpop
        RESPArray.new([ key, @db.left_pop_from(key, list_or_set) ])
      when :rpop
        RESPArray.new([ key, @db.right_pop_from(key, list_or_set) ])
      when :rpoplpush
        raise "Expected a target value for a brpoplpush handling: #{ key }" if target.nil?

        ListUtils.common_rpoplpush(@db, key, target, list_or_set)
      when :zpopmax
        RESPArray.new([ key ] + @db.pop_max_from(key, list_or_set))
      when :zpopmin
        RESPArray.new([ key ] + @db.pop_min_from(key, list_or_set))
      else
        raise "Unknown pop operation #{ operation }"
      end
    end

    def rollback_operation(key, response, operation, target_key)
      case operation
      when :lpop
        element = response.underlying_array[1]
        list = @db.lookup_list_for_write(key)
        list.left_push(element)
      when :rpop
        element = response.underlying_array[1]
        list = @db.lookup_list_for_write(key)
        list.right_push(element)
      when :rpoplpush
        list = @db.lookup_list_for_write(key)
        target_list = @db.lookup_list(target_key)
        element = target_list.left_pop
        @db.data_store.delete(target_key) if target_list.empty?
        list.right_push(element.value)
      when :zpopmax, :zpopmin
        sorted_set = @db.lookup_sorted_set_for_write(key)
        member = response.underlying_array[1]
        score = response.underlying_array[2]
        sorted_set.add(score, member)
      else
        raise "Unknown pop operation #{ operation }"
      end
    end

    def handle_client(client, key, list_or_set)
      blocked_state = client.blocked_state

      # The client is expected to be blocked on a set of keys, we unblock it based on the key
      # arg, which itself comes from @db.ready_keys, which is populated when a key that is
      # blocked on receives a push
      # So we pop (left or right for list, min or max for a set) at key, and send the response
      # to the client
      if client.blocked_state

        response =
          pop_operation(key, list_or_set, blocked_state.operation, blocked_state.target)

        serialized_response = response.serialize
        @logger.debug "Writing '#{ serialized_response.inspect } to #{ client }"

        unless Utils.safe_write(client.socket, serialized_response)
          # If we failed to write the value back, we put the element back in the list or set
          rollback_operation(key, response, blocked_state.operation, blocked_state.target)
          @server.disconnect_client(client)
          return
        end
      else
        @logger.warn "Client was not blocked, weird!: #{ client }"
        return
      end

      true
    end

    def serve_clients_blocked_on_lists(key, list_or_set, clients)
      generic_serve_clients(clients, list_or_set) do |client, clients_waiting_on_different_type|
        if is_client_blocked_on_list?(client)
          handle_client(client, key, list_or_set)
        else
          clients_waiting_on_different_type << client
          nil
        end
      end
    end

    def serve_clients_blocked_on_sorted_sets(key, list_or_set, clients)
      generic_serve_clients(clients, list_or_set) do |client, clients_waiting_on_different_type|
        if is_client_blocked_on_sorted_set?(client)
          handle_client(client, key, list_or_set)
        else
          clients_waiting_on_different_type << client
          nil
        end
      end
    end

    def generic_serve_clients(clients, list_or_set)
      unblocked_clients = []
      clients_waiting_on_different_type = []
      cursor = clients.left_pop

      while cursor
        client = cursor.value

        unblocked_clients << client if yield(client, clients_waiting_on_different_type)

        if list_or_set.empty?
          break
        else
          cursor = clients.left_pop
        end
      end

      return unblocked_clients, clients_waiting_on_different_type
    end

    def serve_client_blocked_on(key, list_or_set, clients)
      case list_or_set
      when List then
        unblocked_clients, clients_waiting_on_different_type =
          serve_clients_blocked_on_lists(key, list_or_set, clients)
      when RedisSortedSet
        unblocked_clients, clients_waiting_on_different_type =
          serve_clients_blocked_on_sorted_sets(key, list_or_set, clients)
      else
        @logger.warn "Found neither a list or sorted set: #{ key } / #{ list_or_set }"
        raise "Found nil or neither a list or sorted set: #{ key } / #{ list_or_set }"
      end

      # Take all the clients we set aside and add them back
      clients_waiting_on_different_type.each do |client|
        clients.right_push(client)
      end

      unblocked_clients
    end

    def is_client_blocked_on_list?(client)
      return false unless client.blocked_state

      [ :lpop, :rpop, :rpoplpush ].include?(client.blocked_state.operation)
    end

    def is_client_blocked_on_sorted_set?(client)
      return false unless client.blocked_state

      [ :zpopmax, :zpopmin ].include?(client.blocked_state.operation)
    end
  end
end
