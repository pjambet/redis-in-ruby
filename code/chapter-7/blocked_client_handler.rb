module BYORedis
  class BlockedClientHandler
    def initialize(server, db)
      @server = server
      @db = db
      @logger = Logger.new(STDOUT)
      @logger.level = LOG_LEVEL
    end

    def handle(key)
      clients = @db.blocking_keys[key]
      unblocked_clients = []

      list = @db.data_store[key]

      if !list || !list.is_a?(List)
        @logger.warn "Something weird happened, not a list: #{ key } / #{ list }"
        raise "Unexpectedly found nothing or not a list: #{ key } / #{ list }"
      end

      raise "Unexpected empty list for #{ key }" if list.empty?

      cursor = clients.left_pop

      while cursor
        client = cursor.value

        if handle_client(client, key, list)
          unblocked_clients << client
        end

        if list.empty?
          break
        else
          cursor = clients.left_pop
        end
      end

      @db.blocking_keys.delete(key) if clients.empty?

      unblocked_clients
    end

    def handle_client(client, key, list)
      blocked_state = client.blocked_state

      # The client is expected to be blocked on a set of keys, we unblock it based on the key
      # arg, which itself comes from @db.ready_keys, which is populated when a key that is
      # blocked on receives a push
      # So we pop (left or right) from the list at key, and send the response to the client
      if client.blocked_state

        response = pop_operation(key, list, blocked_state.operation, blocked_state.target)

        serialized_response = response.serialize
        @logger.debug "Writing '#{ serialized_response.inspect } to #{ client }"

        unless Utils.safe_write(client.socket, serialized_response)
          # If we failed to write the value back, we put the element back in the list
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

    private

    def pop_operation(key, list, operation, target)
      case operation
      when :lpop
        RESPArray.new([ key, @db.left_pop_from(key, list) ])
      when :rpop
        RESPArray.new([ key, @db.right_pop_from(key, list) ])
      when :rpoplpush
        raise "Expected a target value for a brpoplpush handling: #{ key }" if target.nil?

        ListUtils.common_rpoplpush(@db, key, target, list)
      else
        raise "Unknown pop operation #{ operation }"
      end
    end

    def rollback_operation(key, response, operation, target_key)
      list = @db.lookup_list_for_write(key)
      case operation
      when :lpop
        element = response.underlying_array[1]
        list.left_push(element)
      when :rpop
        element = response.underlying_array[1]
        list.right_push(element)
      when :rpoplpush
        target_list = @db.lookup_list(target_key)
        element = target_list.left_pop
        @db.data_store.delete(target_key) if target_list.empty?
        list.right_push(element.value)
      else
        raise "Unknown pop operation #{ operation }"
      end
    end
  end
end
