module BYORedis
  class RedisHash

    ListEntry = Struct.new(:key, :value)

    def initialize
      @underlying = List.new
    end

    def empty?
      @underlying.empty?
    end

    def keys
      case @underlying
      when List then keys_list
      when Dict then @underlying.keys
      else raise "Unknown structure type: #{ @underlying }"
      end
    end

    def values
      case @underlying
      when List then values_list
      when Dict then @underlying.values
      else raise "Unknown structure type: #{ @underlying }"
      end
    end

    def length
      case @underlying
      when List then @underlying.size
      when Dict then @underlying.used
      else raise "Unknown structure type: #{ @underlying }"
      end
    end

    def set(key, value)
      max_string_length = Config.get_config(:hash_max_ziplist_value)
      convert_list_to_dict if @underlying.is_a?(List) &&
                              (key.length > max_string_length || value.length > max_string_length)

      case @underlying
      when List then
        added = set_list(key, value)
        if @underlying.size + length > Config.get_config(:hash_max_ziplist_entries)
          convert_list_to_dict
        end
        added
      when Dict then @underlying.set(key, value)
      else raise "Unknown structure type: #{ @underlying }"
      end
    end
    alias []= set

    def get_all
      case @underlying
      when List then get_all_list
      when Dict then get_all_dict
      else raise "Unknown structure type: #{ @underlying }"
      end
    end

    def get(field)
      case @underlying
      when List then get_list(field)
      when Dict then @underlying[field]
      else raise "Unknown structure type: #{ @underlying }"
      end
    end
    alias [] get

    def delete(field)
      case @underlying
      when List then was_deleted = delete_from_list(field)
      when Dict then
        was_deleted = !@underlying.delete(field).nil?
        if was_deleted && length - 1 == Config.get_config(:hash_max_ziplist_entries)
          convert_dict_to_list
        elsif @underlying.needs_resize?
          @underlying.resize
        end
      else raise "Unknown structure type: #{ @underlying }"
      end

      was_deleted
    end

    private

    def set_list(key, value)
      iterator = List.left_to_right_iterator(@underlying)
      while iterator.cursor && iterator.cursor.value.key != key
        iterator.next
      end

      if iterator.cursor.nil?
        @underlying.right_push(ListEntry.new(key, value))

        true
      else
        iterator.cursor.value.value = value

        false
      end
    end

    def get_all_list
      iterator = List.left_to_right_iterator(@underlying)
      pairs = []
      while iterator.cursor
        pairs.push(iterator.cursor.value.key, iterator.cursor.value.value)
        iterator.next
      end

      pairs
    end

    def get_all_dict
      pairs = []

      @underlying.each do |key, value|
        pairs.push(key, value)
      end

      pairs
    end

    def get_list(field)
      iterator = List.left_to_right_iterator(@underlying)

      while iterator.cursor
        return iterator.cursor.value.value if iterator.cursor.value.key == field

        iterator.next
      end
    end

    def convert_list_to_dict
      dict = Dict.new
      iterator = List.left_to_right_iterator(@underlying)

      while iterator.cursor
        dict[iterator.cursor.value.key] = iterator.cursor.value.value
        iterator.next
      end

      @underlying = dict
    end

    def convert_dict_to_list
      list = List.new
      @underlying.each do |key, value|
        list.right_push(ListEntry.new(key, value))
      end

      @underlying = list
    end

    def delete_from_list(field)
      was_deleted = false
      iterator = List.left_to_right_iterator(@underlying)

      while iterator.cursor
        if iterator.cursor.value.key == field
          @underlying.remove_node(iterator.cursor)

          return true
        end

        iterator.next
      end

      was_deleted
    end

    def keys_list
      iterator = List.left_to_right_iterator(@underlying)
      keys = []

      while iterator.cursor
        keys << iterator.cursor.value.key

        iterator.next
      end

      keys
    end

    def values_list
      iterator = List.left_to_right_iterator(@underlying)
      values = []

      while iterator.cursor
        values << iterator.cursor.value.value

        iterator.next
      end

      values
    end
  end
end
