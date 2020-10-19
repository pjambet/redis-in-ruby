module BYORedis
  class RedisHash

    ListEntry = Struct.new(:key, :value)

    def initialize
      @max_list_size = ENV['HASH_MAX_ZIPLIST_ENTRIES'].to_i.then do |max|
        max <= 0 ? 256 : max
      end
      @underlying_structure = List.new
      @size = 0
    end

    def empty?
      @size == 0
    end

    def keys
      case @underlying_structure
      when List then keys_list
      when Dict then @underlying_structure.keys
      else raise "Unknown structure type: #{ @underlying_structre }"
      end
    end

    def values
      case @underlying_structure
      when List then values_list
      when Dict then @underlying_structure.values
      else raise "Unknown structure type: #{ @underlying_structre }"
      end
    end

    def length
      case @underlying_structure
      when List then @underlying_structure.size
      when Dict then @underlying_structure.used
      else raise "Unknown structure type: #{ @underlying_structre }"
      end
    end

    def set(key, value)
      case @underlying_structure
      when List then
        new_pair_count = set_list(key, value)
        if new_pair_count + @size > @max_list_size
          convert_list_to_dict
        end
      when Dict then
        existing_pair_count = @underlying_structure.used
        @underlying_structure[key] = value
        new_pair_count = @underlying_structure.used - existing_pair_count
      else raise "Unknown structure type: #{ @underlying_structre }"
      end

      @size += 1 if new_pair_count == 1

      new_pair_count
    end
    alias []= set

    def get_all
      case @underlying_structure
      when List then get_all_list
      when Dict then get_all_dict
      else raise "Unknown structure type: #{ @underlying_structre }"
      end
    end

    def get(field)
      case @underlying_structure
      when List then get_list(field)
      when Dict then get_dict(field)
      else raise "Unknown structure type: #{ @underlying_structre }"
      end
    end
    alias [] get

    def delete(field)
      case @underlying_structure
      when List then was_deleted = delete_from_list(field)
      when Dict then
        was_deleted = !@underlying_structure.delete(field).nil?
        if was_deleted && @size - 1 == @max_list_size
          convert_dict_to_list
        elsif @underlying_structure.needs_resize?
          @underlying_structure.resize
        end
      else raise "Unknown structure type: #{ @underlying_structre }"
      end

      @size -= 1 if was_deleted

      was_deleted
    end

    private

    def set_list(key, value)
      iterator = List.left_to_right_iterator(@underlying_structure)
      while iterator.cursor && iterator.cursor.value.key != key
        iterator.next
      end

      if iterator.cursor.nil?
        @underlying_structure.right_push(ListEntry.new(key, value))

        1
      else
        iterator.cursor.value.value = value

        0
      end
    end

    def get_all_list
      iterator = List.left_to_right_iterator(@underlying_structure)
      pairs = []
      while iterator.cursor
        pairs.push(iterator.cursor.value.key, iterator.cursor.value.value)
        iterator.next
      end

      pairs
    end

    def get_all_dict
      pairs = []

      @underlying_structure.each do |key, value|
        pairs.push(key, value)
      end

      pairs
    end

    def get_list(field)
      iterator = List.left_to_right_iterator(@underlying_structure)

      while iterator.cursor
        return iterator.cursor.value.value if iterator.cursor.value.key == field

        iterator.next
      end
    end

    def get_dict(field)
      @underlying_structure[field]
    end

    def convert_list_to_dict
      dict = Dict.new
      iterator = List.left_to_right_iterator(@underlying_structure)

      while iterator.cursor
        dict[iterator.cursor.value.key] = iterator.cursor.value.value
        iterator.next
      end

      @underlying_structure = dict
    end

    def convert_dict_to_list
      list = List.new
      @underlying_structure.each do |key, value|
        list.right_push(ListEntry.new(key, value))
      end

      @underlying_structure = list
    end

    def delete_from_list(field)
      was_deleted = false
      iterator = List.left_to_right_iterator(@underlying_structure)

      while iterator.cursor
        if iterator.cursor.value.key == field
          @underlying_structure.remove_node(iterator.cursor)

          return true
        end

        iterator.next
      end

      was_deleted
    end

    def keys_list
      iterator = List.left_to_right_iterator(@underlying_structure)
      keys = []

      while iterator.cursor
        keys << iterator.cursor.value.key

        iterator.next
      end

      keys
    end

    def values_list
      iterator = List.left_to_right_iterator(@underlying_structure)
      values = []

      while iterator.cursor
        values << iterator.cursor.value.value

        iterator.next
      end

      values
    end
  end
end