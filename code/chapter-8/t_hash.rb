module BYORedis
  class THash

    MAX_LIST_SIZE = 2 # 256 for real though

    ListEntry = Struct.new(:key, :value)

    def initialize
      @underlying_structure = List.new
      @size = 0
    end

    def empty?
      @size == 0
    end

    def set(key, value)
      if @size <= MAX_LIST_SIZE
        new_pair_count = set_list(key, value)
        if new_pair_count + @size > MAX_LIST_SIZE
          convert_list_to_dict
        end
      else
        existing_pair_count = @underlying_structure.used
        @underlying_structure[key] = value
        new_pair_count = @underlying_structure.used - existing_pair_count
      end

      @size += 1 if new_pair_count == 1

      p @size
      p new_pair_count

      new_pair_count
    end
    alias []= set

    def get_all
      if @size <= MAX_LIST_SIZE
        get_all_list
      else
        get_all_dict
      end
    end

    def get(field)
      if @size <= MAX_LIST_SIZE
      else
      end
    end

    def delete(field)
      # Try to convert back to list
      if @size <= MAX_LIST_SIZE
        was_deleted = delete_from_list(field)
      else
        was_deleted = !@underlying_structure.delete(field).nil?
        if was_deleted && @size - 1 == MAX_LIST_SIZE
          convert_dict_to_list
        end
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

    def convert_list_to_dict
      puts 'L2D'
      dict = Dict.new
      iterator = List.left_to_right_iterator(@underlying_structure)

      while iterator.cursor
        dict[iterator.cursor.value.key] = iterator.cursor.value.value
        iterator.next
      end

      @underlying_structure = dict
    end

    def convert_dict_to_list
      puts "D2L"
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
  end
end
