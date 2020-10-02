module BYORedis

  class List

    ListNode = Struct.new(:value, :prev_node, :next_node)
    Iterator = Struct.new(:cursor, :index, :cursor_iterator, :index_iterator) do
      def next
        self.cursor = cursor_iterator.call(cursor)
        self.index = index_iterator.call(index)
      end
    end

    attr_accessor :head, :tail, :size

    def initialize
      @head = nil
      @tail = nil
      @size = 0
    end

    def self.left_to_right_iterator(list)
      # cursor, start_index, iterator, index_iterator
      Iterator.new(list.head, 0, ->(node) { node.next_node }, ->(i) { i + 1 })
    end

    def self.right_to_left_iterator(list)
      # cursor, start_index, iterator, index_iterator
      Iterator.new(list.tail, list.size - 1, ->(node) { node.prev_node }, ->(i) { i - 1 })
    end

    def empty?
      @size == 0
    end

    def left_push(value)
      new_node = ListNode.new(value, nil, @head)

      if @head.nil?
        @tail = new_node
      else
        @head.prev_node = new_node
      end

      @head = new_node
      @size += 1
    end

    def right_push(value)
      new_node = ListNode.new(value, @tail, nil)

      if @head.nil?
        @head = new_node
      else
        @tail.next_node = new_node
      end

      @tail = new_node
      @size += 1
    end

    def left_pop
      return nil if @size == 0

      old_head = @head
      @size -= 1
      @head = @head.next_node
      @head.prev_node = nil if @head
      @tail = nil if @size == 0

      old_head
    end

    def right_pop
      return nil if @size == 0

      old_tail = @tail
      @size -= 1
      @tail = @tail.prev_node
      @tail.next_node = nil if @tail
      @head = nil if @size == 0

      old_tail
    end

    def trim(start, stop)
      current_head = @head

      # Convert negative values
      stop = @size + stop if stop < 0
      stop = @size - 1 if stop >= @size
      start = @size + start if start < 0
      start = 0 if start < 0

      if start >= @size || start > stop
        @size = 0
        @head = nil
        @tail = nil
        return
      end

      return if start == 0 && stop == @size - 1

      distance_to_start = start
      distance_to_stop = @size - stop - 1

      if distance_to_start <= distance_to_stop
        iterator = List.left_to_right_iterator(self)
        target_index = start
      else
        iterator = List.right_to_left_iterator(self)
        target_index = stop
      end

      new_head = nil
      new_tail = nil

      while iterator.index != target_index
        iterator.next
      end

      # We reached the closest element, either start or stop
      # We first update either the head and the nail and then find the fastest way to get to the
      # other boundary
      if target_index == start
        new_head = iterator.cursor
        target_index = stop
        # We reached start, decide if we should keep going right from where we are or start from
        # the tail to reach stop
        if distance_to_stop < stop - iterator.index
          iterator = List.right_to_left_iterator(self)
        end
      else
        new_tail = iterator.cursor
        target_index = start
        # We reached stop, decide if we should keep going left from where we are or start from
        # the head to reach start
        if distance_to_start < iterator.index - start
          iterator = List.left_to_right_iterator(self)
        end
      end

      while iterator.index != target_index
        iterator.next
      end

      # We now reached the other boundary
      if target_index == start
        new_head = iterator.cursor
      else
        new_tail = iterator.cursor
      end

      @head = new_head
      @head.prev_node = nil

      # If start == stop, then there's only element left, and new_tail will not have been set
      # above, so we set here
      if start == stop
        new_tail = new_head
        @size = 1
      else
        # Account for the elements dropped to the right
        @size -= (@size - stop - 1)
        # Account for the elements dropped to the left
        @size -= start
      end

      @tail = new_tail
      @tail.next_node = nil
    end

    def set(index, new_value)
      # Convert a negative index
      index += @size if index < 0

      return if index < 0 || index >= @size

      distance_from_head = index
      distance_from_tail = @size - index - 1

      if distance_from_head <= distance_from_tail
        iterator = List.left_to_right_iterator(self)
      else
        iterator = List.right_to_left_iterator(self)
      end

      while iterator.index != index
        iterator.next
      end

      iterator.cursor.value = new_value
    end

    def remove(count, element)
      delete_count = 0
      if count >= 0
        iterator = List.left_to_right_iterator(self)
      else
        iterator = List.right_to_left_iterator(self)
      end

      while iterator.cursor
        cursor = iterator.cursor
        if cursor.value == element
          if @head == cursor
            @head = cursor.next_node
          else
            cursor.prev_node.next_node = cursor.next_node
          end

          if @tail == cursor
            @tail = cursor.prev_node
          else
            cursor.next_node.prev_node = cursor.prev_node
          end

          delete_count += 1
          @size -= 1

          if count != 0 && (delete_count == count || delete_count == (count * -1))
            break
          end
        end

        iterator.next
      end

      delete_count
    end

    def position(element, count, maxlen, rank)
      return if count && count < 0
      return if @size == 0
      return if rank && rank == 0

      match_count = 0
      maxlen = @size if maxlen == 0 || maxlen.nil?
      indexes = [] if count

      if rank.nil? || rank >= 0
        iterator = List.left_to_right_iterator(self)
      else
        iterator = List.right_to_left_iterator(self)
      end

      while iterator.cursor
        if (rank.nil? || rank >= 0) && iterator.index >= maxlen
          break
        elsif (rank && rank < 0) && (@size - iterator.index - 1) >= maxlen
          break
        end

        if element == iterator.cursor.value
          match_count += 1

          if rank
            reached_rank_from_head = rank > 0 && match_count >= rank
            reached_rank_from_tail = rank < 0 && match_count >= (rank * -1)
          end

          if rank.nil? || reached_rank_from_head || reached_rank_from_tail
            return iterator.index if indexes.nil?

            indexes << iterator.index
          end

          return indexes if indexes && indexes.size == count
        end

        iterator.next
      end

      indexes
    end

    def insert_before(pivot, element)
      generic_insert(pivot) do |node|
        new_node = ListNode.new(element, node.prev_node, node)
        if @head == node
          @head = new_node
        else
          node.prev_node.next_node = new_node
        end

        node.prev_node = new_node
      end
    end

    def insert_after(pivot, element)
      generic_insert(pivot) do |node|
        new_node = ListNode.new(element, node, node.next_node)
        if @tail == node
          @tail = new_node
        else
          node.next_node.prev_node = new_node
        end

        node.next_node = new_node
      end
    end

    def at_index(index)
      index += @size if index < 0
      return if index >= @size || index < 0

      distance_to_head = index
      distance_to_tail = @size - index

      if distance_to_head <= distance_to_tail
        iterator = List.left_to_right_iterator(self)
      else
        iterator = List.right_to_left_iterator(self)
      end

      while iterator.cursor
        return iterator.cursor.value if iterator.index == index

        iterator.next
      end
    end

    private

    def generic_insert(pivot)
      cursor = @head

      while cursor
        break if cursor.value == pivot

        cursor = cursor.next_node
      end

      if cursor.nil?
        -1
      else
        @size += 1

        yield cursor

        @size
      end
    end
  end

  class ListSerializer

    def initialize(list, start, stop)
      @list = list
      @start = start
      @stop = stop
    end

    def serialize
      @stop = @list.size + @stop if @stop < 0
      @start = @list.size + @start if @start < 0

      @stop = @list.size - 1 if @stop >= @list.size
      @start = 0 if @start < 0

      return EmptyArrayInstance.serialize if @start > @stop

      response = ''
      size = 0
      distance_to_head = @start
      distance_to_tail = @list.size - @stop

      if distance_to_head <= distance_to_tail
        iterator = List.left_to_right_iterator(@list)
        within_bounds = ->(index) { index >= @start }
        stop_condition = ->(index) { index > @stop }
        accumulator = ->(value) { response << RESPBulkString.new(value).serialize }
      else
        iterator = List.right_to_left_iterator(@list)
        within_bounds = ->(index) { index <= @stop }
        stop_condition = ->(index) { index < @start }
        accumulator = ->(value) { response.prepend(RESPBulkString.new(value).serialize) }
      end

      until stop_condition.call(iterator.index)
        if within_bounds.call(iterator.index)
          accumulator.call(iterator.cursor.value)
          size += 1
        end

        iterator.next
      end

      response.prepend("*#{ size }\r\n")
    end
  end
end
