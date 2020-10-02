# coding: utf-8

require_relative './test_helper'
require_relative '../list'

describe BYORedis::List do
  describe 'left_pop' do
    it 'returns nil with an empty list' do
      list = new_list

      assert_nil(list.left_pop)
    end

    it 'removes the element at the head of list' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      head = list.left_pop
      assert_equal(1, head.value)
      assert_equal(2, list.head.value)
      assert_nil(list.head.prev_node)

      head = list.left_pop
      assert_equal(2, head.value)
      assert_equal(3, list.head.value)
      assert_nil(list.head.prev_node)

      head = list.left_pop
      assert_equal(3, head.value)

      assert_nil(list.head)
      assert_nil(list.tail)
    end
  end

  describe 'left_push' do
    it 'adds a new element that becomes the head of the list' do
      list = new_list

      (1..3).each { |i| list.left_push(i) }

      assert_has_elements(list, 3, 2, 1)
      head = list.head
      assert_nil(head.prev_node)
      assert_equal(2, head.next_node.value)

      second_node = list.head.next_node
      assert_equal(3, second_node.prev_node.value)
      assert_equal(1, second_node.next_node.value)
      assert_equal(list.tail, second_node.next_node)

      tail = list.head.next_node.next_node
      assert_nil(tail.next_node)
      assert_equal(2, tail.prev_node.value)
    end
  end

  describe 'right_pop' do
    it 'returns nil with an empty list' do
      list = new_list

      assert_nil(list.right_pop)
    end

    it 'removes the element as the tail of the list' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      tail = list.right_pop
      assert_equal(3, tail.value)
      assert_equal(2, list.tail.value)
      assert_nil(list.tail.next_node)

      tail = list.right_pop
      assert_equal(2, tail.value)
      assert_equal(1, list.tail.value)
      assert_nil(list.tail.next_node)

      tail = list.right_pop
      assert_equal(1, tail.value)

      assert_nil(list.head)
      assert_nil(list.tail)
    end
  end

  describe 'right_push' do
    it 'adds a new element that becomes the new tail of the list' do
      list = new_list

      (1..3).each { |i| list.right_push(i) }

      assert_has_elements(list, 1, 2, 3)
      assert_equal(list.head.value, 1)
      assert_equal(list.tail.value, 3)

      head = list.head
      assert_nil(head.prev_node)
      assert_equal(2, head.next_node.value)

      second_node = list.head.next_node
      assert_equal(1, second_node.prev_node.value)
      assert_equal(3, second_node.next_node.value)
      assert_equal(list.tail, second_node.next_node)

      tail = list.head.next_node.next_node
      assert_nil(tail.next_node)
      assert_equal(2, tail.prev_node.value)
    end
  end

  describe 'set' do
    it 'updates the value at the given index' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      list.set(1, 'foo')

      assert_has_elements(list, 1, 'foo', 3)
    end

    it 'updates the value starting from the right at the given negative' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      list.set(-2, 'foo')

      assert_has_elements(list, 1, 'foo', 3)
    end
  end

  describe 'remove' do
    it 'removes n nodes matching element' do
      list = new_list
      (1..5).each { |i| list.right_push(i) }
      list.right_push(1)
      list.right_push(2)
      list.right_push(1)
      list.right_push(2) # List is 1,2,3,4,5,1,2,1,2

      assert_equal(0, list.remove(1, 10))

      assert_equal(1, list.remove(1, 2))
      assert_has_elements(list, 1, 3, 4, 5, 1, 2, 1, 2)

      assert_equal(2, list.remove(2, 2))
      assert_has_elements(list, 1, 3, 4, 5, 1, 1)

      assert_equal(3, list.remove(10, 1))
      assert_has_elements(list, 3, 4, 5)
      assert_equal(3, list.head.value)
      assert_nil(list.head.prev_node)
      assert_equal(5, list.tail.value)
      assert_nil(list.tail.next_node)
    end

    it 'removes n nodes starting from the right with a negative count' do
      list = new_list
      (1..5).each { |i| list.right_push(i) }
      list.right_push(1)
      list.right_push(2)
      list.right_push(1)
      list.right_push(2) # list is 1,2,3,4,5,1,2,1,2

      assert_equal(0, list.remove(-1, 10))

      assert_equal(1, list.remove(-1, 2))
      assert_has_elements(list, 1, 2, 3, 4, 5, 1, 2, 1)

      assert_equal(2, list.remove(-2, 2))
      assert_has_elements(list, 1, 3, 4, 5, 1, 1)

      assert_equal(2, list.remove(-2, 1))
      assert_has_elements(list, 1, 3, 4, 5)
      assert_equal(1, list.head.value)
      assert_nil(list.head.prev_node)
      assert_equal(5, list.tail.value)
      assert_nil(list.tail.next_node)
    end

    it 'removes all nodes matching element with count set to 0' do
      list = new_list
      (1..5).each { |i| list.right_push(i) }
      list.right_push(1)
      list.right_push(2)
      list.right_push(1)
      list.right_push(2) # list is 1,2,3,4,5,1,2,1,2

      assert_equal(3, list.remove(0, 2))
      assert_has_elements(list, 1, 3, 4, 5, 1, 1)

      assert_equal(3, list.remove(0, 1))
      assert_has_elements(list, 3, 4, 5)
    end
  end

  describe 'position' do
    describe 'without any options' do
      it 'returns nil if no nodes match element' do
        list = new_list
        (1..5).each { |i| list.right_push(i) }

        assert_nil(list.position(10, nil, nil, nil))
      end

      it 'returns the index of the first node matching element' do
        list = new_list
        (1..5).each { |i| list.right_push(i) }

        assert_equal(0, list.position(1, nil, nil, nil))
        assert_equal(1, list.position(2, nil, nil, nil))
        assert_equal(2, list.position(3, nil, nil, nil))
        assert_equal(3, list.position(4, nil, nil, nil))
        assert_equal(4, list.position(5, nil, nil, nil))
      end
    end

    describe 'with a count' do
      it 'returns an empty array if no nodes match element' do
        list = new_list
        (1..5).each { |i| list.right_push(i) }

        assert_equal([], list.position(10, 1, nil, nil))
      end

      it 'returns an array of up to n indexes for all matches' do
        list = new_list
        (1..5).each { |i| list.right_push(i) }
        3.times { list.right_push(1) } # list is now 1,2,3,4,5,1,1,1

        assert_equal([ 0 ], list.position(1, 1, nil, nil))
        assert_equal([ 0, 5 ], list.position(1, 2, nil, nil))
        assert_equal([ 0, 5, 6 ], list.position(1, 3, nil, nil))
        assert_equal([ 0, 5, 6, 7 ], list.position(1, 4, nil, nil))
        assert_equal([ 0, 5, 6, 7 ], list.position(1, 5, nil, nil))
      end

      it 'returns nil with a negative count' do
        list = new_list

        assert_nil(list.position(1, -1, nil, nil))
      end
    end

    describe 'with rank' do
      it 'skips n elements from the left with a positive rank' do
        list = new_list
        (1..5).each { |i| list.right_push(i) }
        3.times { list.right_push(1) } # list is now 1,2,3,4,5,1,1,1

        assert_equal(0, list.position(1, nil, nil, 1))
        assert_equal(5, list.position(1, nil, nil, 2))
        assert_equal(6, list.position(1, nil, nil, 3))
        assert_equal(7, list.position(1, nil, nil, 4))
        assert_nil(list.position(1, nil, nil, 5))

        assert_equal([ 0 ], list.position(1, 1, nil, 1))
        assert_equal([ 5 ], list.position(1, 1, nil, 2))
        assert_equal([ 6 ], list.position(1, 1, nil, 3))
        assert_equal([ 7 ], list.position(1, 1, nil, 4))
        assert_equal([], list.position(1, 1, nil, 5))

        assert_equal([ 6, 7 ], list.position(1, 3, nil, 3))
      end

      it 'skips n elements from the right with a negative rank' do
        list = new_list
        (1..5).each { |i| list.right_push(i) }
        3.times { list.right_push(1) } # list is now 1,2,3,4,5,1,1,1

        assert_equal([ 7 ], list.position(1, 1, nil, -1))
        assert_equal([ 7, 6 ], list.position(1, 2, nil, -1))
        assert_equal([ 7, 6, 5 ], list.position(1, 3, nil, -1))
        assert_equal([ 7, 6, 5, 0 ], list.position(1, 4, nil, -1))
        assert_equal([ 7, 6, 5, 0 ], list.position(1, 5, nil, -1))
      end
    end

    describe 'with maxlen' do
      it 'returns nil with a negative maxlen'do
        list = new_list

        assert_nil(list.position(10, 0, -1, 0))
      end

      it 'only inspects maxlen nodes from the left' do
        list = new_list
        (1..5).each { |i| list.right_push(i) }
        3.times { list.right_push(1) } # list is now 1,2,3,4,5,1,1,1

        assert_equal(0, list.position(1, nil, 1, nil))
        assert_equal([ 0 ], list.position(1, 3, 1, nil))
        assert_equal([ 0, 5 ], list.position(1, 3, 6, nil))
      end

      it 'only inspects maxlen nodes from the right with a negative rank' do
        list = new_list
        (1..5).each { |i| list.right_push(i) }
        3.times { list.right_push(1) } # list is now 1,2,3,4,5,1,1,1

        assert_equal(7, list.position(1, nil, 1, -1))
        assert_equal([ 7 ], list.position(1, 3, 1, -1))
        assert_equal([ 7, 6, 5 ], list.position(1, 3, 6, -1))
      end

      it 'inspects the whole list with maxlen set to 0' do
        list = new_list
        (1..5).each { |i| list.right_push(i) }
        3.times { list.right_push(1) } # list is now 1,2,3,4,5,1,1,1

        assert_equal([ 0, 5, 6, 7 ], list.position(1, 0, 0, nil))
      end
    end
  end

  describe 'insert_before' do
    it 'does nothing if no nodes match pivot' do
      list = new_list

      assert_equal(-1, list.insert_before(1, 1))
    end

    it 'insert a new node before the first node matching pivot' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      assert_equal(4, list.insert_before(3, 'new-node'))
      assert_has_elements(list, 1, 2, 'new-node', 3)
      assert_equal(5, list.insert_before(1, 'new-head'))
      assert_has_elements(list, 'new-head', 1, 2, 'new-node', 3)
      assert_nil(list.head.prev_node)
    end
  end

  describe 'insert_after' do
    it 'does nothing if no nodes match pivot' do
      list = new_list

      assert_equal(-1, list.insert_after(1, 1))
    end

    it 'insert a new node after the first node matching pivot' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      assert_equal(4, list.insert_after(2, 'new-node'))
      assert_has_elements(list, 1, 2, 'new-node', 3)
      assert_equal(5, list.insert_after(3, 'new-tail'))
      assert_has_elements(list, 1, 2, 'new-node', 3, 'new-tail')
      assert_nil(list.tail.next_node)
    end
  end

  describe 'at_index' do
    it 'returns nil if index is out of bound' do
      list = new_list

      assert_nil(list.at_index(0))
      assert_nil(list.at_index(1))
    end

    it 'returns the value of the element at the given index' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      assert_equal(1, list.at_index(0))
      assert_equal(2, list.at_index(1))
      assert_equal(3, list.at_index(2))
    end

    it 'returns the value of the element starting from the right at the given negative index' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      assert_equal(3, list.at_index(-1))
      assert_equal(2, list.at_index(-2))
      assert_equal(1, list.at_index(-3))
      assert_nil(list.at_index(-4))
    end
  end

  describe 'trim' do
    it 'is a no-op with 0 -1' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      list.trim(0, -1)

      assert_has_elements(list, 1, 2, 3)
      assert_equal(list.head.value, 1)
      assert_equal(list.tail.value, 3)
    end

    it 'removes elements at the beginning of the list' do
      list = new_list
      (1..5).each { |i| list.right_push(i) }

      list.trim(2, -1)

      assert_has_elements(list, 3, 4, 5)
      assert_equal(list.head.value, 3)
      assert_equal(list.tail.value, 5)
    end

    it 'removes elements at the end of the list' do
      list = new_list
      (1..5).each { |i| list.right_push(i) }

      list.trim(0, 2)

      assert_has_elements(list, 1, 2, 3)
      assert_equal(list.head.value, 1)
      assert_equal(list.tail.value, 3)
    end

    it 'empties the list if start is greater than the list size' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      list.trim(10, 10)

      assert_equal(0, list.size)
      assert_nil(list.head)
      assert_nil(list.tail)
    end

    it 'empties the list if start > stop' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      list.trim(2, 0)

      assert_equal(0, list.size)
      assert_nil(list.head)
      assert_nil(list.tail)
    end

    it 'handles an out of bound start index' do
      list = new_list
      (1..10).each { |i| list.right_push(i) }

      list.trim(-10, 2)

      assert_has_elements(list, 1, 2, 3)
      assert_equal(list.head.value, 1)
      assert_equal(list.tail.value, 3)
    end

    it 'handles an out of bound stop index' do
      list = new_list
      (1..10).each { |i| list.right_push(i) }

      list.trim(8, 100)

      assert_has_elements(list, 9, 10)
      assert_equal(list.head.value, 9)
      assert_equal(list.tail.value, 10)
    end

    it 'handles a negative index for start within the boundaries of the list' do
      list = new_list
      (1..10).each { |i| list.right_push(i) }

      list.trim(-2, 9)

      assert_has_elements(list, 9, 10)
      assert_equal(list.head.value, 9)
      assert_equal(list.tail.value, 10)
    end

    it 'handles a negative index for start outside the boundaries of the list' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      list.trim(-20, 1)

      assert_has_elements(list, 1, 2)
      assert_equal(list.head.value, 1)
      assert_equal(list.tail.value, 2)
    end

    it 'handles a negative index for stop within the boundaries of the list' do
      list = new_list
      (1..10).each { |i| list.right_push(i) }

      list.trim(0, -8)

      assert_has_elements(list, 1, 2, 3)
      assert_equal(list.head.value, 1)
      assert_equal(list.tail.value, 3)
    end

    it 'handles a negative index for stop outside the boundaries of the list' do
      list = new_list
      (1..3).each { |i| list.right_push(i) }

      list.trim(1, 100)

      assert_has_elements(list, 2, 3)
      assert_equal(list.head.value, 2)
      assert_equal(list.tail.value, 3)
    end

    it 'handles a 0 99,999 trim for a 100,001 element list with a single operation' do
      list = new_list
      (1..100_001).each { |i| list.right_push(i) }

      t0 = Time.now
      list.trim(0, 99_999)
      t1 = Time.now
      duration = t1 - t0

      # Assert that it was faster that 0.05 ms
      # A non-optimized version that has to iterate through the whole list would take a few ms
      # between 5ms and 7ms on my 2013 mbp
      assert_operator 0.000050, :>=, duration
      assert_equal(100_000, list.size)
    end

    it 'handles a 99,998 99,999 trim for a 100,001 element list with a single operation' do
      list = new_list
      (1..100_001).each { |i| list.right_push(i) }

      t0 = Time.now
      list.trim(99_998, 99_999)
      t1 = Time.now
      duration = t1 - t0

      # Assert that it was faster that 0.05 ms
      # A non-optimized version that has to iterate through the whole list would take a few ms
      # between 5ms and 7ms on my 2013 mbp
      assert_operator 0.000070, :>=, duration
      assert_equal(2, list.size)
    end

    it 'something' do
      list = new_list
      list.right_push('b')
      list.right_push('c')
      list.right_push('d')
      # (1..100_001).each { |i| list.right_push(i) }

      # t0 = Time.now
      list.trim(2, 22)
      # t1 = Time.now
      # duration = t1 - t0

      # Assert that it was faster that 0.05 ms
      # A non-optimized version that has to iterate through the whole list would take a few ms
      # between 5ms and 7ms on my 2013 mbp
      # assert_operator 0.000050, :>=, duration
      assert_equal(1, list.size)
      assert_equal(list.head.value, 'd')
      assert_equal(list.tail.value, 'd')
    end
  end

  def new_list
    BYORedis::List.new
  end

  def assert_has_elements(list, *elements)
    cursor = list.head

    assert_equal(elements.size, list.size)

    while cursor
      element = elements.shift
      assert_equal(element, cursor.value)

      cursor = cursor.next_node
    end
  end
end
