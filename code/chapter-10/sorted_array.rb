require 'forwardable'

module BYORedis
  class SortedArray
    extend Forwardable

    def_delegators :@underlying, :[], :delete_if, :size, :each, :delete_at, :shift,
                   :bsearch_index, :map, :each_with_index, :pop, :empty?, :slice!

    def self.by_fields(*fields)
      SortedArray.new do |array_element, new_element|
        comparison = nil
        fields.each do |field|
          comparison = new_element.send(field) <=> array_element.send(field)
          # As long as the members are equal for field, we keep comparing
          if comparison == 0
            next
          else
            break
          end
        end

        comparison
      end
    end

    def initialize(&block)
      @underlying = []
      @block = block
    end

    def push(new_element)
      if @underlying.empty?
        index = 0
      else
        index = @underlying.bsearch_index do |element|
          @block.call(element, new_element) <= 0
        end
      end

      index = @underlying.size if index.nil?
      @underlying.insert(index, new_element)
    end
    alias << push

    def index(element)
      if @underlying.empty?
        nil
      else
        @underlying.bsearch_index do |existing_element|
          @block.call(existing_element, element)
        end
      end
    end

    def delete(element)
      index = index(element)
      return if index.nil?

      element_at_index = @underlying[index]
      first_index_to_delete = nil
      number_of_items_to_delete = 0
      while element_at_index
        if element_at_index == element
          first_index_to_delete ||= index
          number_of_items_to_delete += 1
        end

        index += 1
        next_element = @underlying[index]
        if next_element && @block.call(next_element, element_at_index) == 0
          element_at_index = next_element
        else
          break
        end
      end

      @underlying.slice!(first_index_to_delete, number_of_items_to_delete)
    end

    def first_index_in_range(range_spec)
      return nil if empty?

      @underlying.bsearch_index do |existing_element|
        compare = range_spec.compare_with_min(yield(existing_element))
        if range_spec.min_exclusive?
          compare > 0 # existing_element.score > min
        else
          compare >= 0 # existing_element.score >= min
        end
      end
    end

    def last_index_in_range(range_spec)
      return nil if empty?

      first_index_outside = @underlying.bsearch_index do |existing_element|
        compare = range_spec.compare_with_max(yield(existing_element))
        if range_spec.max_exclusive?
          compare >= 0 # existing_element.score > max
        else
          compare > 0 # existing_element.score >= max
        end
      end

      case first_index_outside
      when nil then @underlying.size - 1 # last
      when 0 then nil # the max of the range is smaller than the smallest item
      else first_index_outside - 1
      end
    end
  end
end
