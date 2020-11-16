module BYORedis
  class SortedArray
    def initialize(field)
      @underlying = []
      @field = field.to_sym
    end

    def push(new_element)
      if @underlying.empty?
        index = 0
      else
        index = @underlying.bsearch_index do |element|
          element.send(@field) >= new_element.send(@field)
        end
      end

      index = @underlying.size if index.nil?
      @underlying.insert(index, new_element)
    end
    alias << push

    def [](index)
      @underlying[index]
    end

    def size
      @underlying.size
    end

    def shift
      @underlying.shift
    end

    def delete_if(&block)
      @underlying.delete_if(&block)
    end

    def delete(element)
      index = @underlying.bsearch_index { |x| x.send(@field) >= element.send(@field) }
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
        if next_element && next_element.send(@field) == element.send(@field)
          element_at_index = next_element
        else
          break
        end
      end

      @underlying.slice!(first_index_to_delete, number_of_items_to_delete)
    end
  end
end
