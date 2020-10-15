module BYORedis
  class IntSet

    def initialize
      @underlying_array = []
    end

    def add(member)
      raise "Member is not an int: #{ member }" unless member.is_a?(Integer)

      index = @underlying_array.bsearch_index { |x| x >= member }

      if index.nil?
        @underlying_array.append(member)

        true
      elsif @underlying_array[index] == member
        false
      else
        @underlying_array.insert(index, member)

        true
      end
    end

    def cardinality
      @underlying_array.size
    end
    alias card cardinality

    def each(&block)
      @underlying_array.each(&block)
    end

    def contains?(member)
      return false if member.nil?

      p "Contains for #{ member.inspect } in #{ @underlying_array.inspect }"
      @underlying_array.bsearch { |x| x >= member } == member
    end

    def members
      @underlying_array
    end
  end
end
