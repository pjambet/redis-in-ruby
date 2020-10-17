require_relative './int_set'
require_relative './dict'

module BYORedis
  class RedisSet

    # How many times bigger should be the set compared to the requested size
    # for us to don't use the "remove elements" strategy? Read later in the
    # implementation for more info.
    # See: https://github.com/antirez/redis/blob/6.0.0/src/t_set.c#L609-L612
    SRANDMEMBER_SUB_STRATEGY_MUL = 3

    attr_reader :cardinality

    def initialize
      @max_list_size = ENV['SET_MAX_ZIPLIST_ENTRIES'].to_i.then do |max|
        max <= 0 ? 256 : max
      end
      @underlying_structure = IntSet.new
      @cardinality = 0
    end

    def add(member)
      if @underlying_structure.is_a?(IntSet)

        if int_member = can_be_represented_as_int?(member)
          added = @underlying_structure.add(int_member)

          if added && @cardinality + 1 > @max_list_size
            p "CONVERTING BECAUSE SIZE"
            convert_intset_to_dict
          end
        else
          p "CONVERTING BECAUSE STRING"
          convert_intset_to_dict
          added = add_to_dict_if_needed(member)
        end
      elsif @underlying_structure.is_a?(Dict)
        added = add_to_dict_if_needed(member)
      else
        raise "Unknown type for structure: #{ @underlying_structure }"
      end

      @cardinality += 1 if added

      added
    end

    def diff(other_sets)
      return self if other_sets.empty?

      dest_set = RedisSet.new
      # TODO
      # Sort other_sets?

      # algo 1
      each do |element|
        i = 0
        while i < other_sets.length
          other_set = other_sets[i]
          # There's nothing to do when one of the sets does not exist
          next if other_set.nil?
          # If the other set contains the element then we know we don't want to add element to
          # the diff set
          break if other_set == self

          if other_set.contains?(element)
            p "#{ other_set.inspect } does contain #{ element }"
            break
          else
            p "#{ other_set.inspect } does not contain #{ element }"
          end
          i += 1
        end

        if i == (other_sets.length)
          dest_set.add(element)
        end
      end

      dest_set
    end

    def members
      case @underlying_structure
      when IntSet then @underlying_structure.members.map(&:to_s)
      when Dict then @underlying_structure.keys
      else raise "Unknown type for structure #{ @underlying_structure }"
      end
    end

    def pop(count)
      return [] if count == 0

      case @underlying_structure
      when IntSet then
        popped = @underlying_structure.pop.to_s
        @cardinality -= 1
        popped
      when Dict then
        random_entry = @underlying_structure.random_entry
        p random_entry
        @underlying_structure.delete(random_entry.key)
        @cardinality -= 1
        random_entry.key
      else raise "Unknown type for structure #{ @underlying_structure }"
      end
    end

    def random_members_with_count(count)
      return [] if count.nil? || count == 0

      # Case 1: Count is negative, we return that many elements, ignoring duplicates
      if count < 0
        members = []
        (-count).times do
          members << random_member
        end

        return members
      end

      # Case 2: Count is positive and greater than the size, we return the whole thing
      return self if count >= @cardinality

      # For both case 3 & 4 we need a new set
      new_set = Dict.new
      # Case 3: Number of elements in the set is too small to grab n random distinct members
      # from it so we instead pick random elements to remove from it
      # Start by creating a new set identical to self and then remove elements from it
      p "Count: #{ count }"
      if count * SRANDMEMBER_SUB_STRATEGY_MUL > @cardinality
        size = @cardinality
        each { |member| new_set.add(member, nil) }
        while size > count
          random_entry = new_set.random_entry
          new_set.delete(random_entry.key)
          size -= 1
        end
        return new_set.keys
      end

      # Case 4: The number of elements in the set is big enough in comparison to count so we
      # do the "classic" approach of picking count distinct elements
      added = 0
      while added < count
        member = self.random_member
        p "Trying to add #{ member } for set of size #{ new_set.used } / #{added}"
        res = new_set.add(member, nil)
        p "Res: #{res}"
        if res
          added += 1
        end
      end
      return new_set.keys


      # case @underlying_structure
      # when IntSet then Utils.integer_to_string(@underlying_structure.random_member)
      # when Dict then
        # random_entry = @underlying_structure.random_entry
        # random_entry.key
      # else raise "Unknown type for structure #{ @underlying_structure }"
      # end
    end

    def random_member
      case @underlying_structure
      when IntSet then Utils.integer_to_string(@underlying_structure.random_member)
      when Dict then
        random_entry = @underlying_structure.random_entry
        unless random_entry.next.nil?
          until random_entry.next.nil?
            if rand(2) == 0
              break
            else
              random_entry = random_entry.next
            end
          end
        end
        p '==='
        p random_entry
        random_entry.key
      else raise "Unknown type for structure #{ @underlying_structure }"
      end
    end

    def empty?
      case @underlying_structure
      when IntSet then @underlying_structure.empty?
      when Dict then @underlying_structure.used == 0
      else raise "Unknown type for structure #{ @underlying_structure }"
      end
    end

    def contains?(member)
      return false if member.nil?

      case @underlying_structure
      when IntSet then
        p "Weird intset check for #{ member.inspect }"
        if member.is_a?(Integer)
          member_as_int = member
        else
          member_as_int = Utils.string_to_integer_or_nil(member)
        end

        if member_as_int
          rest = @underlying_structure.contains?(member_as_int)
          p rest
          rest
        else
          false
        end
      when Dict then @underlying_structure.include?(member)
      else raise "Unknown type for structure #{ @underlying_structure }"
      end
    end

    def each(&block)
      case @underlying_structure
      when IntSet then @underlying_structure.each { |i| block.call(Utils.integer_to_string(i)) }
      when Dict then @underlying_structure.each(&block)
      else raise "Unknown type for structure #{ @underlying_structure }"
      end
    end

    private

    def add_to_dict_if_needed(member)
      present = @underlying_structure.include?(member)
      if present
        added = false
      else
        added = true
        @underlying_structure.add(member, nil)
      end

      added
    end

    def convert_intset_to_dict
      dict = Dict.new
      @underlying_structure.each do |member|
        dict[Utils.integer_to_string(member)] = nil
      end

      p "Conversion done#: #{ dict.inspect } from #{ @underlying_structure.inspect }"

      @underlying_structure = dict
    end

    def can_be_represented_as_int?(member)
      Utils.string_to_integer(member)
    rescue InvalidIntegerString
      false
    end
  end

  class SetSerializer

    def initialize(set)
      @set = set
    end

    def serialize
      response = ''
      @set.each do |member|
        response << "$#{ member.size }\r\n#{ member }\r\n"
      end

      response.prepend("*#{ @set.cardinality }\r\n")

      response
    end
  end
end
