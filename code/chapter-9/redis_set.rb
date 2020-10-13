require_relative './int_set'
require_relative './dict'

module BYORedis
  class RedisSet

    # How many times bigger should be the set compared to the requested size
    # for us to don't use the "remove elements" strategy? Read later in the
    # implementation for more info.
    # See: https://github.com/antirez/redis/blob/6.0.0/src/t_set.c#L609-L612
    SRANDMEMBER_SUB_STRATEGY_MUL = 3

    # How many times bigger should be the set compared to the remaining size
    # for us to use the "create new set" strategy? Read later in the
    # implementation for more info.
    # See: https://github.com/antirez/redis/blob/6.0.0/src/t_set.c#L413-416
    SPOP_MOVE_STRATEGY_MUL = 5

    def initialize
      @underlying = IntSet.new
    end

    def self.intersection(sets)
      # Sort the sets smallest to largest
      sets.sort_by!(&:cardinality)

      intersection_set = RedisSet.new
      # Iterate over the first set, if we find a set that does not contain it, discard

      sets[0].each do |member|
        present_in_all_other_sets = true
        sets[1..-1].each do |set|
          unless set.member?(member)
            present_in_all_other_sets = false
            break
          end
        end
        # Otherwise, keep
        intersection_set.add(member) if present_in_all_other_sets
      end

      intersection_set
    end

    def self.union(sets)
      if sets.empty?
        RedisSet.new
      else
        union_set = RedisSet.new
        sets.each do |set|
          set&.each { |member| union_set.add(member) }
        end

        union_set
      end
    end

    def self.difference(sets)
      first_set = sets[0]
      return RedisSet.new if first_set.nil?

      # Decide which algorithm to use
      algo_one_work = 0
      algo_two_work = 0
      sets.each do |other_set|
        algo_one_work += sets[0].cardinality
        algo_two_work += other_set ? other_set.cardinality : 0
      end
      # Directly from Redis:
      # Algorithm 1 has better constant times and performs less operations
      # if there are elements in common. Give it some advantage:
      algo_one_work /= 2
      diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2

      if diff_algo == 1
        if sets.length > 1
          sets[0..0] + sets[1..-1].sort_by! { |s| -1 * s.cardinality }
        end
        difference_algorithm1(sets)
      else
        difference_algorithm2(sets)
      end
    end

    def self.difference_algorithm1(sets)
      return RedisSet.new if sets.empty? || sets[0].nil?

      dest_set = RedisSet.new

      sets[0].each do |element|
        i = 0
        other_sets = sets[1..-1]
        while i < other_sets.length
          other_set = other_sets[i]
          # There's nothing to do when one of the sets does not exist
          next if other_set.nil?
          # If the other set contains the element then we know we don't want to add element to
          # the diff set
          break if other_set == self

          break if other_set.member?(element)

          i += 1
        end

        if i == other_sets.length
          dest_set.add(element)
        end
      end

      dest_set
    end
    private_class_method :difference_algorithm1

    def self.difference_algorithm2(sets)
      return self if sets.empty? || sets[0].nil?

      dest_set = RedisSet.new

      # Add all the elements from the first set to the new one
      sets[0].each do |element|
        dest_set.add(element)
      end

      # Iterate over all the other sets and remove them from the first one
      sets[1..-1].each do |set|
        set.each do |member|
          dest_set.remove(member)
        end
      end

      dest_set
    end
    private_class_method :difference_algorithm2

    def add(member)
      case @underlying
      when IntSet
        int_member = convert_to_int_or_nil(member)
        if int_member
          added = @underlying.add(int_member)

          if added && cardinality + 1 > Config.get_config(:set_max_intset_entries)
            convert_intset_to_dict
          end

          added
        else
          convert_intset_to_dict
          @underlying.set(member, nil)
        end
      when Dict then @underlying.set(member, nil)
      else
        raise "Unknown type for structure: #{ @underlying }"
      end
    end

    def cardinality
      case @underlying
      when IntSet then @underlying.cardinality
      when Dict then @underlying.used
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    def members
      case @underlying
      when IntSet then @underlying.members.map { |i| Utils.integer_to_string(i) }
      when Dict then @underlying.keys
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    def pop
      case @underlying
      when IntSet then Utils.integer_to_string(@underlying.pop)
      when Dict then
        random_entry = @underlying.fair_random_entry
        @underlying.delete(random_entry.key)
        random_entry.key
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    def pop_with_count(count)
      return [] if count.nil? || count == 0

      # Case 1: count is greater or equal to the size of the set, we return the whole thing
      if count >= cardinality
        all_members = members
        clear
        return all_members
      end

      remaining = cardinality - count
      if remaining * SPOP_MOVE_STRATEGY_MUL > count
        # Case 2: Count is small compared to the size of the set, we "just" pop random elements

        count.times.map { pop }
      else
        # Case 3: count is big and close to the size of the set, and remaining is small, we do
        # the reverse, we pick remaining elements, and they become the new set
        new_set = RedisSet.new
        remaining.times { new_set.add(pop) }
        # We have removed all the elements that will be left in the set, so before swapping
        # them, we store all the elements left in the set, which are the ones that will end up
        # popped
        result = members

        # Now that we have saved all the members left, we clear the content of the set and copy
        # all the items from new_set, which are the ones left
        clear
        new_set.each { |member| add(member) }

        result
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
      return self if count >= cardinality

      # For both case 3 & 4 we need a new set
      new_set_content = Dict.new
      # Case 3: Number of elements in the set is too small to grab n random distinct members
      # from it so we instead pick random elements to remove from it
      # Start by creating a new set identical to self and then remove elements from it
      if count * SRANDMEMBER_SUB_STRATEGY_MUL > cardinality
        size = cardinality
        each { |member| new_set_content.add(member, nil) }
        while size > count
          random_entry = new_set_content.fair_random_entry
          new_set_content.delete(random_entry.key)
          size -= 1
        end
        return new_set_content.keys
      end

      # Case 4: The number of elements in the set is big enough in comparison to count so we
      # do the "classic" approach of picking count distinct elements
      added = 0
      while added < count
        member = random_member
        added += 1 if new_set_content.add(member, nil)
      end

      new_set_content.keys
    end

    def random_member
      case @underlying
      when IntSet then Utils.integer_to_string(@underlying.random_member)
      when Dict then @underlying.fair_random_entry.key
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    def empty?
      case @underlying
      when IntSet then @underlying.empty?
      when Dict then @underlying.used == 0
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    def include?(member)
      return false if member.nil?

      case @underlying
      when IntSet then
        if member.is_a?(Integer)
          member_as_int = member
        else
          member_as_int = Utils.string_to_integer_or_nil(member)
        end

        if member_as_int
          @underlying.member?(member_as_int)
        else
          false
        end
      when Dict then @underlying.member?(member)
      else raise "Unknown type for structure #{ @underlying }"
      end
    end
    alias member? include?

    def each(&block)
      case @underlying
      when IntSet then @underlying.each { |i| block.call(Utils.integer_to_string(i)) }
      when Dict then @underlying.each(&block)
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    def remove(member)
      case @underlying
      when IntSet
        member_as_integer = Utils.string_to_integer_or_nil(member)
        if member_as_integer
          @underlying.remove(member_as_integer)
        else
          false
        end
      when Dict then !@underlying.delete_entry(member).nil?
      else raise "Unknown type for structure #{ @underlying }"
      end
    end

    private

    def clear
      @underlying = IntSet.new
    end

    def convert_intset_to_dict
      dict = Dict.new
      @underlying.each do |member|
        dict[Utils.integer_to_string(member)] = nil
      end

      @underlying = dict
    end

    def convert_to_int_or_nil(member)
      Utils.string_to_integer(member)
    rescue InvalidIntegerString
      nil
    end
  end
end
