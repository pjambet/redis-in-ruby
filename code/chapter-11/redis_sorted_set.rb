require 'bigdecimal'

require_relative './dict'
require_relative './list'
require_relative './zset'

module BYORedis
  class RedisSortedSet

    Pair = Struct.new(:score, :member)

    class GenericRangeSpec

      attr_reader :min, :max, :min_exclusive, :max_exclusive
      alias min_exclusive? min_exclusive
      alias max_exclusive? max_exclusive

      def self.lex_range_spec(min, max, min_exclusive, max_exclusive)
        GenericRangeSpec.new(min, max, min_exclusive, max_exclusive) do |a, b|
          RedisSortedSet.lex_compare(a, b)
        end
      end

      def self.score_range_spec(min, max, min_exclusive, max_exclusive)
        GenericRangeSpec.new(min, max, min_exclusive, max_exclusive) do |a, b|
          a <=> b
        end
      end

      def self.rank_range_spec(min, max, cardinality)
        max = cardinality + max if max < 0
        min = cardinality + min if min < 0

        max = cardinality - 1 if max >= cardinality
        min = 0 if min < 0

        GenericRangeSpec.new(min, max, false, false) do |a, b|
          a <=> b
        end
      end

      def initialize(min, max, min_exclusive, max_exclusive, &block)
        @min = min
        @min_exclusive = min_exclusive
        @max = max
        @max_exclusive = max_exclusive
        @block = block
      end

      def empty?
        comparison = compare_with_max(min)
        comparison > 0 || (comparison == 0 && (min_exclusive? || max_exclusive?))
      end

      def compare_with_max(element)
        @block.call(element, @max)
      end

      def compare_with_min(element)
        @block.call(element, @min)
      end

      def in_range?(element)
        return false if empty?

        comparison_min = compare_with_min(element)
        comparison_max = compare_with_max(element)
        comparison_min_ok = min_exclusive? ? comparison_min == 1 : comparison_min >= 0
        comparison_max_ok = max_exclusive? ? comparison_max == -1 : comparison_max <= 0

        comparison_min_ok && comparison_max_ok
      end
    end

    attr_reader :underlying

    def initialize
      @underlying = List.new
    end

    def self.lex_compare(s1, s2)
      return 0 if s1 == s2
      return -1 if s1 == '-' || s2 == '+'
      return 1 if s1 == '+' || s2 == '-'

      s1 <=> s2
    end

    def self.intersection(sets_with_weight, aggregate: :sum)
      # Sort the sets smallest to largest
      sets_with_weight.sort_by! { |set, _| set.nil? ? 0 : set.cardinality }

      smallest_set = sets_with_weight[0][0]
      smallest_set_weight = sets_with_weight[0][1]
      return RedisSortedSet.new if smallest_set.nil?

      intersection_set = RedisSortedSet.new

      # Iterate over the first set, if we find a set that does not contain the member, discard
      smallest_set.each do |set_member|
        present_in_all_other_sets = true
        if set_member.is_a?(Pair)
          pair = set_member
        else
          pair = Pair.new(BigDecimal(1), set_member)
        end
        weighted_pair_score = Utils.multiply_or_zero_if_nan(smallest_set_weight, pair.score)

        # For each member of the smallest set, we loop through all the other sets and try to
        # find the member, if we don't find it, we break the loop and move on, if we do find
        # a member, then we need to apply the weight/aggregate logic to it
        sets_with_weight[1..-1].each do |set_with_weight|
          set = set_with_weight[0]
          weight = set_with_weight[1]

          if set == smallest_set
            other_pair = pair
          elsif set.is_a?(RedisSet)
            other_pair = set.member?(pair.member) ? Pair.new(BigDecimal(1), pair.member) : nil
          elsif set.is_a?(RedisSortedSet)
            other_pair = set.find_pair(pair.member)
          else
            raise "Unknown set type: #{ set }"
          end

          if other_pair
            weighted_other_pair_score = Utils.multiply_or_zero_if_nan(other_pair.score, weight)
            weighted_pair_score =
              aggregate_scores(aggregate, weighted_other_pair_score, weighted_pair_score)
          else
            present_in_all_other_sets = false
            break
          end
        end
        # Otherwise, keep
        if present_in_all_other_sets
          intersection_set.add(weighted_pair_score, pair.member, options: {})
        end
      end

      intersection_set
    end

    def self.union(sets_with_weight, aggregate: :sum)
      return RediSortedSet.new({}) if sets_with_weight.empty?

      accumulator = Dict.new

      sets_with_weight[0..-1].each do |set_with_weight|
        set = set_with_weight[0]
        weight = set_with_weight[1]
        next if set.nil?

        set.each do |set_member|
          if set.is_a?(RedisSet)
            pair = Pair.new(BigDecimal(1), set_member)
          elsif set.is_a?(RedisSortedSet)
            pair = set_member
          else
            raise "Unknown set type: #{ set }"
          end

          weighted_score = Utils.multiply_or_zero_if_nan(pair.score, weight)
          existing_entry = accumulator.get_entry(pair.member)
          if existing_entry
            new_score = aggregate_scores(aggregate, existing_entry.value, weighted_score)
            existing_entry.value = new_score
          else
            accumulator[pair.member] = weighted_score
          end
        end
      end

      union_set = RedisSortedSet.new
      accumulator.each do |key, value|
        union_set.add(value, key)
      end
      union_set
    end

    def self.aggregate_scores(aggregate, a, b)
      case aggregate
      when :sum then Utils.add_or_zero_if_nan(a, b)
      when :max then a < b ? b : a
      when :min then a < b ? a : b
      else raise "Unknown aggregate method: #{ aggregate }"
      end
    end
    private_class_method :aggregate_scores

    def cardinality
      case @underlying
      when List then @underlying.size
      when ZSet then @underlying.cardinality
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def add(score, member, options: {})
      convert_to_zset if @underlying.is_a?(List) &&
                         member.length > Config.get_config(:zset_max_ziplist_value)

      case @underlying
      when List
        added = add_list(score, member, options: options)
        convert_to_zset if added && cardinality >= Config.get_config(:zset_max_ziplist_entries)
        added
      when ZSet then @underlying.add(score, member, options)
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def find_pair(member)
      case @underlying
      when List then list_find_pair(member)
      when ZSet
        dict_entry = @underlying.dict.get_entry(member)
        Pair.new(dict_entry.value, dict_entry.key) if dict_entry
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def remove(member)
      case @underlying
      when List then remove_list(member)
      when ZSet then @underlying.remove_member(member)
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def remove_lex_range(range_spec)
      return 0 if range_spec.empty? ||
                  no_overlap_with_range?(range_spec) { |pair, _| pair.member }

      case @underlying
      when List then remove_lex_range_list(range_spec)
      when ZSet then @underlying.remove_lex_range(range_spec)
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def remove_rank_range(range_spec)
      return 0 if range_spec.empty? || no_overlap_with_range?(range_spec) { |_, rank| rank }

      case @underlying
      when List then remove_rank_range_list(range_spec)
      when ZSet then @underlying.remove_rank_range(range_spec.min, range_spec.max)
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def remove_score_range(range_spec)
      return 0 if range_spec.empty? ||
                  no_overlap_with_range?(range_spec) { |pair, _| pair.score }

      case @underlying
      when List then remove_score_range_list(range_spec)
      when ZSet then @underlying.remove_score_range(range_spec)
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def rev_rank(member)
      member_rank = rank(member)
      cardinality - 1 - member_rank if member_rank
    end

    def rank(member)
      case @underlying
      when List then find_member_in_list(member) { |_, rank| rank }
      when ZSet
        entry = @underlying.dict.get_entry(member)
        return nil unless entry

        @underlying.array.index(Pair.new(entry.value, member))
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def score(member)
      case @underlying
      when List then find_member_in_list(member) { |pair, _| pair.score }
      when ZSet then @underlying.dict[member]
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def empty?
      cardinality == 0
    end

    def each(&block)
      case @underlying
      when List then @underlying.each(&block)
      when ZSet then @underlying.array.each(&block)
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def pop_max(count)
      case @underlying
      when List then generic_pop(count) { @underlying.right_pop&.value }
      when ZSet
        generic_pop(count) do
          max = @underlying.array.pop
          @underlying.dict.delete(max.member) if max
          max
        end
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def pop_min(count)
      case @underlying
      when List then generic_pop(count) { @underlying.left_pop&.value }
      when ZSet
        generic_pop(count) do
          min = @underlying.array.shift
          @underlying.dict.delete(min.member) if min
          min
        end
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def count_in_lex_range(range_spec)
      return 0 if range_spec.empty? ||
                  no_overlap_with_range?(range_spec) { |pair, _| pair.member }

      case @underlying
      when List then count_in_lex_range_list(range_spec)
      when ZSet then @underlying.count_in_lex_range(range_spec)
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def count_in_score_range(range_spec)
      return 0 if range_spec.empty? ||
                  no_overlap_with_range?(range_spec) { |pair, _| pair.score }

      case @underlying
      when List then count_in_score_range_list(range_spec)
      when ZSet then @underlying.count_in_score_range(range_spec)
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def increment_score_by(member, increment)
      current_score = score(member) || BigDecimal(0)

      new_score = Utils.add_or_raise_if_nan(current_score, increment)
      add(new_score, member)

      new_score
    end

    def no_overlap_with_range?(range_spec, &block)
      # Note that in that each condition the "value" is abstract and determined by the return
      # value of calling the block variable, in practice it's either score, member, or rank
      # There is no overlap under the four following conditions:
      # 1. the range spec min is greater than the max value:
      # set  : |---|
      # range:       |---| (min can be inclusive or exclusive, doesn't matter)
      # 2. the range spec min is exclusive and is equal to the max value
      # set  : |---|
      # range:     (---|   (min is exclusive)
      # 3. the min value is greater than range spec max
      # set  :       |---|
      # range: |---|       (max can be inclusive or exclusive, doesn't matter)
      # 4. the min value is equal to the range spec max which is exclusive
      # set  :     |---|
      # range: |---(       (max is exclusive)
      max_pair, max_pair_rank = max_pair_with_rank
      min_pair, min_pair_rank = min_pair_with_rank
      set_max_range_spec_min_comparison =
        range_spec.compare_with_min(block.call(max_pair, max_pair_rank))
      set_min_range_spec_max_comparison =
        range_spec.compare_with_max(block.call(min_pair, min_pair_rank))

      set_max_range_spec_min_comparison == -1 || # case 1
        (range_spec.min_exclusive? && set_max_range_spec_min_comparison == 0) || # case 2
        set_min_range_spec_max_comparison == 1 || # case 3
        (range_spec.max_exclusive? && set_min_range_spec_max_comparison == 0) # case 4
    end

    private

    # @return [Array] Two values, the first is a Pair, and the second is the rank
    def max_pair_with_rank
      case @underlying
      when List
        return @underlying.tail.value, @underlying.size
      when ZSet
        return @underlying.array[-1], @underlying.array.size - 1
      else raise "Unknown type for #{ @underlying }"
      end
    end

    # @return [Array] Two values, the first is a Pair, and the second is the rank
    def min_pair_with_rank
      case @underlying
      when List
        return @underlying.head.value, 0
      when ZSet
        return @underlying.array[0], 0
      else raise "Unknown type for #{ @underlying }"
      end
    end

    def generic_count_list(range_spec)
      count = 0
      entered_range = false

      @underlying.each do |pair|
        in_range = range_spec.in_range?(yield(pair))

        if in_range
          entered_range ||= true
          count += 1
        elsif entered_range
          break
        end
      end

      count
    end

    def count_in_lex_range_list(range_spec)
      generic_count_list(range_spec, &:member)
    end

    def count_in_score_range_list(range_spec)
      generic_count_list(range_spec, &:score)
    end

    def find_member_in_list(member)
      index = 0
      @underlying.each do |pair|
        return yield pair, index if pair.member == member

        index += 1
      end

      nil
    end

    def remove_list(member)
      iterator = List.left_to_right_iterator(@underlying)
      while iterator.cursor
        if iterator.cursor.value.member == member
          @underlying.remove_node(iterator.cursor)
          return true
        end

        iterator.next
      end

      false
    end

    def generic_remove_range_list(range_spec)
      removed_count = 0
      iterator = List.left_to_right_iterator(@underlying)
      entered_range = false
      rank = 0

      while iterator.cursor
        pair = iterator.cursor.value
        in_range = range_spec.in_range?(yield(pair, rank))

        if in_range
          entered_range ||= true
          removed_count += 1
          next_node = iterator.cursor.next_node
          @underlying.remove_node(iterator.cursor)
          iterator.cursor = next_node
        elsif entered_range
          break
        else
          iterator.next
        end
        rank += 1
      end

      removed_count
    end

    def remove_lex_range_list(range_spec)
      generic_remove_range_list(range_spec) { |pair, _| pair.member }
    end

    def remove_rank_range_list(range_spec)
      generic_remove_range_list(range_spec) { |_, rank| rank }
    end

    def remove_score_range_list(range_spec)
      generic_remove_range_list(range_spec) { |pair, _| pair.score }
    end

    def generic_pop(count)
      popped = []
      return popped if count < 0

      while count > 0
        min = yield

        if min
          popped.push(min.member, min.score)
          count -= 1
        else
          break
        end
      end

      popped
    end

    def convert_to_zset
      raise "#{ @underlying } is not a List" unless @underlying.is_a?(List)

      zset = ZSet.new
      @underlying.each do |pair|
        zset.dict[pair.member] = pair.score
        zset.array << pair
      end

      @underlying = zset
    end

    def add_list(score, member, options: {})
      raise "#{ @underlying } is not a List" unless @underlying.is_a?(List)

      unless [ nil, :nx, :xx ].include?(options[:presence])
        raise "Unknown presence value: #{ options[:presence] }"
      end

      iterator = List.left_to_right_iterator(@underlying)

      while iterator.cursor
        cursor = iterator.cursor
        pair = iterator.cursor.value

        if pair.member == member
          # We found a pair in the list with a matching member

          if pair.score == score && !options[:incr]
            # We found an exact match, without the INCR option, so we do nothing
            return false
          elsif options[:presence] == :nx
            # We found an element, but because of the NX option, we do nothing
            return false
          else
            # The score changed, so we might to reinsert the element at the correct location to
            # maintain the list sorted
            new_score = options[:incr] ? Utils.add_or_raise_if_nan(pair.score, score) : score
            prev_node = cursor.prev_node
            next_node = cursor.next_node

            if (next_node.nil? ||
                next_node.value.score > new_score ||
                (next_node.value.score == score && next_node.value.member > member)) &&
               (prev_node.nil? ||
                prev_node.value.score < new_score ||
                (prev_node.value.score == score && prev_node.value.member < member))

              cursor.value.score = new_score
            else
              @underlying.remove_node(cursor)
              # We add the node back, which takes care of finding the correct index
              unless add_list(new_score, member, options: { member_does_not_exist: true })
                raise 'Unexpectedly failed to re-insert node after update'
              end
            end

            if options[:incr]
              return new_score
            else
              # If options[:ch] == true, then we want to count this update and return true
              return options[:ch]
            end
          end
        elsif pair.score > score || (pair.score == score && pair.member > member)
          # As soon as we find a node where its score is greater than the score of the
          # element we're attempting to insert, we store its reference in `location` so that
          # we can use insert_before_node below.
          # In case of a score equality, the right side of the || above, we use the
          # lexicographic order of the member value to sort them
          # We cannot stop here however because there might be an exact member match later in
          # the list, in which case the `if pair.member == member` check above will trigger
          # and return
          location ||= cursor
          if options[:member_does_not_exist]
            break
          else
            iterator.next
          end
        elsif pair.score < score || (pair.score == score && pair.member < member)
          # In this case we haven't found a node where the score is greater than the one we're
          # trying to insert, or the scores are equal but the lexicographic order tells us that
          # member is greater than the current node, so we keep searching for an insert location
          # to the right
          iterator.next
        else
          # We've covered all cases, this is never expected to happen
          raise "Unexpected else branch reached for #{ score }/#{ member }"
        end
      end

      return false if options[:presence] == :xx

      new_pair = Pair.new(score, member)
      if location
        @underlying.insert_before_node(location, new_pair)
      else
        @underlying.right_push(new_pair)
      end

      if options[:incr]
        score
      else
        true
      end
    end

    def list_find_pair(member)
      @underlying.each do |pair|
        return pair if pair.member == member
      end

      nil
    end
  end

  class SortedSetRankSerializer
    def initialize(sorted_set, range_spec, withscores: false, reverse: false)
      @sorted_set = sorted_set
      @range_spec = range_spec
      @withscores = withscores
      @reverse = reverse
    end

    def serialize
      return RESPArray.new([]).serialize if @range_spec.empty?

      case @sorted_set.underlying
      when List then serialize_list
      when ZSet then serialize_zset
      else raise "Unknown type for #{ @underlying }"
      end
    end

    private

    def serialize_zset
      members = []
      (@range_spec.min..@range_spec.max).each do |rank|
        pair = @sorted_set.underlying.array[rank]

        if @reverse
          members.prepend(Utils.float_to_string(pair.score)) if @withscores
          members.prepend(pair.member)
        else
          members.push(pair.member)
          members.push(Utils.float_to_string(pair.score)) if @withscores
        end
      end

      RESPArray.new(members).serialize
    end

    def serialize_list
      ltr_acc = lambda do |value, response|
        response << RESPBulkString.new(value.member).serialize
        if @withscores
          response << RESPBulkString.new(Utils.float_to_string(value.score)).serialize
        end
        @withscores ? 2 : 1
      end

      rtl_acc = lambda do |value, response|
        if @withscores
          response.prepend(RESPBulkString.new(Utils.float_to_string(value.score)).serialize)
        end
        response.prepend(RESPBulkString.new(value.member).serialize)
        @withscores ? 2 : 1
      end

      if @reverse
        tmp = ltr_acc
        ltr_acc = rtl_acc
        rtl_acc = tmp
      end

      ListSerializer.new(@sorted_set.underlying, @range_spec.min, @range_spec.max)
                    .serialize_with_accumulators(ltr_acc, rtl_acc)
    end
  end

  class SortedSetSerializerBy
    def initialize(sorted_set, range_spec,
                   offset: 0, count: -1, withscores: false, reverse: false, &block)
      @sorted_set = sorted_set
      @range_spec = range_spec
      @offset = offset
      @count = count
      @withscores = withscores
      @reverse = reverse
      if block.arity != 2
        @block = proc { |element, _| block.call(element) }
      else
        @block = block
      end
    end

    def serialize
      if @offset < 0 ||
         @range_spec.empty? ||
         @sorted_set.no_overlap_with_range?(@range_spec, &@block)

        return RESPArray.new([]).serialize
      end

      case @sorted_set.underlying
      when List then serialize_list
      when ZSet then serialize_zset
      else raise "Unknown type for #{ @underlying }"
      end
    end

    private

    def serialize_zset
      members = []

      if @reverse
        start_index = @sorted_set.underlying.array.last_index_in_range(@range_spec, &@block)
        if start_index.nil?
          raise "Unexpectedly failed to find last index in range for #{ self }"
        end

        indices = start_index.downto(0)
      else
        start_index = @sorted_set.underlying.array.first_index_in_range(@range_spec, &@block)
        if start_index.nil?
          raise "Unexpectedly failed to find first index in range for #{ self }"
        end

        indices = start_index.upto(@sorted_set.cardinality - 1)
      end

      indices.each do |i|
        item = @sorted_set.underlying.array[i]

        if @range_spec.in_range?(@block.call(item))
          if @offset == 0
            members << item.member
            members << Utils.float_to_string(item.score) if @withscores

            @count -= 1
            break if @count == 0
          else
            @offset -= 1
          end
        else
          break
        end
      end

      RESPArray.new(members).serialize
    end

    def serialize_list
      if @reverse
        iterator = List.right_to_left_iterator(@sorted_set.underlying)
      else
        iterator = List.left_to_right_iterator(@sorted_set.underlying)
      end
      members = []
      entered_range = false

      while iterator.cursor && @count != 0
        member = iterator.cursor.value

        if @range_spec.in_range?(@block.call(member))
          entered_range ||= true
          if @offset == 0
            members << member.member
            members << Utils.float_to_string(member.score) if @withscores

            @count -= 1
          else
            @offset -= 1
          end
        elsif entered_range == true
          break
        end

        iterator.next
      end

      RESPArray.new(members).serialize
    end
  end
end
