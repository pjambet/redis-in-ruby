module BYORedis
  class ZSet

    attr_reader :dict, :array

    def initialize
      @dict = Dict.new
      @array = SortedArray.by_fields(:score, :member)
    end

    def cardinality
      @array.size
    end

    def add(score, member, options)
      entry = @dict.get_entry(member)

      if entry
        return false if options[:presence] == 'nx'

        if entry.value != score || options[:incr]

          existing_pair = new_pair(entry.value, member)
          index = @array.index(existing_pair)
          if index.nil?
            raise "Failed to find #{ member }/#{ entry.value } in #{ @array.inspect }"
          end

          array_element = @array[index]
          if array_element != existing_pair
            raise "Failed to find #{ member }/#{ entry.value } in #{ @array.inspect }"
          end

          new_score = options[:incr] ? Utils.add_or_raise_if_nan(entry.value, score) : score
          next_member = @array[index + 1]
          prev_member = @array[index - 1]

          if (next_member.nil? ||
              next_member.score > new_score ||
              (next_member.score == new_score && next_member.member > member)) &&
             (prev_member.nil? ||
              prev_member.score < new_score ||
              (prev_member.score == new_score && prev_member.member < member))

            array_element.score = new_score
          else
            @array.delete_at(index)
            @array << new_pair(new_score, member)
          end
          entry.value = new_score
        end

        if options[:incr]
          new_score
        else
          options[:ch] # false by default
        end
      else
        return false if options[:presence] == 'xx'

        @array << new_pair(score, member)
        @dict[member] = score

        if options[:incr]
          score
        else
          true
        end
      end
    end

    def remove_member(member)
      entry = @dict.delete_entry(member)
      return false unless entry

      index = @array.index(new_pair(entry.value, member))
      @array.delete_at(index)

      true
    end

    def remove_lex_range(range_spec)
      generic_remove(range_spec) do |pair|
        pair.member
      end
    end

    def remove_rank_range(start, stop)
      removed = @array.slice!(start..stop)
      return 0 if removed.nil?

      removed.each do |pair|
        @dict.delete(pair.member)
      end
      removed.size
    end

    def remove_score_range(range_spec)
      generic_remove(range_spec) do |pair|
        pair.score
      end
    end

    def count_in_lex_range(range_spec)
      generic_count(range_spec) do |pair|
        pair.member
      end
    end

    def count_in_rank_range(range_spec)
      generic_count(range_spec) do |pair|
        pair.score
      end
    end

    private

    # It is more than recommended to check that there is some overlap between the range_spec and
    # this set RedisSortedSet provides that with the no_overlap_with_range? method
    def generic_count(range_spec, &block)
      first_in_range_index = @array.first_index_in_range(range_spec, &block)
      last_in_range_index = @array.last_index_in_range(range_spec, &block)

      # We need to add 1 because the last index - the first index is off by one:
      # < 1, 2, 3, 4, 5>, with the range 2, 4, has the indices 1 & 3, 3 - 1 + 1 == 3
      last_in_range_index - first_in_range_index + 1
    end

    def new_pair(score, member)
      RedisSortedSet::Pair.new(score, member)
    end

    def generic_remove(range_spec, &block)
      first_in_range_index = @array.first_index_in_range(range_spec, &block)
      last_in_range_index = first_in_range_index
      (first_in_range_index.upto(@array.size - 1)).each do |rank|
        pair = @array[rank]
        in_range = range_spec.in_range?(yield(pair))

        if in_range
          last_in_range_index = rank
        else
          break
        end
      end
      remove_rank_range(first_in_range_index, last_in_range_index)
    end
  end
end
