require_relative './siphash'
require_relative './dict_entry'
require_relative './hash_table'

module BYORedis
  class Dict

    INITIAL_SIZE = 4
    MAX_SIZE = 2**63

    attr_reader :hash_tables
    def initialize
      @hash_tables = [ HashTable.new(0), HashTable.new(0) ]
      @rehashidx = -1
    end

    def used
      main_table.used + rehashing_table.used
    end

    def rehash_milliseconds(millis)
      start = Time.now.to_f * 1000
      rehashes = 0
      while rehash(100) == 1
        rehashes += 100
        time_ellapsed = Time.now.to_f * 1000 - start

        break if time_ellapsed > millis
      end
      rehashes
    end

    def resize
      return if rehashing?

      minimal = main_table.used
      minimal = INITIAL_SIZE if minimal < INITIAL_SIZE

      expand(minimal)
    end

    def include?(key)
      !get_entry(key).nil?
    end

    # Dangerous method that can create duplicate if used incorrectly, should only be called if
    # get_entry was previously called and returned nil
    # Explain that calling add while rehashing can create a race condition
    def add(key, value)
      index = key_index(key)

      # Only happens if we didn't check the presence before calling this method
      return nil if index == -1

      rehash_step if rehashing?

      hash_table = rehashing? ? rehashing_table : main_table
      entry = hash_table.table[index]

      entry = entry.next while entry && entry.key != key

      if entry.nil?
        entry = DictEntry.new(key, value)
        entry.next = hash_table.table[index]
        hash_table.table[index] = entry
        hash_table.used += 1
      else
        raise "Unexpectedly found an entry with same key when trying to add #{ key } / #{ value }"
      end
    end

    def set(key, value)
      entry = get_entry(key)
      if entry
        entry.value = value
      else
        add(key, value)
        # Maybe raise if didn't add?
      end
    end
    alias []= set

    def get_entry(key)
      return if main_table.used == 0 && rehashing_table.used == 0

      rehash_step if rehashing?

      hash = SipHash.digest(RANDOM_BYTES, key)

      iterate_through_hash_tables_unless_rehashing do |hash_table|
        index = hash & hash_table.sizemask

        entry = hash_table.table[index]

        while entry
          return entry if entry.key == key

          entry = entry.next
        end
      end

      nil
    end

    def get(key)
      get_entry(key)&.value
    end
    alias [] get

    def delete(key)
      return if main_table.used == 0 && rehashing_table.used == 0

      rehash_step if rehashing?

      hash_key = SipHash.digest(RANDOM_BYTES, key)
      iterate_through_hash_tables_unless_rehashing do |hash_table|
        index = hash_key & hash_table.sizemask
        entry = hash_table.table[index]
        previous_entry = nil

        while entry
          if entry.key == key
            if previous_entry
              previous_entry.next = entry.next
            else
              hash_table.table[index] = entry.next
            end
            hash_table.used -= 1
            return entry.value
          end
          previous_entry = entry
          entry = entry.next
        end
      end

      nil
    end

    def each
      return if main_table.used == 0 && rehashing_table.used == 0

      start_index = rehashing? ? @rehashidx : 0
      main_table.table[start_index..-1].each do |bucket|
        next if bucket.nil?

        until bucket.nil?
          yield bucket.key, bucket.value
          bucket = bucket.next
        end
      end
      return unless rehashing?

      rehashing_table.each do |bucket|
        next if bucket.nil?

        until bucket.nil?
          yield bucket.key, bucket.value
          bucket = bucket.next
        end
      end
    end

    def keys
      keys = []

      each do |key, _|
        keys << key
      end

      keys
    end

    def values
      values = []

      each do |_, value|
        values << value
      end

      values
    end

    def needs_resize?(min_fill: 10)
      size = slots

      size > INITIAL_SIZE && ((used * 100) / size < min_fill)
    end

    private

    def slots
      hash_tables[0].size + hash_tables[1].size
    end

    def main_table
      @hash_tables[0]
    end

    def rehashing_table
      @hash_tables[1]
    end

    def expand(size)
      return if rehashing? || main_table.used > size

      real_size = next_power(size)

      return if real_size == main_table.size

      new_hash_table = HashTable.new(real_size)

      # Is this the first initialization? If so it's not really a rehashing
      # we just set the first hash table so that it can accept keys.
      if main_table.table.nil?
        @hash_tables[0] = new_hash_table
      else
        @hash_tables[1] = new_hash_table
        @rehashidx = 0
      end
    end

    # In the Redis codebase, they extensively use the following pattern:
    # for (table = 0; table <= 1; table++) {
    #   ...
    #   if (!dictIsRehashing(d)) break;
    # }
    # This is common for many operations, such as finding or deleting an item in the dict,
    # we first need to look at the main table, the first table, but we haven't found in the
    # first one, we should look in the rehashing table, the second one, but only if we're in
    # the process of rehashing.
    # Taking advantage of Ruby blocks, we can write this helper method instead
    def iterate_through_hash_tables_unless_rehashing
      @hash_tables.each do |hash_table|
        yield hash_table
        break unless rehashing?
      end
    end

    def key_index(key)
      expand_if_needed
      hash = SipHash.digest(RANDOM_BYTES, key)
      index = nil

      iterate_through_hash_tables_unless_rehashing do |hash_table|
        index = hash & hash_table.sizemask
        entry = hash_table.table[index]
        while entry
          # The key is already present in the hash so there's no valid index where to add it
          if entry.key == key
            return -1
          else
            entry = entry.next
          end
        end
      end

      index
    end

    def rehash(n)
      empty_visits = n * 10
      return 0 unless rehashing?

      while n > 0 && main_table.used != 0
        n -= 1
        entry = nil

        while main_table.table[@rehashidx].nil?
          @rehashidx += 1
          empty_visits -= 1
          return 1 if empty_visits == 0
        end

        entry = main_table.table[@rehashidx]

        while entry
          next_entry = entry.next
          idx = SipHash.digest(RANDOM_BYTES, entry.key) & rehashing_table.sizemask

          entry.next = rehashing_table.table[idx]
          rehashing_table.table[idx] = entry
          main_table.used -= 1
          rehashing_table.used += 1
          entry = next_entry
        end
        main_table.table[@rehashidx] = nil
        @rehashidx += 1
      end

      # Check if we already rehashed the whole table
      if main_table.used == 0
        @hash_tables[0] = rehashing_table
        @hash_tables[1] = HashTable.new(0)
        @rehashidx = -1
        0
      else
        # There's more to rehash
        1
      end
    end

    def rehashing?
      @rehashidx != -1
    end

    def expand_if_needed
      return if rehashing?

      if main_table.empty?
        expand(INITIAL_SIZE)
      elsif main_table.used >= main_table.size
        expand(main_table.size * 2)
      end
    end

    def next_power(size)
      # Ruby has practically no limit to how big an integer can be, because under the hood the
      # Integer class allocates the necessary resources to go beyond what could fit in a 64 bit
      # integer.
      # That being said, let's still copy what Redis does, since it makes sense to have an
      # explicit limit about how big our Dicts can get
      i = INITIAL_SIZE
      return MAX_SIZE if size >= MAX_SIZE

      loop do
        return i if i >= size

        i *= 2
      end
    end

    def rehash_step
      rehash(1)
    end
  end
end
