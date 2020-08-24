require 'fiddle'
require_relative './siphash'

class Dict

  INITIAL_SIZE = 4

  attr_reader :hash_tables

  def initialize(random_bytes)
    @hash_tables = [ HashTable.new(0), HashTable.new(0) ]
    @random_bytes = random_bytes
    @rehashidx = -1
  end

  def main_table
    @hash_tables[0]
  end

  def each
    main_table.each do |bucket|
      if bucket
        while bucket.next != nil
          yield bucket.key, bucket.value
        end
      end
    end
  end

  def rehash_milliseconds(millis)
    start = Time.now.to_f * 1000
    rehashes = 0
    while rehash(100) == 1
      rehashes += 100
      break if Time.now.to_f - start > millis
    end
    rehashes
  end

  def resize
    return if is_rehashing?

    minimal = main_table.used
    if minimal < INITIAL_SIZE
      minimal = INITIAL_SIZE
    end
    expand(minimal)
  end

  def expand(size)
    p 'EXPANDING! to '
    p size
    return if is_rehashing? || main_table.used > size

    real_size = next_power(size)

    return if real_size == main_table.size

    new_hash_table = HashTable.new(real_size)
    if main_table.table.nil?
      @hash_tables[0] = new_hash_table
      return
    end

    @hash_tables[1] = new_hash_table
    @rehashidx = 0
  end

  def include?(key)
    get(key) != nil
  end

  def add(key, value)
    # resize if need_resize

    index = key_index(key)
    p "index: #{ index }"

    table = is_rehashing? ? @hash_tables[1] : @hash_tables[0]

    p self
    p is_rehashing?
    p table

    entry = table.table[index]
    while entry
      if entry.key == key
        p 'already exists'
        break
      end
      entry = entry.next
    end

    if entry.nil?
      entry = DictEntry.new(key, value)
      entry.next = table.table[index]
      table.table[index] = entry
      table.used += 1
    else
      entry.value = value
    end

    p table
  end
  alias_method :[]=, :add

  def get(key)
    return if @hash_tables[0].used == 0 && @hash_tables[1].used == 0

    rehash_step if is_rehashing?

    hash = SipHash.digest(@random_bytes, key)
    (0..1).each do |i|
      table = @hash_tables[i]
      index = hash & table.sizemask
      p "THERE"
      p self
      entry = table.table[index]
      p 'HERE'
      p table
      while entry
        return entry.value if entry.key == key

        entry = entry.next
      end

      break unless is_rehashing?
    end
    return
  end
  alias_method :[], :get

  def delete(key)
    return if @hash_tables[0].used == 0 && @hash_tables[1].used == 0

    rehash_step if is_rehashing?

    hash_key = SipHash.digest(@random_bytes, key)
    (0..1).each do |i|
      table = @hash_tables[i]
      index = hash_key & table.sizemask
      entry = table.table[index]
      previous_entry = nil
      p "DELETING"
      p table
      while entry
        if entry.key == key
          if previous_entry
            previous_entry.next = entry.next
          else
            table.table[index] = entry.next
          end
        end
        previous_entry = entry
        entry = entry.next
        table.used -= 1
        return entry
      end
      break unless is_rehashing?
    end

    return
  end

  private

  def key_index(key)
    # TODO: Use the same API where we return -1 if already exists
    expand_if_needed
    hash = SipHash.digest(@random_bytes, key)
    index = nil

    (0..1).each do |i|
      table = @hash_tables[i]
      index = hash & table.sizemask

      break unless is_rehashing?
    end

    index
  end

  def rehash(n)
    p "REHASHING!"
    empty_visits = n * 10
    return 0 unless is_rehashing?

    while (n > 0 && main_table.used != 0)
      n -= 1
      entry = nil
      while main_table.table[@rehashidx].nil?
        @rehashidx += 1
        empty_visits -= 1
        if empty_visits == 0
          return 1
        end
      end
      entry = main_table.table[@rehashidx]
      while entry
        next_entry = entry.next
        idx = SipHash.digest(@random_bytes, entry.key) & @hash_tables[1].sizemask
        p self
        entry.next = @hash_tables[1].table[idx]
        @hash_tables[1].table[idx] = entry
        @hash_tables[0].used -= 1
        @hash_tables[1].used += 1
        entry = next_entry
      end
      main_table.table[@rehashidx] = nil
      @rehashidx += 1
    end

    if main_table.used == 0
      @hash_tables[0] = @hash_tables[1]
      @hash_tables[1] = HashTable.new(0)
      @rehashidx = -1
      return 0
    end

    p "REHASH SUMMARY"
    p @hash_tables[0]
    p @hash_tables[1]

    return 1
  end

  def is_rehashing?
    @rehashidx != -1
  end

  def expand_if_needed
    return if is_rehashing?

    if main_table.empty?
      expand(INITIAL_SIZE)
    elsif main_table.used >= main_table.size
      expand(main_table.size * 2)
    end
  end

  def next_power(size)
    # TODO: Handle wrapping integer
    i = INITIAL_SIZE
    loop do
      return i if i >= size

      i *= 2
    end
  end

  def rehash_step
    # Missing iterators check
    rehash(1)
  end

  class DictEntry

    attr_accessor :next, :value
    attr_reader :key

    def initialize(key, value)
      @key = key
      @value = value
      @next = nil
    end
  end

  class HashTable

    attr_reader :table, :size, :sizemask
    attr_accessor :used

    def initialize(size)
      @table = size == 0 ? nil : Array.new(size)
      @size = size
      @sizemask = size == 0 ? 0 : size - 1
      @used = 0
    end

    def empty?
      @size == 0
    end

    def each
      return unless @table

      @table.each
    end
  end
end
