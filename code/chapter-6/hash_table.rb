module Redis
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
