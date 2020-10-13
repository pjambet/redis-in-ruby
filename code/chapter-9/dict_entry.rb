module BYORedis
  class DictEntry

    attr_accessor :next, :value
    attr_reader :key

    def initialize(key, value)
      @key = key
      @value = value
      @next = nil
    end
  end
end
