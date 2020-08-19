def add(map, key, value)
  map.each do |pair|
    pair_key = pair[0]
    # Override the value if the key is already present
    if key == pair_key
      pair[1] = value
      return pair
    end
  end
  pair = [key, value]
  map << pair
  pair
end

def lookup(map, key)
  map.each do |pair_key, pair_value|
    return pair_value if key == pair_key
  end
  return
end

map = []
add(map, "key-1", "value-1") # => ["key-1", "value-1"]
add(map, "key-2", "value-2") # => ["key-2", "value-2"]
p add(map, "key-2", "value-3") # => ["key-2", "value-3"]
p map

lookup(map, "key-1") # => "value-1"
lookup(map, "key-2") # => "value-2"
lookup(map, "key-3") # => nil
