require 'fiddle'

class BYOArray < BasicObject

  PTR_SIZE = ::Fiddle::SIZEOF_LONG

  def initialize(max_size)
    @max_size = max_size
    @current_size = 0
    @beginning_address = ::Fiddle::Pointer.malloc(PTR_SIZE)
  end

  def add(str)
    ::Kernel.raise 'Array is full' if @current_size == @max_size

    ptr = ::Fiddle::Pointer.to_ptr(::Marshal.dump(str))
    offset = @current_size * PTR_SIZE # 0 at first, then 8, 16, etc ...

    @beginning_address[offset, PTR_SIZE] = ptr.ref

    @current_size += 1
    self
  end

  def get(i)
    return nil if i < 0 || i >= @current_size

    address = @beginning_address[i * PTR_SIZE, PTR_SIZE].unpack('Q')[0]
    ::Marshal.load(::Fiddle::Pointer.new(address).to_s)
  end

  def to_s
    "Size: #{ @current_size }"
  end
end

C = ::Struct.new(:a)

ary = BYOArray.new(10)
::Kernel.puts ary.add("foo")
::Kernel.puts ary.add(C.new(1))

::Kernel.puts ary.get(0)

::Kernel.puts ary.get(1)
::Kernel.puts ary.get(2)
::Kernel.puts ary.add("bar")
