module BYORedis
  class SkipList

    MAX_LEVEL = 32 # 32 is what Redis uses
    P = 0.25

    Node = Struct.new(:member, :score, :backward, :levels, keyword_init: true)
    Level = Struct.new(:forward, :span, keyword_init: true)

    attr_reader :length

    def initialize
      @header = Node.new(member: nil, score: 0, backward: nil,
                         levels: Array.new(MAX_LEVEL) { |_| Level.new(forward: nil, span: 0) })
      @tail = nil
      @length = 0
      @level = 1
    end

    def search(val)
    end

    def insert(score, member)
      x = @header
      update = Array.new(MAX_LEVEL)
      rank = Array.new(MAX_LEVEL)

      (@level - 1).downto(0) do |level|
        rank[level] = (level == @level - 1) ? 0 : rank[level + 1]
        # p "levels: #{ x.levels.inspect }, level: #{ level }"
        # p "levels: #{ x.levels[level] }"
        while x.levels[level].forward && (x.levels[level].forward.score < score ||
                                          (x.levels[level].forward.score == score &&
                                           x.levels[level].forward.member < member))
          rank[level] = x.levels[level].span
          x = x.levels[level].forward
        end
        update[level] = x
      end

      # p "Done with the update: #{ update.inspect }"

      level = random_level
      if level > @level
        @level.upto(level - 1).each do |i|
          rank[i] = 0
          update[i] = @header
          update[i].levels[i].span = @length
        end
        @level = level
      end

      # p "We're at level: #{ level }/ #{ @level }"

      x = Node.new(member: member, score: score, backward: nil,
                   levels: Array.new(level) { |_| Level.new(span: 0, forward: nil) })
      # p "Here's x:"
      # p x

      0.upto(level - 1).each do |i|
        x.levels[i].forward = update[i].levels[i].forward
        update[i].levels[i].forward = x

        # Update span covered by update[i] as x is inserted here
        x.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
        update[i].levels[i].span = (rank[0] - rank[i]) + 1
      end

      # Increment span for untouched levels
      level.upto(@level - 1) do |i|
        update[i].levels[i].span += 1
      end

      # p "Done updating the levels, last step is backward and tail, #{ update[0] == @header }"

      x.backward = (update[0] == @header) ? nil : update[0]
      if x.levels[0].forward
        x.levels[0].forward.backward = x
      else
        @tail = x
      end

      @length += 1
      # p update

      x
    end

    def show
      p '---'
      @header.levels.each.with_index do |l,i|
        els = if l.forward.nil?
                ["N/A"]
              else
                nodes = []
                while l.forward
                  nodes << "#{l.forward.member}/#{l.forward.score}"
                  l = l.forward.levels[i]
                end
                nodes
              end
        p "Level: #{i + 1}, #{ els.join(',') }"
      end
      p "@tail:"
      p @tail
    end

    def delete(score, member)
    end

    private

    def update_score(current_score, element, new_score)
    end

    def delete_node(node, update)
    end

    def random_level
      level = 1

      while rand(0xffff) < (P * 0xffff)
        level += 1
      end

      # p "Ended up at level: #{ level }"

      level < MAX_LEVEL ? level : MAX_LEVEL
    end
  end
end
