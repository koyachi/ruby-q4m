
class Q4M::Worker
  attr_accessor :queue

  def initialize
    @queue = nil
  end

  def work(job, queue=nil)
  end
end
