
module Q4M::Worker
  attr_accessor :queue_tables

  def initialize(config=nil)
    @queue_tables = config[:queue_tables] || nil
  end

  def work(job, queue=nil)
  end
end
