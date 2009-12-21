# -*- coding: utf-8 -*-
$:.unshift File.dirname(__FILE__)

require 'spec_helper'
require 'timeout'

class Worker1
  include Q4M::Worker
  def initialize(config=nil)
    config ||= {:queue_tables => Q4MTestHelper::TABLES[0]}
    @queue_tables = config[:queue_tables]
    @count = 1
  end

  def work(job, queue)
    job[:v].to_i.should == @count
    @count += 1
  end
end

class Worker2
  include Q4M::Worker
  def initialize(config=nil)
    config ||= {:queue_tables => Q4MTestHelper::TABLES[1]}
    @queue_tables = config[:queue_tables]
    @count = 1
  end

  def work(job, queue)
    job[:v].to_i.should == @count * 2
    @count += 1
  end
end

class Worker3
  include Q4M::Worker
  def initialize(config=nil)
    config ||= {:queue_tables => [Q4MTestHelper::TABLES[2], Q4MTestHelper::TABLES[3]]}
    @queue_tables = config[:queue_tables]
    @count = 1
  end

  def work(job, queue)
    if queue == Q4MTestHelper::TABLES[2]
      job[:v].to_i.should == @count
      if @count == 32
        @count = 1
      else
        @count += 1
      end
    else
      job[:v].to_i.should == @count * 2
      @count += 1
    end
  end
end

class Worker4
  include Q4M::Worker
  def initialize(config=nil)
    config ||= {:queue_tables => [Q4MTestHelper::TABLES[4], Q4MTestHelper::TABLES[5]]}
    @queue_tables = config[:queue_tables]
    @count = 1
  end

  def work(job, queue)
    if queue == Q4MTestHelper::TABLES[4]
      job[:v].to_i.should == @count * 3
      if @count == 32
        @count = 1
      else
        @count += 1
      end
    else
      job[:v].to_i.should == @count * 4
      @count += 1
    end
  end
end

describe 'basic' do
  before do
    @table = Q4MTestHelper::TABLES[0]
    @table2 = Q4MTestHelper::TABLES[1]
    @table3 = Q4MTestHelper::TABLES[2]
    @table4 = Q4MTestHelper::TABLES[3]
    @table5 = Q4MTestHelper::TABLES[4]
    @table6 = Q4MTestHelper::TABLES[5]
    @q = nil
  end

  after do
    @q.disconnect unless @q.nil?
  end

  after(:all) do
    dbh = DBI.connect *Q4MTestHelper::CONNECT_INFO
    Q4MTestHelper::TABLES.each do |table|
      dbh.do "DROP TABLE #{table}"
    end
  end


  it 'should start_worker with 1 worker' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    max = 32
    1.upto(max) do |i|
      @q.insert @table, {:v => i}
    end
    begin
      timeout(33) do
        @q.start_worker [Worker1]
      end
    rescue TimeoutError => e
    end
  end

  it 'should start_worker with 1 worker, with config at start_worker' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    max = 32
    1.upto(max) do |i|
      @q.insert @table, {:v => i}
    end
    begin
      timeout(33) do
        @q.start_worker [Worker1], {:queue_tables => Q4MTestHelper::TABLES[0]}
      end
    rescue TimeoutError => e
    end
  end


  it 'should start_worker with 2 workers' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    max = 32
    1.upto(max) do |i|
      @q.insert @table, {:v => i}
      @q.insert @table2, {:v => i * 2}
    end
    begin
      timeout(33) do
        @q.start_worker [Worker1, Worker2]
      end
    rescue TimeoutError => e
    end
  end

# queue_tables二つ指定するとどっちから取得できるかタイミングで違うので同じ期待値にできない(workメソッド内のshould)
#  it 'should start_worker with 2 workers, with config at start_worker' do
#    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
#    @q.should be_an_instance_of(Q4M::Client)
#
#    max = 32
#    1.upto(max) do |i|
#      @q.insert @table, {:v => i}
#      @q.insert @table2, {:v => i * 2}
#    end
#    begin
#      timeout(33) do
#        @q.start_worker [Worker1, Worker2], {:queue_tables => [Q4MTestHelper::TABLES[0], Q4MTestHelper::TABLES[1]]}
#      end
#    rescue TimeoutError => e
#    end
#  end


  it 'should start_worker with 1 worker, 2 queues' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    max = 32
    1.upto(max) do |i|
      @q.insert @table3, {:v => i}
      @q.insert @table4, {:v => i * 2}
    end
    begin
      timeout(32 * 2) do
        @q.start_worker [Worker3]
      end
    rescue TimeoutError => e
    end
  end

  it 'should start_worker with 1 worker, 2 queues, with config at start_worker' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    max = 32
    1.upto(max) do |i|
      @q.insert @table3, {:v => i}
      @q.insert @table4, {:v => i * 2}
    end
    begin
      timeout(32 * 2) do
        @q.start_worker [Worker3], {:queue_tables => [Q4MTestHelper::TABLES[2], Q4MTestHelper::TABLES[3]]}
      end
    rescue TimeoutError => e
    end
  end


  it 'should start_worker with 2 workers, 2 queues' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    max = 32
    1.upto(max) do |i|
      @q.insert @table3, {:v => i}
      @q.insert @table4, {:v => i * 2}
      @q.insert @table5, {:v => i * 3}
      @q.insert @table6, {:v => i * 4}
    end
    begin
      timeout(32 * 4) do
        @q.start_worker [Worker3, Worker4]
      end
    rescue TimeoutError => e
    end
  end

# queue_tables二つ指定するとどっちから取得できるかタイミングで違うので同じ期待値にできない(workメソッド内のshould)
#  it 'should start_worker with 2 workers, 2 queues, with config at start_worker' do
#    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
#    @q.should be_an_instance_of(Q4M::Client)
#
#    max = 32
#    1.upto(max) do |i|
#      @q.insert @table3, {:v => i}
#      @q.insert @table4, {:v => i * 2}
#      @q.insert @table5, {:v => i * 3}
#      @q.insert @table6, {:v => i * 4}
#    end
#    begin
#      timeout(32 * 4) do
#        @q.start_worker [Worker3, Worker4], {:queue_tables => [Q4MTestHelper::TABLES[2], Q4MTestHelper::TABLES[3], Q4MTestHelper::TABLES[4], Q4MTestHelper::TABLES[5]]}
#      end
#    rescue TimeoutError => e
#    end
#  end
end
