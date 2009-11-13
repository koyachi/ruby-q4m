# -*- coding: utf-8 -*-
$:.unshift File.dirname(__FILE__)

require 'spec_helper'

describe 'basic' do
  before do
    @table = Q4MTestHelper::TABLES[0]
#    @q = Queue::Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
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

  it 'should 1' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    max = 32
    1.upto(max) do |i|
      # sth.executeがnil返すのでとりあえずノーチェック
#      @q.insert(@table, {:v => i}).should == 1
      @q.insert @table, {:v => i}
    end
    count = 0
    while @q.next(@table) do
      h = @q.fetch_hash
      count += 1
      break if h.v.to_i == max
    end
    # eachのほうがいいか？ -> queue-q4m-rubish/perlish
    # これだとh[:v]の確認できないな
    # @q.each(@table) do |h|
    #   count += 1
    # end

    count.should == max
  end

  it '2' do
    @q = Q4M.connect :table => @table, :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    _before = Time.now
    @q.next @table, 5
    (Time.now - _before).should >= 4
  end

  it '3' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    @table = Q4MTestHelper::TABLES[rand Q4MTestHelper::TABLES.length]
    @q.insert @table, {:v => 1}

    max = 1
    count = 0
#    while which = @q.next([Q4MTestHelper::TABLES, 5].flatten) do
#    while which = @q.next(*([Q4MTestHelper::TABLES, 5].flatten)) do
    while which = @q.next(Q4MTestHelper::TABLES, 5) do
      which.to_s.should == @table
      v = @q.fetch(which, 'v')
      count += 1
      break if count >= max
    end
  end

  it '4' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    timeout = 1
    @q.next(@table, timeout).should == nil # どうすっか
  end

  it '5' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    max = 32
    1.upto(max) do |i|
      @q.insert @table, {:v => i}
    end

    cond = "#{@table}:v>16"
    count = 0
    while rv = @q.next(cond) do
      rv.table.should == @table
      h = @q.fetch_hash
      count += 1
      break if h.v.to_i == max
    end

    count.should == 16

    @q.dbh.do "DELETE FROM #{@table}"
  end

  it '6' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    @q.disconnect

#    @q.insert(@table, {:v => 1}).should == 1
    @q.insert @table, {:v => 1}
#    @q.clear(@table).should == 1
    @q.clear(@table)
    
  end

  it '7' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

#    @q.insert(@table, {:v => 1}).should == 1
    @q.insert @table, {:v => 1}
#    @q.next(@table).should == 1
    @q.next(@table).rv.should == true
    @q.fetch(@table).should == ['1']
  end
end

