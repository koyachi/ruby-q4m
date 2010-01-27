# -*- coding: utf-8 -*-
$:.unshift File.dirname(__FILE__)

require 'spec_helper'

describe 'Q4M::Client basic methods' do
  before do
    Q4MTestHelper.create_test_table
    @table = Q4MTestHelper::TABLES[0]
    @q = nil
  end

  after do
    @q.disconnect unless @q.nil?
  end

  after(:all) do
    Q4MTestHelper.destroy_tables
  end

  it 'should fetched as hash when fetched by #fetch_hash' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    max = 32
    1.upto(max) do |i|
      @q.insert @table, {:v => i}
    end
    count = 0
    while @q.next(@table) do
      h = @q.fetch_hash
      count += 1
      h[:v].to_i.should == count
      break if h.v.to_i == max
    end

    count.should == max
  end

  it 'should timeout if specified timeout sec to #next' do
    @q = Q4M.connect :table => @table, :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    _before = Time.now
    @q.next @table, 5
    (Time.now - _before).should >= 4
  end

  it 'should fetch inserted queue if more than one queues that include inserted queue ware specified' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    @table = Q4MTestHelper::TABLES[rand Q4MTestHelper::TABLES.length]
    @q.insert @table, {:v => 1}

    max = 1
    count = 0
    while which = @q.next(Q4MTestHelper::TABLES, 5) do
      which.to_s.should == @table
      v = @q.fetch(which, 'v')
      count += 1
      break if count >= max
    end
  end

  it 'should return nil if expire specified time to #next' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    timeout = 1
    @q.next(@table, timeout).should == nil
  end

  it 'should fetch queues that meets condition specified ti #next' do
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

  it 'should have no rows after #clear' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    @q.disconnect

    @q.insert @table, {:v => 1}
    @q.clear(@table)
    
    @q.dbh.select_one("SELECT * FROM #{@table};").should == nil
  end

  it 'should #fetch' do
    @q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    @q.should be_an_instance_of(Q4M::Client)

    @q.insert @table, {:v => 1}
    @q.next(@table).rv.should == true
    @q.fetch(@table).should == ['1']
  end
end

