# -*- coding: utf-8 -*-
$:.unshift File.dirname(__FILE__)
 
require 'spec_helper'
require 'dbi'

describe 'Q4M::Status' do
  before do
    Q4MTestHelper.create_test_table
    @table = Q4MTestHelper::TABLES[0]
  end

  after do
    Q4MTestHelper.destroy_tables
  end

  it 'should be consistent' do
    q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    q.should be_an_instance_of(Q4M::Client)

    before_status = q.status
    
    max = 32
    1.upto(max) do |i|
      q.insert @table, {:v => i}
    end
    count = 0
    while q.next(@table) do
      h = q.fetch_hash
      count += 1
      break if h.v.to_i == max
    end

    count.should == max
    q.disconnect

    after_status = q.status

    (after_status.rows_written.to_i - before_status.rows_written.to_i).should == max
  end
end
