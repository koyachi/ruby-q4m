# -*- coding: utf-8 -*-
$:.unshift File.dirname(__FILE__)
 
require 'spec_helper'
require 'dbi'

describe 'status' do
  before do
#    @dsn = ENV['Q4M_DSN']
#    @username = ENV['Q4M_USER']
#    @password = ENV['Q4M_PASSWORD']
    @tables = (1..10).map {|i| [%w[q4m test table], i, $$].flatten.join '_'}
#    if @dsn =~ /^dbi:mysql:/i
#      @dsn = "dbi:mysql:dbname=#{@dsn}"
#    end
#    @dbh = DBI.connect @dsn, @username, @password
    @dbh = DBI.connect *Q4MTestHelper::CONNECT_INFO
    @tables.each do |table|
      @dbh.do "CREATE TABLE IF NOT EXISTS #{table} (v INTEGER NOT NULL) ENGINE=queue;"
    end
    @table = @tables[0]
  end

  after do
    @tables.each do |table|
      @dbh.do "DROP TABLE #{table}"
    end
  end

  it 'should foffff' do
    q = Q4M.connect :connect_info => Q4MTestHelper::CONNECT_INFO
    q.should be_an_instance_of(Q4M::Client)

    before_status = q.status
    
    max = 32
    1.upto(max) do |i|
#      q.insert(@table, {:v => i}).should == 1
      q.insert(@table, {:v => i}).should
    end
    count = 0
    while q.next(@table) do
      h = q.fetch_hash
      count += 1
      break if h.v.to_i == max
    end
    # eachのほうがいいか？ -> queue-q4m-rubish/perlish
    # これだとh[:v]の確認できないな
    # @q.each(@table) do |h|
    #   count += 1
    # end

    count.should == max
    q.disconnect

    after_status = q.status

#    q.status.should be_an_instance_of(Q4M::Status)
#    q.status.rows_written.to_i.should == 1
    (after_status.rows_written.to_i - before_status.rows_written.to_i).should == max
  end
end
