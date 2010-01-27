$:.unshift File.dirname(__FILE__) + '/../lib/'

require 'q4m'

module Q4MTestHelper
  dsn = ENV['Q4M_DSN'] || 'DBI:Mysql:database=test_q4m'
  username = ENV['Q4M_USER'] || 'root'
  password = ENV['Q4M_PASSWORD'] || ''

  if dsn !~ /^DBI:Mysql:/
    dsn = "DBI:Mysql:database=#{dsn}"
  end

  CONNECT_INFO = [dsn, username, password]
  TABLES = (1..10).map {|v| ['q4m', 'test', v, $$].join('_')}

  class << self
    def create_test_table
      dbh = DBI.connect *CONNECT_INFO
      TABLES.each do |table|
        sql = "CREATE TABLE IF NOT EXISTS #{table} (v INTEGER NOT NULL) ENGINE=QUEUE;"
        dbh.do sql
      end
    end

    def create_queue
      Q4M :connect_info => CONNECT_INFO
    end

    def destroy_tables
      begin
        dbh = DBI.connect *CONNECT_INFO
        TABLES.each do |table|
          dbh.do "DROP TABLE IF EXISTS #{table}"
        end
      rescue
        p $!
      end
    end
  end
end
