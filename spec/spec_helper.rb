$:.unshift File.dirname(__FILE__) + '/../lib/'

require 'q4m'

module Q4MTestHelper
  dsn = ENV['Q4M_DSN'] || 'DBI:Mysql:database=test_q4m'
  username = ENV['Q4M_USER'] || 'root'
  password = ENV['Q4M_PASSWORD'] || ''

  if dsn !~ /^DBI:Mysql:/
    dsn = "DBI:Mysql:database=#{dsn}"
  end

  begin
    CONNECT_INFO = [dsn, username, password]
    dbh = DBI.connect *CONNECT_INFO
    TABLES = (1..10).map {|v| ['q4m', 'test', v, $$].join('_')}
    TABLES.each do |table|
      sql = "CREATE TABLE IF NOT EXISTS #{table} (v INTEGER NOT NULL) ENGINE=QUEUE;"
      dbh.do sql
    end
  rescue Exception => e
    p e
    # SKIP ALL "Could not setup mysql"
  end

  def create_queue
    Q4M :connect_info => CONNECT_INFO
  end

  def destroy_tables
    begin
      dbh = DBI.connect *CONNECT_INFO
      TABLES.each do |table|
        dbh.do 'DROP TABLES #{table}'
      end
    rescue e
      p e
    end
  end
end
