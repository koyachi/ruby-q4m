$:.unshift File.dirname(__FILE__) + '/../lib/'

require 'rubygems'
require 'q4m'

q = Q4M.connect :connect_info => ['DBI:Mysql:database=test', 'root', '']
table = 'my_queue'
q.insert(table, :v1 => 100, :v2 => 'hoge ' + Time.now.to_s)
