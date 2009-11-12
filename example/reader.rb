$:.unshift File.dirname(__FILE__) + '/../lib/'

require 'rubygems'
require 'q4m'

Signal.trap(:INT) do
  Q4M.disconnect
  p 'END'
  exit
end

q = Q4M.connect :connect_info => ['DBI:Mysql:database=test', 'root', '']
table = 'my_queue'
i = 1

def type_a(q, table)
  while q.next(table)
    v1,v2 = q.fetch(table)
    p "v1 = #{v1}, v2 = #{v2}"
    p "...waiting..."
    sleep 1
  end
end
def type_b(q, table)
  q.next(table)
  v1,v2 = q.fetch(table)
  p "v1 = #{v1}, v2 = #{v2}"
  p "...waiting..."
  sleep 1

  q.next(table)
  v1,v2 = q.fetch(table)
  p "v1 = #{v1}, v2 = #{v2}"
  p "...waiting..."
  sleep 1
end

#type_b q, table
type_a q, table
q.disconnect
p "end"
