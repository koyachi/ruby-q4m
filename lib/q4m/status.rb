# -*- coding: utf-8 -*-
module Q4M
  class Status < Struct
    class Error < StandardError; end
    class RequireDatabaseHandler < Error; end

    def self.fetch(dbh=nil)
      raise RequireDatabaseHandler if dbh.nil?
      sth = dbh.prepare 'SHOW ENGINE QUEUE STATUS'
      sth.execute
      dummy1, dummy2, status = sth.fetch
      sth.finish
      hash = {}
      new_methods = []
      status.split(/\r?\n/).each do |line|
        next unless (line =~ /^([\w_]+)\s+(\d+)$/)

        name, value = $1, $2
        # rubyのメソッドとかぶらないかチェック必要か
#        unless self.class.methods.include? name
#          new_methods.push name
#        end
        hash[name] = value
      end
      ::Q4M.StructFromHash hash
    end
  end
end
