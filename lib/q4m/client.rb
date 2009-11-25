# -*- coding: utf-8 -*-

class Q4M::Client
  def self.destroy(_dbh, _owner_mode)
    proc {
      if _dbh
        _dbh.do "SELECT queue_abort();" if _owner_mode == 1
        _dbh.do "SELECT queue_end();"
        _dbh.disconnect
        _dbh = nil
      end
    }
  end

  class Error < StandardError; end
  class NotConnected < Error; end

  include ::Q4M::Loggable
  attr_accessor :connect_info, :auto_connect, :owner_mode
  attr_accessor :_connect_pid, :_dbh, :__table, :__res

  def initialize(args)
    @connect_info = args[:connect_info] || nil
    @auto_reconnect = args[:auto_reconnect] || 1
    @owner_mode = args[:owner_mode] || 0
    @_connect_pid = args[:_connect_pid] || nil
    # @sql_maker
    @_dbh = nil
    @__table = nil
    @__res = nil

    ObjectSpace.define_finalizer self, self.class.destroy(@_dbh, @owner_mode)
  end

  def _connect
    @_dbh = DBI.connect *@connect_info
    @_dbh
  end

  def disconnect
    self.class.destroy(self.dbh, owner_mode).call
  end

  def dbh
    _dbh = @_dbh
    pid = @_connect_pid
    pingpong = begin
      !_dbh.ping
      true
    rescue DBI::InterfaceError => e
      false
    end
    if (pid || '') != $$ || !_dbh || !pingpong
      @auto_reconnect || raise(NotConnected)
      _dbh = _connect
      @_connect_pid = $$
    end
    ::Q4M.dbh = _dbh
    _dbh
  end

  def next(*args)
    @__table = nil
    tables = args.flatten.map {|v| v.to_s.sub(/:.*$/, '')}.find_all {|v| v !~ /^\d+$/}
    _dbh = self.dbh
    sql = sprintf "SELECT queue_wait(%s)", Array.new(*args.flatten.length) { '?' }.join(',')
# prepare/execute/fetch_arrayいっきにやるメソッドがあるかどうか -> TODO:調べる
    sth = _dbh.prepare sql
    sth.execute *args.flatten
    index = sth.fetch[0]
    sth.finish
    table = !index.nil? && index > 0 ? tables[index - 1] : nil
    res = Q4M::Result.new :rv => !table.nil?, :table => table, :on_release => proc{self.__table = nil}
    unless table.nil? 
      @__table = table
    end
    @__res = res if res
    @owner_mode = 1
#    res
    table.nil? ? nil : res
  end

  def queue_end
    _dbh.do "SELECT queue_end();"
  end

  # v 複数ワーカー指定可能
  # v ワーカー１つで複数キュー指定時は優先度キューとして動作
  # v 複数ワーカーで複数キュー指定時
  def start_worker(*workers)
    worker_instances = workers.map {|w| w.new}
    handlers = {}
    worker_instances.each do |wi|
      if wi.queue.instance_of? String || (wi.queue.instance_of? Array && wi.queue.length == 1)
        handlers.merge!({wi.queue => wi})
      else
        handlers.merge!(Hash[*wi.queue.map {|q| [q, wi]}.flatten])
      end
    end
    queues = [worker_instances.map{|w| w.queue}.flatten]
    loop do
      table = self.next(queues, 10)
      if table
        table = table.to_s
        result = self.fetch_hash table
        handlers[table].__send__ 'work', result, table
        self.queue_end
      end
      sleep 1
    end
  end

  %w[array hash].each do |m|
    module_eval %{
      def fetch_#{m}(table=nil, *rst)
        table ||= @__table
        if !table.nil? && table.instance_of?(Q4M::Result)
          table = table.table
        end

        # TODO to_bみたいなboolean比較メソッドを定義する必要あり nilチェック？
        table or raise 'no table'

        # TODO SQL::Abstractみたいの
        cols = rst.length == 0 ? '*' : rst.join(',')
        sql = "SELECT " + cols + " FROM " + table
        logger.debug sql
        logger.debug *rst
        sth = dbh.prepare sql
        @owner_mode = 0
        sth.execute
        result = sth.__send__ "fetch_#{m}"
        
        # m == hash時のみ
        result = ::Q4M.StructFromHash result if result.instance_of? Hash
        sth.finish
        result
      end
    }
  end

  def fetch(*args)
    fetch_array *args
  end

  def insert(table, hash)
    sql = "INSERT INTO #{table} (#{hash.keys.join(',')}) VALUES (#{hash.keys.map{|v| '?'}.join(',')})"
    logger.debug sql
    sth = dbh.prepare sql
    # sth.executeは戻り値nilなのでほかの方法で結果を確認したい perl版とあわせるなら
    sth.execute *hash.values
    sth.finish
  end

  def clear(table)
    dbh.do "DELETE FROM #{table}"
  end

  def status
    ::Q4M::Status.fetch self.dbh
  end
end
