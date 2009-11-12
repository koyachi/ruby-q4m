# -*- coding: utf-8 -*-

# TODO
# - dbi, prepare/execute/fetch_arrayを一行で実行したい
#   - Q4M::Q4M#next
#   - 
# - モジュール、クラス名の整備、特にQ4M::Q4M
# - Q4M::Resultが適当なのでどうにかする
#   - boolean
#   - 
#   - 
# - ruby風にeachとか yield? -> EMのループをまねするといいかも
# - stacktrace, perlのCarp confessみたいの
# - TEST!!!!
# - 

require 'dbi'
require 'logger'

module Q4M
  def self.StructFromHash(hash)
    Struct.new(*hash.keys.map{|k| k.to_sym}).new(*hash.values.map{|s| Hash === s ? ::Q4M.StructFromHash(s) : s})
  end
end

DIR = File.expand_path(File.dirname(File.expand_path(__FILE__)))
$:.unshift DIR
%w[loggable q4m result status].each do |m|
  require "q4m/#{m}"
end

module Q4M
  Q4M_MINIMUM_VERSION = 0.8

  @logger = Logger.new(STDOUT)
  @logger.level = Logger::INFO

  def logger
    self.class.logger
  end

  class << self
    include ::Q4M::Loggable
    attr_accessor :dbh, :owner_mode, :instance
    attr_accessor :logger
    @instance = nil

    class Error < StandardError; end
    class DoesNotMeetRequiredQ4MVersion < Error; end

    def connect(args)
      unless self.instance
        self.instance = ::Q4M::Q4M.new args
      end
      if old = self.instance._dbh
        begin
          old.disconnect
        rescue DBI::InterfaceError => e
        end
      end
      dbh = self.instance._connect
      self.owner_mode = self.instance.owner_mode

      begin
        sth = dbh.prepare <<-'EOSQL'
          SELECT PLUGIN_VERSION from  information_schema.plugins
          WHERE plugin_name = ?
        EOSQL
        sth.execute 'QUEUE'
        version = sth.fetch[0].to_f
        sth.finish
      rescue Exception => e
        logger.info(e)
      end
      if !version || version < ::Q4M::Q4M_MINIMUM_VERSION
        # Carp.confessみたいにスタックとレース出すやつあったっけ？
        #      raise "Connected database does not meet the minimum required q4m version(#{Q4M_MINIMUM_VERSION}). Got version #{version || '(undef)'}"
        raise DoesNotMeetRequiredQ4MVersion.new(version)
      end
      self.instance
    end

    def disconnect
      self.instance.disconnect
    end
  end
end

def Q4M(*args)
  Q4M.connect args
end
