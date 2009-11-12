module Q4M
  class Result
    attr_reader :rv, :table, :on_release

    def initialize(args)
      @rv = args[:rv]
      @table = args[:table]
      @on_release = args[:on_release]
      ObjectSpace.define_finalizer self, self.class.destroy(@on_release)
    end

    def self.destroy(on_release)
      proc do
        on_release.call
      end
    end

    def to_s
      @table
    end
  end
end
