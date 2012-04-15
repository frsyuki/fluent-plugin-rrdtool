module Fluent


class RRDToolOutput < Fluent::ObjectBufferedOutput
  Fluent::Plugin.register_output('rrdtool', self)

  require "#{File.dirname(__FILE__)}/out_rrdtool_mixin"
  include MetricsMixin

  class Aggregator
  end

  # max(value) aggregator
  class MaxAggregator < Aggregator
    def initialize
      @value = 0
    end

    def add(value)
      @value = value if @value < value
    end

    def get
      @value
    end
  end

  # sum(value) aggregator
  class SumAggregator < Aggregator
    def initialize
      @value = 0
    end

    def add(value)
      @value += value
    end

    def get
      @value
    end
  end

  # avg(value) aggregator
  class AverageAggregator < Aggregator
    def initialize
      @value = 0
      @num = 0
    end

    def add(value)
      @value += value
      @num += 1
    end

    def get
      @value / @num
    end
  end

  config_param :time_slice_interval, :time, :default => 60
  config_param :buffer_queue_limit, :integer, :default => 5
  config_param :http_port, :integer, :default => 9876
  config_param :http_bind, :integer, :default => '0.0.0.0'
  config_param :archive_path, :string, :default => '/tmp/fluent-plugin-rrdtool/archive'

  def initialize
    super
    require 'librrd'
    @mutex = Mutex.new
  end

  def configure(conf)
    super

    @time_slice_interval = @time_slice_interval.to_i

    @rrb = RoundRobinBuffer.new(@time_slice_interval, @buffer_queue_limit)

    FileUtils.mkdir_p(@archive_path)

    configure_graphs(metrics)
  end

  def start
    super
    start_api_server(@http_bind, @http_port)
  end

  def shutdown
    super
    shutdown_api_server
  end

  SUPPORTED_METHOD = {
    'max' => MaxAggregator,
    'average' => AverageAggregator,
    'sum' => SumAggregator,
  }

  class RRDToolMetrics < Metrics
    config_param :method, :string, :default => 'max'
    attr_accessor :method_class
    config_param :graph, :string, :default => nil

    def configure(conf)
      super

      if method_class = SUPPORTED_METHOD[@method]
        @method_class = method_class
      else
        raise ConfigError, "Unsupported aggregation method '#{@method}'"
      end
    end
  end

  # override
  def new_metrics(conf, pattern)
    m = RRDToolMetrics.new(pattern)
    m.configure(conf)
    m
  end

  class RoundRobinBuffer
    def initialize(time_slice_interval, buffer_queue_limit)
      @time_slice_interval = time_slice_interval
      @buffer_queue_limit = buffer_queue_limit

      @array = []
      @ts_offset = 0

      @complete = {}
    end

    attr_reader :complete

    def [](time)
      ts = time / @time_slice_interval

      index = ts - @ts_offset
      return if index < 0  # TODO log?

      over = index - @buffer_queue_limit
      if over > 0
        @array.slice!(0, over).each_with_index {|chunk,i|
          $log.trace "complete: #{(@ts_offset+i)*@time_slice_interval}: #{chunk.inspect}"
          @complete[(@ts_offset+i)*@time_slice_interval] = chunk if chunk
        }
        @ts_offset += over
        index -= over
      end

      return (@array[index] ||= Hash.new)
    end
  end

  def emit(tag, es, chain)
    stream = nil
    @mutex.synchronize do
      stream = update_buffer(tag, es)
    end
    if stream
      super("", stream, chain)
    else
      chain.next
    end
  end

  def ensure_flush_buffer
    @mutex.synchronize do
      @rrb[Engine.now]
    end
  end

  def update_buffer(tag, es)
    es.each {|time,record|
      chunk = @rrb[time]

      each_metrics(tag, time, record) {|d|
        value = d.value * d.count
        source = d.keys.join('+') unless d.each_keys.empty?
        key = encode_key(d.metrics.method, d.name, source)

        aggr = (chunk[key] ||= d.metrics.method_class.new)
        aggr.add(value)
      }
    }

    if @rrb.complete.empty?
      return nil
    else
      stream = Fluent::MultiEventStream.new

      @rrb.complete.each_pair {|time,chunk|
        record = {}
        chunk.each_pair {|key,aggr|
          record[key] = aggr.get
        }
        stream.add(time, record)
      }

      @rrb.complete.clear

      return stream
    end
  end

  RRD_CF_NAME = {
    'max' => 'MAX',
    'average' => 'AVERAGE',
    'sum' => 'MAX',
  }

  RRD_LINE_COLORS = []
  safe = %w[00 33 66 99 cc ff]
  safe.each {|r|
    safe.each {|g|
      safe.each {|b|
        next if r == g && g == b
        luminance = 0.298912*r.to_i + 0.586611*g.to_i + 0.114478*b.to_i
        next if luminance < 20
        next if luminance > 230
        RRD_LINE_COLORS << "#{r}#{g}#{b}"
      }
    }
  }
  r = Random.new(3)
  RRD_LINE_COLORS.sort_by! {|x| r.rand }

  MAX_RRA_RECORDS = 300

  def configure_graphs(metrics)
    graphs = {}
    metrics.each {|m|
      title = m.graph || m.name
      g = (graphs[title] ||= GraphDesc.new(title))
      g.add_line(m.method, m.name, !m.each_keys.empty?)
    }
    @graphs = graphs.values
  end

  class GraphDesc < Struct.new(:title, :lines)
    class LineDesc < Struct.new(:method, :name, :has_source)
      alias has_source? has_source
    end

    def initialize(title)
      super(title, [])
    end

    def add_line(method, name, has_source)
      lines << LineDesc.new(method, name, has_source)
    end
  end

  def write_objects(key, chunk)
    chunk.each {|time,record|
      record.each_pair {|key,value|
        add_value(key, time, value)
      }
    }
  end

  def list_graphs
    graphs = {}

    @graphs.each {|g|
      lines = []
      g.lines.each {|ln|
        rrd_search_files(ln.method, ln.name) {|path,source|
          lines << [path, ln.name, ln.method, source]
        }
      }
      graphs[g.title] = lines
    }

    graphs
  end

  def get_graph(title, only_source)
    lines = []

    lines = list_graphs[title]
    return nil unless lines

    now = Engine.now

    if only_source
      path = "#{title}:#{only_source}.png"
    else
      path = "#{title}.png"
    end
    args = [
      path,
      "--title", "#{title}",
      "--start", "#{now-@time_slice_interval*MAX_RRA_RECORDS}",
      "--end", "#{now}",
      "--imgformat", "PNG",
      "--width=500",
    ]

    none = true
    lines.each_with_index {|(path,name,method,source),i|
      if only_source && only_source != source
        next
      end

      path = path.gsub(':', "\\:")
      if source
        legend = "#{name}:#{source}".gsub(':', "\\:")
      else
        legend = "#{name}".gsub(':', "\\:")
      end
      color = RRD_LINE_COLORS[i % RRD_LINE_COLORS.length]
      cf = RRD_CF_NAME[method]
      args << "DEF:value#{i}=#{path}:value:#{cf}"
      args << "AREA:value#{i}##{color}:#{legend}"

      none = false
    }

    if none
      nil
    else
      RRD.graph(*args)
      return File.read(path) rescue nil
    end
  end

  def add_value(key, time, value)
    $log.trace { "#{key}: #{time}: #{value.inspect}" }

    method, name, source = decode_key(key)

    path = rrd_path(key)
    unless File.exists?(path)
      begin
        create_rrd(method, path, time)
      rescue
        $log.error $!.to_s
        $log.error_backtrace
        File.unlink(path)
        return
      end
    end

    begin
      RRD.update(path, "-t", "value", "#{time}:#{value}")
    rescue
      $log.error $!.to_s
      $log.error_backtrace
      File.unlink(path)
    end
  end

  def create_rrd(method, path, time)
    unknown_limit = @time_slice_interval * 10
    cf = RRD_CF_NAME[method]
    RRD.create(
      path,
      "--start", "#{time-@time_slice_interval}",
      "--step", "#{@time_slice_interval}",
      "DS:value:GAUGE:#{unknown_limit}:U:U",
      "RRA:#{cf}:0.5:1:#{MAX_RRA_RECORDS}"
    )
  end

  def rrd_path(key)
    "#{@archive_path}/#{key}.rrd"  # TODO escape
  end

  def rrd_search_files(method, name, &block)
    r = []
    Dir.entries(@archive_path).each {|e|
      if m = /#{method}:#{name}(?:\:(.*?))?.rrd/.match(e)
        yield "#{@archive_path}/#{e}", $~[1]
      end
    }
  end

  def encode_key(method, name, source)
    # TODO escape
    if source
      "#{method}:#{name}:#{source}"
    else
      "#{method}:#{name}"
    end
  end

  def decode_key(key)
    method, name, source = key.split(':', 3)
    return method, name, source
  end


  require 'webrick'

  def start_api_server(bind, port)
    options = {
      :DocumentRoot => File.dirname(__FILE__)+'/static',
      :BindAddress => bind,
      :Port => port,
    }
    @api_server = WEBrick::HTTPServer.new(options)
    @api_server.mount('/', APIServlet, self)
    @api_server_thread = Thread.new {
      @api_server.start
    }
  end

  def shutdown_api_server
    @api_server.stop
    @api_server_thread.join
  end

  class APIServlet < WEBrick::HTTPServlet::AbstractServlet
    def initialize(server, parent)
      super(server)
      @parent = parent
    end

    def do_GET(req, res)
      @parent.do_GET(req, res)
    end
  end

  def do_GET(req, res)
    ensure_flush_buffer

    if /^\/graph\/([^\/]+)(?:\/([^\/]+))?$/.match(req.path)
      # graph/:title
      title = $~[1]
      source = $~[2]
      if png = get_graph(title, source)
        res.body = png
        res.content_type = 'image/png'
      else
        res.status = 404
        res.body = "Not found"
        res.content_type = 'text/plain'
      end

    elsif /^\/list\/([^\/]+)$/.match(req.path)
      # list/:source
      only_source = $~[1]
      graphs = list_graphs
      titles = []
      graphs.each_pair {|title,lines|
        if lines.find {|path,name,method,source| source == only_source }
          titles << title
        end
      }

      body = %[<body>]
      titles.each {|title|
        body << %[<img src="/graph/#{title}/#{only_source}"></img>]
      }
      body << %[</body>]

      res.body = body
      res.content_type = 'text/html'

    elsif '/' == req.path
      # index
      graphs = list_graphs
      titles = graphs.keys
      sources = graphs.values.map {|lines|
        lines.map {|path,name,method,source| source }
      }.flatten.compact.uniq.sort

      body = %[<body>]

      titles.each {|title|
        body << %[<img src="/graph/#{title}"></img>]
      }

      sources.each {|source|
        body << %[<p><a href="/list/#{source}">#{source}</a></p>]
      }

      body << %[</body>]

      res.body = body
      res.content_type = 'text/html'

    else
      res.status = 404
      res.body = "Not found"
      res.content_type = 'text/plain'

    end
  end
end


end

