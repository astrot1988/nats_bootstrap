require 'nats/client'
require_relative 'retranslator'

StreamConfig = NATS::JetStream::API::StreamConfig
def JetStream( name, *opts, &block)
  Config.new(name, opts).tap do |cfg|
    cfg.instance_eval(&block) if block
    cfg.apply
  end
end

class Config
  attr_accessor :nats_url, :opts

  def initialize(name, opts)
    @opts = { name:}
    opts.first.each { |k, v| send(k,v) }
    @config = StreamConfig.new @opts
  end

  def method_missing(method_name, args) @opts[method_name.to_s.gsub(/_/,'-')] = args; end
  def nats_url(val);     @nats_url = val; end
  def subjects(val);     @opts[:subjects] = Array(val).flatten; end

  def apply
    puts 'Apply'
    connect
    @jetstream = @conn.jetstream
    ex = @jetstream.stream_info(@config[:name]) rescue nil
    ex ? update_stream : create_stream
    puts 'Done'
  end

  private

  def connect
    puts "Connecting to NATS at #{@nats_url}..."
    @conn = NATS.connect(@nats_url)
  rescue
    raise "Error connecting to NATS server: #{@config[:name]}"
  end

  def create_stream
    puts "Creating stream #{@config[:name]}..."
    @jetstream.add_stream(@config)
  end

  def update_stream
    # TODO: Compare existing stream with new config and update if necessary

    puts "Updating stream #{@config[:name]}..."
    @jetstream.update_stream @config
  end

end

load './config.rb'
start_retranslator
