require 'nats/client'

STACK_NAME = ENV['STACK_NAME'] || 'stack_default'
STACK_SERVICE_NAME = ENV['STACK_SERVICE_NAME'] || 'service_default'

DURABLE_PREFIX = "#{STACK_NAME}_#{STACK_SERVICE_NAME}"
$retranslators = []
def Retranslate( *opts, &block)
  RetranslateConfig.new(name, opts).tap do |cfg|
    cfg.instance_eval(&block) if block
    $retranslators << cfg
  end
end

class RetranslateConfig
  attr_accessor :nats_url, :opts

  def initialize(opts)
    @opts = {}
    opts.first.each { |k, v| send(k,v) }
  end

  def method_missing(method_name, args) @opts[method_name.to_s] = args; end

  private

  def start
    @up_conn = NATS.connect(@upstream_url)
    @js_up = NATS::IO::JetStream::Context.new(@up_conn)
    puts "Connected to upstream JetStream: #{@upstream_url}, stream: #{@from}"
    @down_conn = NATS.connect(@nats_url)
    @js_down = NATS::IO::JetStream::Context.new(@down_conn)
    puts "Connected to downstream JetStream: #{@nats_url}"

    durable = "#{DURABLE_PREFIX}_relay"
    @js_up.subscribe(@opts[:from], durable: , ack: :explicit) do |msg|
      target_subject = opts[:subject_prefix].to_s.empty? ?
                         msg.subject : "#{opts[:subject_prefix]}.#{msg.subject}"
      @js_down.publish(target_subject, msg.data)
      msg.ack
      puts "[#{target_subject}] Relayed: #{msg.data[0..50]}..."
    end
  end

end

Retranslate nats_url: 'nats://localhost:4222', upstream_url: 'nats://localhost:4222', from: 'notam_aftn' do
  subject_prefix 'external'
end


def start_retranslator
  $retranslators.each(&:start)
  sleep
end
