require 'json'
require 'nats-async'

class NatsAsyncRuntime
  BACKEND_MODES = %i[auto core jetstream].freeze
  DEFAULT_MAX_PAYLOAD = 1_048_576
  DEFAULT_JS_API_PREFIX = '$JS.API'.freeze

  attr_reader :backend_mode, :backend, :boot_error, :client

  def initialize(url, options = {})
    @url = url
    @headers = options.delete(:headers)
    @backend_mode = options.delete(:backend_mode).to_s.downcase.to_sym
    @js_api_prefix = options.delete(:js_api_prefix) || DEFAULT_JS_API_PREFIX
    raise ArgumentError, "Invalid NATS_MODE. Allowed: #{BACKEND_MODES.join(', ')}" unless BACKEND_MODES.include?(@backend_mode)

    @client = nil
    @backend = nil
    @started = false
    @ready = false
    @boot_error = nil
  end

  def start(task:, stream:)
    return self if ready?

    @client ||= build_client
    @client.start(task:)
    @backend = @client.resolve_backend(mode: @backend_mode, stream:)
    @started = true
    @ready = true
    @boot_error = nil
    self
  rescue => e
    @boot_error = e
    @ready = false
    safe_client_close
    @client = nil
    raise
  end

  def close
    return true unless @client

    safe_client_close
    @client = nil
    @ready = false
    @started = false
    true
  end

  def started? = @started

  def ready? = @ready && !@client.nil?

  def failed? = !@boot_error.nil?

  def publish(message, subject, reply = nil, raw: true, headers: nil)
    ensure_ready!

    payload = serialize_message(message)
    merged_headers = merge_headers(headers)
    return @client.publish(subject, payload, reply:, headers: merged_headers) if raw

    @client.jetstream.publish(subject, payload, headers: merged_headers)
  end

  def subscribe(subject, queue: nil, handler: nil, &block)
    ensure_ready!
    @client.subscribe(subject, queue:, handler:, &block)
  end

  def unsubscribe(sid)
    ensure_ready!
    @client.unsubscribe(sid)
  end

  def jetstream
    ensure_ready!
    @client.jetstream
  end

  def max_payload
    ensure_ready!

    value = @client.server_info&.dig(:max_payload)
    value.to_i.positive? ? value.to_i : DEFAULT_MAX_PAYLOAD
  rescue
    DEFAULT_MAX_PAYLOAD
  end

  def connection_snapshot
    return closed_snapshot unless @client

    {
      status: @client.status,
      connected: @client.connected?,
      disconnected: !@client.closed? && !@client.connected?,
      closed: @client.closed?,
      draining: false,
      last_error: @client.last_error,
      server_info: @client.server_info,
      sent_pings: @client.sent_pings,
      received_pings: @client.received_pings,
      received_pongs: @client.received_pongs,
      max_payload: max_payload
    }
  rescue => e
    closed_snapshot.merge(last_error: e)
  end

  def jetstream_info(stream:, consumer:)
    ensure_ready!

    {
      stream_info: @client.jetstream.stream_info(stream),
      consumer_info: @client.jetstream.consumer_info(stream, consumer)
    }
  end

  private

  def build_client
    NatsAsync::Client.new(
      url: @url,
      verbose: false,
      js_api_prefix: @js_api_prefix,
      reconnect: true,
      reconnect_interval: 1
    )
  end

  def ensure_ready!
    raise 'NATS runtime is not ready' unless ready?
  end

  def safe_client_close
    @client&.close
  rescue => e
    LOGGER.warn "NATS runtime client close failed: #{e.class} - #{e.message}"
  end

  def merge_headers(headers)
    merged = {}
    merged.merge!(@headers) if @headers.is_a?(Hash)
    merged.merge!(headers) if headers.is_a?(Hash)
    merged.empty? ? nil : merged
  end

  def serialize_message(message)
    return message if message.is_a?(String)
    return '' if message.nil?

    JSON.generate(message)
  end

  def closed_snapshot
    {
      status: :closed,
      connected: false,
      disconnected: false,
      closed: true,
      draining: false,
      last_error: nil,
      server_info: nil,
      sent_pings: 0,
      received_pings: 0,
      received_pongs: 0,
      max_payload: DEFAULT_MAX_PAYLOAD
    }
  end
end
