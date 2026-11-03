require 'async/http/faraday'
require 'async/http/endpoint'
require 'io/endpoint/connected_endpoint'
require 'socket'
require 'timeout'

# Adapts the existing TCP session to Faraday without implementing HTTP framing.
class HttpTunnel
  class Error < StandardError
    attr_reader :status

    def initialize(message, status: 502)
      @status = status
      super(message)
    end
  end

  class Clients < Async::HTTP::Faraday::Clients
    def initialize(endpoint, **options)
      super(**options)
      @endpoint = endpoint
    end

    def make_client(_endpoint) = super(@endpoint)
  end

  attr_reader :connection, :url

  def initialize(core:, tcp_bridge:, request_id:, method:, path:, timeout:)
    @core = core
    @context = core.bridge_session_open(
      session_id: request_id,
      payload: { 'ingress_kind' => 'http_gateway', 'method' => method, 'path' => path }
    )
    event = @context.response_queue.pop(timeout:)
    raise Timeout::Error, "HTTP tunnel establishment timed out after #{timeout}s" unless event
    unless event['type'] == BridgeProtocol::SESSION_ESTABLISHED
      detail = "unexpected event #{event['type']}"
      if event['type'] == BridgeProtocol::RESPONSE_START
        body = BridgeProtocol.collect_non_streaming_body(@context.response_queue, timeout:)
        detail = body.to_s
      end
      raise Error.new("HTTP tunnel could not be established: #{detail}", status: event['status'].to_i >= 500 ? event['status'].to_i : 502)
    end

    @context.receiver_service_id = event.fetch('receiver_service_id')
    @url = event.fetch('http_url')
    core.send_session_credit_downstream(request_id, @context.receiver_service_id, core.flow_initial_window_bytes)
    @client_io, @tunnel_io = Socket.pair(:UNIX, :STREAM, 0)
    @pump = Async { tcp_bridge.run_requester_tunnel(io: @tunnel_io, context: @context, session_id: request_id) }

    transport = IO::Endpoint::ConnectedEndpoint.new(nil, @client_io)
    endpoint = Async::HTTP::Endpoint.parse(url, transport, protocol: Async::HTTP::Protocol::HTTP1)
    @connection = Faraday.new(url:, headers: event['http_headers'] || {}) do |f|
      f.options.timeout = 300
      f.adapter :async_http, clients: ->(**options) { Clients.new(endpoint, **options) }, retries: 1
    end
  rescue Exception
    close
    raise
  end

  def close
    return if @closed

    @closed = true
    begin
      if @context&.receiver_service_id
        @core.close_session(@context.request_id, reason: 'http_request_finished', receiver_service_id: @context.receiver_service_id)
      elsif @context
        @core.cancel_request(@context, reason: 'http_tunnel_failed')
      end
    rescue => e
      LOGGER.warn "HTTP tunnel close failed: #{e.class} - #{e.message}"
    ensure
      @client_io&.close unless @client_io&.closed?
      @tunnel_io&.close unless @tunnel_io&.closed?
      @pump&.stop
      @core.release_pending(@context.request_id) if @context
    end
  end
end
