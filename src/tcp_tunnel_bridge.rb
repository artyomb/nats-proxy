require 'json'
require 'async'
require 'io/endpoint/host_endpoint'
require 'socket'
require 'securerandom'
require 'uri'
require_relative 'bridge_protocol'

class TcpTunnelBridge
  DEFAULT_CHUNK_SIZE = 32_768

  def initialize(
    core:,
    nats_client:,
    nats_backend:,
    service_id:,
    nats_response_timeout:,
    stream_response_timeout:
  )
    @core = core
    @nats_client = nats_client
    @nats_backend = nats_backend
    @service_id = service_id
    @nats_response_timeout = nats_response_timeout
    @stream_response_timeout = stream_response_timeout
    max_payload = nats_client.respond_to?(:max_payload) ? nats_client.max_payload.to_i : 1_048_576
    max_payload = 1_048_576 if max_payload <= 0
    @chunk_size = [max_payload / 2, DEFAULT_CHUNK_SIZE].min
  end

  def dispatch_connect_request(env:)
    host_port = env['REQUEST_URI'] || env['HTTP_HOST'] || env['PATH_INFO']
    host, port = parse_connect_target(host_port)

    unless host && port.positive?
      return [400, { 'content-type' => 'text/plain' }, ['Invalid CONNECT target']]
    end

    unless @core.bridge_outbound?
      return [503, { 'content-type' => 'text/plain' }, ['Bridge not available']]
    end

    session_id = SecureRandom.hex(16)
    context = @core.bridge_session_open(
      session_id: session_id,
      payload: {
        'host' => host,
        'port' => port,
        'ingress_kind' => 'http_connect',
        'method' => 'CONNECT'
      }
    )

    established = BridgeProtocol.wait_for_session_established(
      context.response_queue, timeout: @nats_response_timeout
    )

    unless established
      @core.release_pending(session_id)
      return [504, { 'content-type' => 'text/plain' }, ['Session establishment timeout']]
    end

    context.receiver_service_id = established['receiver_service_id']
    hijack = env['rack.hijack']
    unless hijack.respond_to?(:call)
      @core.close_session(session_id, reason: 'hijack_not_supported', receiver_service_id: context.receiver_service_id)
      @core.release_pending(session_id)
      return [501, { 'content-type' => 'text/plain' }, ['CONNECT tunneling requires rack.hijack support']]
    end

    headers = {
      'content-type' => 'application/octet-stream',
      'rack.hijack' => proc do |io|
        run_requester_tunnel(io:, context:, session_id:)
      end
    }
    [200, headers, []]
  rescue BridgeProtocol::ProtocolError => e
    LOGGER.error "Session protocol error: error=#{e.message}"
    @core.release_pending(session_id) if session_id
    [502, { 'content-type' => 'text/plain' }, ["Session failed: #{e.message}"]]
  end

  def run_requester_tunnel(io:, context:, session_id:)
    with_async_task do |task|
      pump_requester_tunnel(task, io, context, session_id)
    end
  end

  def handle_stream_request(
    payload:,
    cancel_check:,
    emit_failure_response:,
    request_id:,
    upstream_queue:,
    detached: false,
    task_parent: nil,
    on_complete: nil,
    &emit
  )
    host = payload['host'].to_s
    port = payload['port'].to_i
    requester_service_id = payload['requester_service_id'].to_s
    socket_detached = false
    task_parent ||= Async::Task.current? if detached

    if host.empty? || port <= 0
      raise BridgeProtocol::InvalidRequestError, 'Missing or invalid host/port for tcp_stream'
    end

    if requester_service_id.empty?
      raise BridgeProtocol::InvalidRequestError, 'Missing requester_service_id for tcp_stream'
    end

    unless upstream_queue
      raise BridgeProtocol::InvalidRequestError, 'Missing upstream_queue for tcp_stream session'
    end

    if detached && !task_parent
      raise BridgeProtocol::UpstreamUnavailableError, 'Missing async task parent for tcp_stream session'
    end

    socket = connect_target(host, port)
    unless socket
      error_msg = "Failed to connect to #{host}:#{port}"
      if emit_failure_response
        emit.call(
          'type' => BridgeProtocol::RESPONSE_START,
          'status' => 502,
          'headers' => { 'content-type' => 'text/plain' },
          'content_type' => 'text/plain',
          'streaming' => false
        )
        emit.call(BridgeProtocol.chunk_event(error_msg))
        emit.call(BridgeProtocol.end_event)
        return BridgeProtocol::OUTCOME_UPSTREAM_ERROR
      end

      raise BridgeProtocol::UpstreamUnavailableError, error_msg
    end

    bind_host, bind_port = local_bind_address(socket)
    emit.call(
      BridgeProtocol.session_established_event(
        session_id: request_id,
        bind_host: bind_host,
        bind_port: bind_port,
        ingress_kind: payload['ingress_kind']
      )
    )
    LOGGER.info "Session established: session_id=#{request_id}, target=#{host}:#{port}, backend=#{@nats_backend}"

    if detached
      socket_detached = true
      task_parent.async(annotation: "tcp-session-#{request_id}") do |session_task|
        outcome = pump_receiver_tunnel(session_task, socket, upstream_queue, cancel_check, request_id, requester_service_id, &emit)
        on_complete&.call(outcome)
      rescue Async::Stop
        raise
      rescue => e
        LOGGER.error "Detached session failed: session_id=#{request_id}, error=#{e.class} - #{e.message}"
        emit.call(BridgeProtocol.error_event("Tunnel error: #{e.message}"))
        on_complete&.call(BridgeProtocol::OUTCOME_UPSTREAM_ERROR)
      ensure
        socket&.close rescue nil
      end
      return BridgeProtocol::OUTCOME_DETACHED
    end

    with_async_task do |task|
      pump_receiver_tunnel(task, socket, upstream_queue, cancel_check, request_id, requester_service_id, &emit)
    end
  ensure
    socket&.close unless socket_detached rescue nil
  end

  private

  def with_async_task
    task = Async::Task.current?
    return yield(task) if task

    Sync do |root|
      yield(root)
    end
  end

  def pump_receiver_tunnel(task, socket, upstream_queue, cancel_check, session_id, requester_service_id)
    stop = false

    reader = task.async do
      loop do
        break if stop || cancel_check.call

        chunk = read_chunk(socket)
        break unless chunk
        next if chunk.empty?

        @core.send_session_downstream(session_id, requester_service_id, chunk)
      end
    rescue IOError, Errno::ECONNRESET, Errno::EPIPE => e
      LOGGER.info "Session target socket closed: session_id=#{session_id}, error=#{e.class}"
    ensure
      stop = true
      socket.close rescue nil
    end

    writer = task.async do
      loop do
        break if stop || cancel_check.call

        data = upstream_queue.dequeue(timeout: 0.2)
        next if data.nil?
        break if data == :session_close

        write_all(socket, data)
      end
    rescue IOError, Errno::ECONNRESET, Errno::EPIPE => e
      LOGGER.info "Session target write failed: session_id=#{session_id}, error=#{e.class}"
    ensure
      stop = true
      socket.close rescue nil
    end

    reader.wait
    writer.wait

    reason = cancel_check.call ? 'cancel_requested' : 'target_closed'
    yield BridgeProtocol.session_close_event(reason:)
    cancel_check.call ? BridgeProtocol::OUTCOME_CANCELED : BridgeProtocol::OUTCOME_COMPLETED
  rescue => e
    LOGGER.error "Session tunnel error: session_id=#{session_id}, error=#{e.class} - #{e.message}"
    yield BridgeProtocol.error_event("Tunnel error: #{e.message}")
    BridgeProtocol::OUTCOME_UPSTREAM_ERROR
  end

  def connect_target(host, port)
    IO::Endpoint.tcp(host, port).connect
  rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT, SocketError => e
    LOGGER.error "TCP connect failed: #{host}:#{port}, error=#{e.class} - #{e.message}"
    nil
  end

  def pump_requester_tunnel(task, client_io, context, session_id)
    stop = false

    reader = task.async do
      loop do
        break if stop

        bytes = read_chunk(client_io)
        break unless bytes
        next if bytes.empty?

        @core.send_session_data(session_id, bytes, receiver_service_id: context.receiver_service_id)
      end
      @core.close_session(session_id, reason: 'client_closed', receiver_service_id: context.receiver_service_id)
    rescue IOError, Errno::ECONNRESET, Errno::EPIPE
      @core.close_session(session_id, reason: 'client_disconnected', receiver_service_id: context.receiver_service_id)
    ensure
      stop = true
    end

    writer = task.async do
      outcome = tunnel_writer_loop(client_io, context)

      case outcome
      when :timeout
        @core.cancel_request(context, reason: 'stream_timeout')
      when Hash
        @core.cancel_request(context, reason: 'upstream_error')
      end
    rescue IOError, Errno::ECONNRESET, Errno::EPIPE
      @core.cancel_request(context, reason: 'downstream_disconnect')
    ensure
      stop = true
    end

    writer.wait
    reader.stop
  rescue => e
    LOGGER.error "Requester tunnel error: session_id=#{session_id}, error=#{e.class} - #{e.message}"
  ensure
    client_io.close rescue nil
    @core.release_pending(session_id)
    LOGGER.info "Requester tunnel closed: session_id=#{session_id}"
  end

  def tunnel_writer_loop(client_io, context)
    idle_deadline = Time.now + @stream_response_timeout

    loop do
      chunk = context.tunnel_data_queue&.dequeue(timeout: 0.1)
      if chunk
        write_all(client_io, chunk)
        idle_deadline = Time.now + @stream_response_timeout
        next
      end

      control = context.response_queue.dequeue(timeout: 0)
      if control
        idle_deadline = Time.now + @stream_response_timeout

        case control['type']
        when BridgeProtocol::SESSION_CLOSE
          return :finished
        when BridgeProtocol::RESPONSE_ERROR
          return control
        else
          raise BridgeProtocol::ProtocolError, "Unexpected tunnel control event #{control['type']}"
        end
      end

      return :timeout if Time.now >= idle_deadline
    end
  end

  def local_bind_address(socket)
    addr = socket.local_address
    [addr.ip_address, addr.ip_port]
  rescue StandardError
    [nil, nil]
  end

  def parse_connect_target(host_port)
    value = host_port.to_s.strip
    return [nil, 0] if value.empty?

    if value.include?('://')
      uri = URI.parse(value)
      host = uri.host.to_s
      port = uri.port || 443
      return [host.empty? ? nil : host, port.to_i]
    end

    host, port_text = value.split(':', 2)
    return [nil, 0] if host.to_s.empty?

    port = port_text.to_i
    port = 443 if port <= 0
    [host, port]
  rescue URI::InvalidURIError
    [nil, 0]
  end

  def read_chunk(io)
    io.readpartial(@chunk_size)
  rescue EOFError
    nil
  end

  def write_all(io, data)
    io.write(data.to_s.b)
  end
end
