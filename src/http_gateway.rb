require 'async'
require 'async/http/faraday'
require 'faraday'
require 'faraday/retry'
require 'json'
require 'securerandom'
require 'uri'
require_relative 'request_context'
require_relative 'bridge_protocol'
require_relative 'flow_credit_window'

class HttpGateway
  InvalidProxyTargetError = Class.new(ArgumentError)

  DownstreamDisconnectedError = Class.new(StandardError)

  STREAMING_MEDIA_TYPES = %w[text/event-stream application/x-ndjson].freeze
  HOP_BY_HOP_HEADERS = %w[
    connection
    keep-alive
    proxy-authenticate
    proxy-authorization
    te
    trailer
    transfer-encoding
    upgrade
  ].freeze
  NON_FORWARDABLE_REQUEST_HEADERS = (HOP_BY_HOP_HEADERS + %w[content-length host]).freeze

  def initialize(
    core:,
    upstream_url:,
    nats_backend:,
    service_id:,
    nats_response_timeout:,
    stream_response_timeout:,
    flow_initial_window_bytes: FlowCreditWindow.default_initial_bytes(chunk_size: 32_768),
    flow_credit_batch_bytes: FlowCreditWindow.default_batch_bytes(chunk_size: 32_768),
    flow_credit_wait_timeout: nil
  )
    @core = core
    @nats_backend = nats_backend
    @service_id = service_id
    @nats_response_timeout = nats_response_timeout
    @stream_response_timeout = stream_response_timeout
    @flow_initial_window_bytes = flow_initial_window_bytes.to_i
    @flow_credit_batch_bytes = flow_credit_batch_bytes.to_i
    @flow_credit_wait_timeout = (flow_credit_wait_timeout || stream_response_timeout).to_f
    @upstream = build_upstream_connection(upstream_url)
  end

  def receiver_capable? = !@upstream.nil?

  alias bridge_target_available? receiver_capable?

  def proxy_forward_request?(req)
    request_path = req.env['REQUEST_PATH'].to_s
    return true if protocol_absolute_form_target(req)

    proxy_absolute_form?(request_path) || !proxy_target_from_headers(req).to_s.empty?
  end

  def local_passthrough_path?(path)
    path.start_with?('/observability') || %w[/health /healthcheck].include?(path)
  end

  def dispatch_http_request(app:, method:)
    path = request_target(app.request)
    raw = request_body_for_method(app.request, method)
    headers = request_headers_from_env(app.request.env)
    body = parse_request_body(raw.to_s)

    context = if @core.bridge_outbound?
      @core.bridge_request(
        request_id: SecureRandom.hex(16),
        operation: 'http_request',
        payload: { 'method' => method, 'path' => path, 'headers' => headers, 'body' => body }
      )
    elsif receiver_capable?
      direct_upstream_request(method:, path:, headers:, body:)
    else
      return service_unavailable_response('No bridge or upstream available')
    end

    render_response(app:, context:)
  end

  def handle_bridge_request(payload:, cancel_check:, emit_failure_response:, request_id: nil, response_credit_window: nil)
    method = payload['method']
    path = payload['path']
    headers = payload['headers'] || {}
    body = payload['body']

    if method.to_s.empty? || path.to_s.empty?
      raise BridgeProtocol::InvalidRequestError, 'Missing method or path'
    end

    upstream_connection, upstream_path, = resolve_upstream_target(path, @upstream)

    unless upstream_connection
      error_message = 'Receiver is missing UPSTREAM_URL'
      if emit_failure_response
        yield build_start_event(status: 503, headers: { 'content-type' => 'application/json' })
        yield BridgeProtocol.chunk_event({ error: error_message }.to_json)
        yield BridgeProtocol.end_event
        return BridgeProtocol::OUTCOME_UPSTREAM_ERROR
      end
      raise BridgeProtocol::UpstreamUnavailableError, error_message
    end

    proxy_upstream_request(
      connection: upstream_connection,
      method:,
      path: upstream_path,
      headers:,
      body:,
      emit_failure_response:,
      cancel_check:,
      request_id:,
      response_credit_window:
    ) { |event| yield event }
  rescue InvalidProxyTargetError => e
    raise BridgeProtocol::InvalidRequestError, e.message
  end

  def proxy_upstream_request(connection:, method:, path:, headers: {}, body: nil, emit_failure_response: true, cancel_check: nil, request_id: nil, response_credit_window: nil)
    raise ArgumentError, 'block required' unless block_given?

    state = { start_event: nil, streaming_started: false, buffered_chunks: [] }
    request_headers = forwardable_request_headers(headers)

    response = connection.run_request(method.downcase.to_sym, path, nil, nil) do |req|
      request_headers.each { |key, value| req.headers[key] = value }

      if body
        req.body = body.is_a?(String) ? body : body.to_json
        req.headers['content-type'] ||= 'application/json' unless body.is_a?(String)
      end

      req.options.on_data = proc do |chunk, _bytes, env|
        raise BridgeProtocol::StreamCanceledError, 'stream canceled by cancel signal' if cancel_check&.call

        state[:start_event] ||= build_start_event(
          status: extract_status(env),
          headers: extract_headers(env)
        )

        if state[:start_event]['streaming']
          unless state[:streaming_started]
            yield state[:start_event]
            state[:streaming_started] = true
          end
          emit_stream_chunk(chunk, request_id:, response_credit_window:, cancel_check:) { |event| yield event } unless chunk.empty?
        else
          state[:buffered_chunks] << chunk unless chunk.empty?
        end
      end
    end

    state[:start_event] ||= build_start_event(status: response.status, headers: response.headers)

    unless state[:streaming_started]
      yield state[:start_event]

      body_chunk =
        if state[:buffered_chunks].empty?
          response.body.to_s
        else
          state[:buffered_chunks].join
        end

      yield BridgeProtocol.chunk_event(body_chunk) unless body_chunk.empty?
    end

    yield BridgeProtocol.end_event
    BridgeProtocol::OUTCOME_COMPLETED
  rescue BridgeProtocol::StreamCanceledError
    BridgeProtocol::OUTCOME_CANCELED
  rescue BridgeProtocol::FlowCreditTimeoutError
    if state[:streaming_started]
      yield BridgeProtocol.error_event('Response flow credit timeout')
      yield BridgeProtocol.end_event
    end
    BridgeProtocol::OUTCOME_TIMEOUT
  rescue Faraday::ConnectionFailed, Faraday::TimeoutError => e
    error_message = "Upstream unavailable: #{e.message}"

    if state[:streaming_started]
      yield BridgeProtocol.error_event(error_message)
      yield BridgeProtocol.end_event
    elsif emit_failure_response
      yield build_start_event(status: 503, headers: { 'content-type' => 'application/json' })
      yield BridgeProtocol.chunk_event({ error: error_message }.to_json)
      yield BridgeProtocol.end_event
    else
      raise BridgeProtocol::UpstreamUnavailableError, error_message
    end

    BridgeProtocol::OUTCOME_UPSTREAM_ERROR
  end

  def build_start_event(status:, headers:, extra: nil)
    normalized = forwardable_headers(headers, streaming: false)
    media_type = http_content_type(normalized)

    event = {
      'type' => BridgeProtocol::RESPONSE_START,
      'status' => status.to_i,
      'headers' => normalized,
      'content_type' => media_type,
      'streaming' => streaming_media_type?(media_type)
    }
    event.merge!(extra) if extra.is_a?(Hash)
    event
  end

  def format_stream_error(message, content_type:)
    error_json = { error: message.to_s }.to_json

    case content_type.to_s.downcase
    when /\Atext\/event-stream/
      "event: error\ndata: #{error_json}\n\n"
    when /\Aapplication\/x-ndjson/
      "#{error_json}\n"
    else
      error_json
    end
  end

  def forwardable_headers(headers, streaming: false)
    forwarded = BridgeProtocol.normalize_headers(headers).reject { |key, _| HOP_BY_HOP_HEADERS.include?(key) }
    forwarded.delete('content-length') if streaming
    forwarded
  end

  def forwardable_request_headers(headers)
    BridgeProtocol.normalize_headers(headers).each_with_object({}) do |(key, value), forwarded|
      next if NON_FORWARDABLE_REQUEST_HEADERS.include?(key)

      forwarded[key] = value.is_a?(Array) ? value.join(', ') : value
    end
  end

  private

  def proxy_absolute_form?(value)
    value.to_s.match?(/\Ahttps?:\/\//i)
  end

  def split_proxy_target(path)
    uri = URI.parse(path.to_s)
    raise InvalidProxyTargetError, 'Invalid proxy target URL' unless uri.is_a?(URI::HTTP) && uri.host

    base_url = "#{uri.scheme}://#{uri.host}"
    base_url = "#{base_url}:#{uri.port}" if uri.port && uri.port != uri.default_port
    [base_url, uri.request_uri]
  rescue URI::InvalidURIError
    raise InvalidProxyTargetError, 'Invalid proxy target URL'
  end

  def build_upstream_connection(url)
    return nil if url.to_s.empty?

    Faraday.new(url) do |f|
      f.request :retry, max: 2, interval: 0.2, backoff_factor: 2
      f.options.timeout = 300
      f.adapter :async_http
    end
  end

  def resolve_upstream_target(path, default_upstream)
    return [default_upstream, path, nil] unless proxy_absolute_form?(path)

    base_url, target_path = split_proxy_target(path)
    @proxy_connection_cache ||= {}
    connection = @proxy_connection_cache[base_url] ||= build_upstream_connection(base_url)
    [connection, target_path, path]
  end

  def streaming_media_type?(media_type)
    STREAMING_MEDIA_TYPES.any? { |type| media_type&.start_with?(type) }
  end

  def http_content_type(headers)
    value = headers['content-type']
    value = value.first if value.is_a?(Array)
    value&.split(';', 2)&.first&.strip&.downcase
  end

  def parse_request_body(raw_body)
    raw_body.empty? ? nil : (JSON.parse(raw_body) rescue raw_body)
  end

  def response_payload(status:, error:, message:)
    body = { error:, message: }.to_json
    [status, { 'content-type' => 'application/json', 'content-length' => body.bytesize.to_s }, [body]]
  end

  def gateway_timeout_response(timeout:)
    response_payload(status: 504, error: 'Gateway Timeout', message: "No response from bridge within #{timeout}s")
  end

  def gateway_protocol_error_response(message)
    response_payload(status: 502, error: 'Bad Gateway', message:)
  end

  def service_unavailable_response(message)
    response_payload(status: 503, error: 'Service Unavailable', message:)
  end

  def request_headers_from_env(env)
    headers = {}
    env.each do |key, value|
      next if value.nil?

      case key
      when 'CONTENT_TYPE'
        headers['content-type'] = value
      when 'CONTENT_LENGTH'
        headers['content-length'] = value
      when /\AHTTP_(.+)\z/
        header_name = Regexp.last_match(1).split('_').join('-').downcase
        headers[header_name] = value
      end
    end
    forwardable_request_headers(headers)
  end

  def request_target(req)
    request_path = req.env['REQUEST_PATH'].to_s
    return request_path if proxy_absolute_form?(request_path)

    protocol_target = protocol_absolute_form_target(req)
    return protocol_target if protocol_target

    proxy_target = proxy_target_from_headers(req)
    return proxy_target if proxy_target

    path = req.path_info.to_s
    query = req.query_string.to_s
    return path if query.empty? || path.include?('?')

    "#{path}?#{query}"
  end

  def proxy_target_from_headers(req)
    request_uri = req.env['REQUEST_URI'].to_s
    return request_uri if proxy_absolute_form?(request_uri)

    proxy_hint = req.env['HTTP_PROXY_CONNECTION'] || req.env['PROXY_CONNECTION']
    return nil if proxy_hint.to_s.empty?

    host = req.env['HTTP_HOST'].to_s
    return nil if host.empty?

    request_uri = req.path_info.to_s if request_uri.empty?
    return nil if proxy_absolute_form?(request_uri)

    request_uri = "/#{request_uri}" unless request_uri.start_with?('/')
    return nil if local_passthrough_path?(request_uri)

    scheme = req.env['rack.url_scheme'].to_s
    scheme = 'http' if scheme.empty?
    "#{scheme}://#{host}#{request_uri}"
  end

  def protocol_absolute_form_target(req)
    protocol_request = req.env["protocol.http.request"]
    return nil unless protocol_request

    target = if protocol_request.respond_to?(:request_target)
      protocol_request.request_target.to_s
    end
    return target if proxy_absolute_form?(target)

    absolute_form = protocol_request.respond_to?(:absolute_form_target) && protocol_request.absolute_form_target
    return nil unless absolute_form

    scheme = protocol_request.respond_to?(:scheme) ? protocol_request.scheme.to_s : "http"
    authority = protocol_request.respond_to?(:authority) ? protocol_request.authority.to_s : ""
    path = protocol_request.respond_to?(:path) ? protocol_request.path.to_s : ""
    return nil if authority.empty? || path.empty?

    "#{scheme}://#{authority}#{path}"
  end

  def request_body_for_method(request, method)
    return nil if %w[GET HEAD].include?(method)

    request.body.read
  end

  def direct_upstream_request(method:, path:, headers:, body:)
    request_id = SecureRandom.hex(16)
    context = RequestContext.new(request_id:)

    worker = Async do
      handle_bridge_request(
        payload: { 'method' => method, 'path' => path, 'headers' => headers, 'body' => body },
        cancel_check: -> { context.cancel_requested? },
        emit_failure_response: true
      ) do |event|
        context.response_queue.push(event)
      end
    rescue => e
      LOGGER.error "Direct response worker error: #{e.class} - #{e.message}"
      context.response_queue.push(build_start_event(status: 502, headers: { 'content-type' => 'application/json' }))
      context.response_queue.push(BridgeProtocol.chunk_event({ error: e.message }.to_json))
      context.response_queue.push(BridgeProtocol.end_event)
    end

    context.worker = worker
    context
  end

  def cancel_context(context, reason:)
    if context.request_subject
      @core.cancel_request(context, reason:)
    else
      state = context.request_cancel!(reason:)
      return false unless state == :ready

      context.worker&.stop
      context.mark_cancel_sent!
      LOGGER.info "Direct request canceled: request_id=#{context.request_id}, reason=#{reason}"
      true
    end
  end

  def apply_response_start_event(app, event)
    app.status event['status']
    forwardable_headers(event['headers'], streaming: event['streaming']).each do |key, value|
      app.response.headers[key] = value
    end
  end

  def release_context(context)
    @core.release_pending(context.request_id) if context.request_subject
    context.worker&.stop
  end

  def render_response(app:, context:)
    start_event = BridgeProtocol.wait_for_start_event(context.response_queue, timeout: @nats_response_timeout)

    unless start_event
      context.mark_terminal!(BridgeProtocol::OUTCOME_TIMEOUT)
      LOGGER.warn "Bridge start timeout: request_id=#{context.request_id}, timeout=#{@nats_response_timeout}, backend=#{@nats_backend}"
      return gateway_timeout_response(timeout: @nats_response_timeout)
    end

    context.receiver_service_id = start_event['receiver_service_id']

    if start_event['streaming']
      context.mark_streaming!
      apply_response_start_event(app, start_event)
      stream_content_type = start_event['content_type']
      disconnect_reason = nil
      grant_initial_response_credit(context)

      disconnect = lambda do |reason|
        next if disconnect_reason

        disconnect_reason = reason
        cancel_context(context, reason: 'downstream_disconnect')
        LOGGER.info "Downstream disconnected: request_id=#{context.request_id}, reason=#{reason}, backend=#{@nats_backend}"
      end

      app.stream(:keep_open) do |out|
        out.callback { disconnect.call('stream_callback') } if out.respond_to?(:callback)
        out.errback do |error = nil|
          reason = error ? "stream_errback_#{error.class}" : 'stream_errback'
          disconnect.call(reason)
        end if out.respond_to?(:errback)

        outcome = BridgeProtocol.each_stream_chunk(context.response_queue, timeout: @stream_response_timeout) do |chunk|
          raise DownstreamDisconnectedError, disconnect_reason if disconnect_reason

          begin
            out << chunk
            return_response_credit(context, chunk.bytesize)
          rescue IOError, Errno::EPIPE, Errno::ECONNRESET => e
            disconnect.call(e.class.to_s)
            raise DownstreamDisconnectedError, e.message
          end

          raise DownstreamDisconnectedError, disconnect_reason if disconnect_reason
        end

        case outcome
        when :timeout
          context.mark_terminal!(BridgeProtocol::OUTCOME_TIMEOUT)
          LOGGER.warn "Bridge stream timeout: request_id=#{context.request_id}, timeout=#{@stream_response_timeout}, backend=#{@nats_backend}"
          out << format_stream_error('Gateway Timeout', content_type: stream_content_type)
        when Hash
          context.mark_terminal!(BridgeProtocol::OUTCOME_UPSTREAM_ERROR)
          LOGGER.warn "Bridge stream error: request_id=#{context.request_id}, error=#{outcome['error']}, backend=#{@nats_backend}"
          out << format_stream_error(outcome['error'], content_type: stream_content_type)
        else
          context.mark_terminal!(BridgeProtocol::OUTCOME_COMPLETED)
        end
      rescue DownstreamDisconnectedError
        context.mark_terminal!(BridgeProtocol::OUTCOME_CANCELED)
      rescue BridgeProtocol::ProtocolError => e
        context.mark_terminal!(BridgeProtocol::OUTCOME_PROTOCOL_ERROR)
        LOGGER.error "Bridge streaming protocol error: request_id=#{context.request_id}, error=#{e.message}"
        out << format_stream_error(e.message, content_type: stream_content_type)
      ensure
        flush_response_credit(context)
        out.close
        LOGGER.info "Stream finalized: request_id=#{context.request_id}, outcome=#{context.outcome}, receiver_service_id=#{context.receiver_service_id || '-'}, cancel_reason=#{context.cancel_reason || '-'}, backend=#{@nats_backend}"
        release_context(context)
      end
    else
      response_body = BridgeProtocol.collect_non_streaming_body(context.response_queue, timeout: @stream_response_timeout)
      unless response_body
        context.mark_terminal!(BridgeProtocol::OUTCOME_TIMEOUT)
        LOGGER.warn "Bridge body timeout: request_id=#{context.request_id}, timeout=#{@stream_response_timeout}, backend=#{@nats_backend}"
        return gateway_timeout_response(timeout: @stream_response_timeout)
      end

      context.mark_terminal!(BridgeProtocol::OUTCOME_COMPLETED)
      apply_response_start_event(app, start_event)
      response_body
    end
  rescue BridgeProtocol::ProtocolError => e
    context.mark_terminal!(BridgeProtocol::OUTCOME_PROTOCOL_ERROR) if context
    LOGGER.error "Bridge protocol error: request_id=#{context&.request_id}, error=#{e.message}"
    gateway_protocol_error_response(e.message)
  rescue BridgeProtocol::ResponseStreamError => e
    context.mark_terminal!(BridgeProtocol::OUTCOME_UPSTREAM_ERROR) if context
    LOGGER.error "Bridge response error: request_id=#{context&.request_id}, error=#{e.message}"
    gateway_protocol_error_response(e.message)
  ensure
    unless start_event&.dig('streaming')
      LOGGER.info "Request finalized: request_id=#{context&.request_id}, outcome=#{context&.outcome}, backend=#{@nats_backend}" if context
      release_context(context) if context
    end
  end

  def extract_status(env) = env.status

  def extract_headers(env) = env.response_headers

  def emit_stream_chunk(chunk, request_id:, response_credit_window:, cancel_check:)
    unless response_credit_window
      yield BridgeProtocol.chunk_event(chunk)
      return
    end

    bytes = chunk.to_s.b
    offset = 0
    while offset < bytes.bytesize
      reserved = reserve_response_credit(response_credit_window, request_id:, bytes: bytes.bytesize - offset, cancel_check:)
      raise BridgeProtocol::FlowCreditTimeoutError, 'response flow credit timeout' if reserved == FlowCreditWindow::TIMEOUT
      return unless reserved

      part = bytes.byteslice(offset, reserved)
      yield BridgeProtocol.chunk_event(part)
      offset += reserved
    end
  end

  def reserve_response_credit(window, request_id:, bytes:, cancel_check:)
    return true unless window

    result = window.reserve(
      bytes,
      timeout: @flow_credit_wait_timeout,
      cancel_check:,
      on_wait: -> { record_flow_credit_wait(request_id, BridgeProtocol::DIRECTION_RESPONSE) }
    )
    if result == FlowCreditWindow::TIMEOUT
      record_flow_credit_timeout(request_id, BridgeProtocol::DIRECTION_RESPONSE)
      LOGGER.warn "Response flow credit timeout: request_id=#{request_id}, timeout=#{@flow_credit_wait_timeout}, backend=#{@nats_backend}"
    end
    result
  end

  def grant_initial_response_credit(context)
    return if context.receiver_service_id.to_s.empty?

    @core.send_response_credit(context.request_id, context.receiver_service_id, @flow_initial_window_bytes)
  end

  def return_response_credit(context, bytes)
    return if context.receiver_service_id.to_s.empty?

    pending = context.pending_response_credit_bytes.to_i + bytes.to_i
    if pending >= flow_credit_threshold
      @core.send_response_credit(context.request_id, context.receiver_service_id, pending)
      context.pending_response_credit_bytes = 0
    else
      context.pending_response_credit_bytes = pending
    end
  end

  def flush_response_credit(context)
    pending = context.pending_response_credit_bytes.to_i
    context.pending_response_credit_bytes = 0
    @core.send_response_credit(context.request_id, context.receiver_service_id, pending) if pending.positive? && !context.receiver_service_id.to_s.empty?
  end

  def flow_credit_threshold
    [@flow_credit_batch_bytes, @flow_initial_window_bytes].select(&:positive?).min || 1
  end

  def record_flow_credit_wait(request_id, direction)
    collector = @core.collector if @core.respond_to?(:collector)
    collector&.record_flow_credit_wait(request_id:, direction:) if collector&.respond_to?(:record_flow_credit_wait)
  end

  def record_flow_credit_timeout(request_id, direction)
    collector = @core.collector if @core.respond_to?(:collector)
    collector&.record_flow_credit_timeout(request_id:, direction:) if collector&.respond_to?(:record_flow_credit_timeout)
  end
end
