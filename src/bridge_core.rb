require 'async'
require 'async/barrier'
require 'async/queue'
require 'async/semaphore'
require 'concurrent-ruby'
require_relative 'request_context'
require_relative 'bridge_protocol'

class BridgeCore
  class PullMessage
    def initialize(message, subject:)
      @message = message
      @subject = subject
    end

    attr_reader :subject

    def method_missing(name, *args, **kwargs, &block)
      @message.public_send(name, *args, **kwargs, &block)
    end

    def respond_to_missing?(name, include_private = false)
      @message.respond_to?(name, include_private) || super
    end
  end

  def initialize(
    nats_client:,
    service_id:,
    collector:,
    nats_backend:,
    config:
  )
    @nats_client = nats_client
    @service_id = service_id
    @collector = collector
    @nats_backend = nats_backend
    @operation_handlers = {}

    @request_subject_root   = config.fetch(:request_subject_root)
    @response_subject_root  = config.fetch(:response_subject_root)
    @listen_subject         = config.fetch(:listen_subject)
    @nats_stream            = config.fetch(:nats_stream)
    @consumer_name          = config.fetch(:consumer_name)
    @queue_group            = config.fetch(:queue_group)
    @response_timeout       = config.fetch(:response_timeout)
    @stream_timeout         = config.fetch(:stream_timeout)
    @max_inflight           = config.fetch(:max_inflight)
    @queue_size             = config.fetch(:queue_size)

    @pending_requests = Concurrent::Map.new
    @active_streams   = Concurrent::Map.new

    @bridge_ready      = Concurrent::AtomicBoolean.new(false)
    @bridge_last_error = Concurrent::AtomicReference.new(nil)
    @inflight          = Concurrent::AtomicFixnum.new(0)
    @dispatch_queue    = Concurrent::AtomicReference.new(nil)
    @dispatcher_alive  = Concurrent::AtomicBoolean.new(false)

    @response_listener_task = nil
    @request_listener_task = nil
    @dispatcher_task = nil
    @upstream_session_listener_task = nil
    @downstream_session_listener_task = nil
    @cancel_listener_task = nil
    @session_barrier = Concurrent::AtomicReference.new(nil)
  end

  def bridge_outbound? = listener_alive?(@response_listener_task)

  def bridge_inbound? = listener_alive?(@request_listener_task)

  def bridge_ready? = @bridge_ready.value

  def inflight_count = @inflight.value

  def queue_depth = @dispatch_queue.get&.size || 0

  def dispatcher_alive? = @dispatcher_alive.value

  def last_bridge_error = @bridge_last_error.get

  def register_handler(operation_kind, &handler)
    @operation_handlers[operation_kind.to_s] = handler
  end

  def bridge_request(request_id:, operation: 'http_request', payload:)
    context = RequestContext.new(request_id:)
    context.request_subject = subject_for_request(request_id)
    @pending_requests[request_id] = context

    message = BridgeProtocol.request_envelope(
      request_id:,
      reply_to: subject_for_response(request_id),
      operation:,
      payload:
    )
    @collector.record_request_published(
      request_id:,
      subject: context.request_subject,
      method: payload['method'],
      path: payload['path'],
      nats_body: message
    )
    publish_message(message, context.request_subject, headers: trace_headers(payload['headers']))
    LOGGER.info "Published bridge request: request_id=#{request_id}, operation=#{operation}, backend=#{@nats_backend}, request_subject=#{context.request_subject}"

    context
  end

  def bridge_session_open(session_id:, payload:)
    context = RequestContext.new(request_id: session_id)
    context.request_subject = subject_for_request(session_id)
    context.tunnel_data_queue = Async::Queue.new
    @pending_requests[session_id] = context

    message = BridgeProtocol.request_envelope(
      request_id: session_id,
      reply_to: subject_for_response(session_id),
      operation: 'tcp_stream',
      payload: payload.merge('requester_service_id' => @service_id)
    )
    @collector.record_request_published(
      request_id: session_id,
      subject: context.request_subject,
      method: payload['method'],
      path: "#{payload['host']}:#{payload['port']}",
      nats_body: message
    )
    publish_message(message, context.request_subject)
    LOGGER.info "Published session open: session_id=#{session_id}, target=#{payload['host']}:#{payload['port']}, backend=#{@nats_backend}"

    context
  end

  def cancel_request(context, reason:)
    state = context.request_cancel!(reason:)
    return false unless state == :ready

    published = publish_cancel_signal(context.request_id, context, reason:)
    context.mark_cancel_sent! if published
    LOGGER.info "Stream cancel requested: request_id=#{context.request_id}, reason=#{reason}, cancel_published=#{published}, backend=#{@nats_backend}"
    published
  end

  def send_session_data(session_id, binary_data, receiver_service_id:)
    subject = session_upstream_subject(receiver_service_id, session_id)
    @collector.record_session_chunk(request_id: session_id, subject:, direction: 'upstream')
    publish_message(binary_data, subject, headers: {
      'Nats-Session-Id' => session_id,
      'Nats-Frame-Type' => 'session_data'
    })
  end

  def send_session_downstream(session_id, requester_service_id, binary_data)
    subject = session_downstream_subject(requester_service_id, session_id)
    @collector.record_session_chunk(request_id: session_id, subject:, direction: 'downstream')
    publish_message(binary_data, subject, headers: {
      'Nats-Session-Id' => session_id,
      'Nats-Frame-Type' => 'session_data_downstream'
    })
  end

  def close_session(session_id, reason: 'normal', receiver_service_id:)
    publish_message(
      BridgeProtocol.session_close_event(reason:).to_json,
      session_upstream_subject(receiver_service_id, session_id),
      headers: {
        'Nats-Session-Id' => session_id,
        'Nats-Frame-Type' => 'session_close'
      }
    )
  end

  def process_response_event(msg)
    request_id = msg.subject.split('.').last
    context = @pending_requests[request_id]

    unless context
      LOGGER.warn "Dropping late bridge response: request_id=#{request_id}, service_id=#{@service_id}, backend=#{@nats_backend}"
      return
    end

    raw = msg.data
    event = BridgeProtocol.parse_event(raw.to_s)

    unless event
      LOGGER.warn "Dropping unparseable bridge response: request_id=#{request_id}, service_id=#{@service_id}, backend=#{@nats_backend}"
      return
    end

    event_type = event.fetch('type', nil)
    @collector.record_response_event(request_id:, subject: msg.subject, event:, nats_body: raw)

    if context.cancel_requested?
      if context.allow_event_after_cancel?(event_type, allow_end: true)
        context.response_queue.push(event)
        return
      end
      LOGGER.info "Dropping post-cancel response event: request_id=#{request_id}, event_type=#{event_type || 'unknown'}, backend=#{@nats_backend}"
      return
    end

    LOGGER.info "Dispatching bridge response: request_id=#{request_id}, service_id=#{@service_id}, backend=#{@nats_backend}"
    context.response_queue.push(event)
  end

  def process_bridge_request(msg)
    process_bridge_request_data(msg, parse_bridge_message(msg))
  rescue JSON::ParserError
    reply_to = reply_subject(msg)
    raise ArgumentError, 'Invalid JSON' if reply_to.to_s.empty?

    publish_error(reply_to, status: 400, error: 'Invalid JSON', headers: trace_headers(msg.header))
    LOGGER.warn "Rejected malformed bridge request: error=Invalid JSON, backend=#{@nats_backend}, reply_to=#{reply_to}"
    raise ArgumentError, 'Invalid JSON' unless publish_raw?
  end

  def start_response_listener(task:)
    subject = "#{response_scope}.>"

    @response_listener_task = task.async(annotation: "bridge-response-listener-#{@service_id}") do |listener_task|
      with_reconnect_loop(listener_task, 'Response listener') do
        if @nats_backend == :jetstream
          set_bridge_state(ready: true)
          run_pull_listener(
            subject:,
            stream: @nats_stream,
            consumer_name: "#{@consumer_name}-responses-#{@service_id}",
            params: { config: { inactive_threshold: @stream_timeout + 60 } },
            manual_ack: false,
            stale_ack: true
          ) { |msg| process_response_event(msg) }
        else
          begin
            sid = subscribe_core(subject) { |msg| process_response_event(msg) }
            LOGGER.info "Response listener active: backend=#{@nats_backend}, subject=#{subject}"
            set_bridge_state(ready: true)
            wait_until_canceled(listener_task)
          ensure
            unsubscribe_core(sid)
          end
        end
      end
    end
  end

  def start_request_listener(task:)
    ack_wait = [@response_timeout, @stream_timeout].max + 30
    in_progress_interval = [ack_wait / 3, 30].min
    dispatch_queue = Async::LimitedQueue.new(@queue_size)
    @dispatch_queue.set(dispatch_queue)
    @dispatcher_task = start_dispatcher(task, dispatch_queue, in_progress_interval:)

    @request_listener_task = task.async(annotation: "bridge-request-listener-#{@service_id}") do |listener_task|
      with_reconnect_loop(listener_task, 'Request listener') do
        if @nats_backend == :jetstream
          set_bridge_state(ready: true)
          run_pull_listener(
            subject: @listen_subject,
            stream: @nats_stream,
            consumer_name: @consumer_name,
            params: { config: { max_ack_pending: 200, max_waiting: 20, ack_wait:, max_deliver: 3 } },
            manual_ack: true
          ) { |msg| dispatch_bridge_message(msg, queue: dispatch_queue, manual_ack: true) }
        else
          begin
            sid = subscribe_core(@listen_subject, queue: @queue_group) do |msg|
              dispatch_bridge_message(msg, queue: dispatch_queue, manual_ack: false)
            end
            LOGGER.info "Request listener active: backend=#{@nats_backend}, subject=#{@listen_subject}, queue=#{@queue_group}"
            set_bridge_state(ready: true)
            wait_until_canceled(listener_task)
          ensure
            unsubscribe_core(sid)
          end
        end
      end
    ensure
      dispatch_queue.close
      @dispatch_queue.set(nil)
      @dispatcher_task&.stop
      @dispatcher_task = nil
    end
  end

  def start_upstream_session_listener(task:)
    subject = upstream_session_listen_subject

    @upstream_session_listener_task = task.async(annotation: "bridge-upstream-session-listener-#{@service_id}") do |listener_task|
      with_reconnect_loop(listener_task, 'Upstream session data listener') do
        if @nats_backend == :jetstream
          run_pull_listener(
            subject:,
            stream: @nats_stream,
            consumer_name: "#{@consumer_name}-sessions-upstream-#{@service_id}",
            params: { config: { inactive_threshold: @stream_timeout + 60 } },
            manual_ack: false,
            stale_ack: true
          ) { |msg| dispatch_session_data(msg) }
        else
          begin
            sid = subscribe_core(subject) { |msg| dispatch_session_data(msg) }
            LOGGER.info "Upstream session data listener active: backend=#{@nats_backend}, subject=#{subject}"
            wait_until_canceled(listener_task)
          ensure
            unsubscribe_core(sid)
          end
        end
      end
    end
  end

  def start_downstream_session_listener(task:)
    subject = downstream_session_listen_subject

    @downstream_session_listener_task = task.async(annotation: "bridge-downstream-session-listener-#{@service_id}") do |listener_task|
      with_reconnect_loop(listener_task, 'Downstream session data listener') do
        if @nats_backend == :jetstream
          run_pull_listener(
            subject:,
            stream: @nats_stream,
            consumer_name: "#{@consumer_name}-sessions-downstream-#{@service_id}",
            params: { config: { inactive_threshold: @stream_timeout + 60 } },
            manual_ack: false,
            stale_ack: true
          ) { |msg| dispatch_session_data(msg) }
        else
          begin
            sid = subscribe_core(subject) { |msg| dispatch_session_data(msg) }
            LOGGER.info "Downstream session data listener active: backend=#{@nats_backend}, subject=#{subject}"
            wait_until_canceled(listener_task)
          ensure
            unsubscribe_core(sid)
          end
        end
      end
    end
  end

  def start_cancel_listener(task:)
    subject = cancel_listen_subject

    @cancel_listener_task = task.async(annotation: "bridge-cancel-listener-#{@service_id}") do |listener_task|
      with_reconnect_loop(listener_task, 'Cancel listener') do
        if @nats_backend == :jetstream
          run_pull_listener(
            subject:,
            stream: @nats_stream,
            consumer_name: "#{@consumer_name}-cancel-#{@service_id}",
            params: { config: { inactive_threshold: @stream_timeout + 60 } },
            manual_ack: false,
            stale_ack: true
          ) { |msg| process_cancel_listener_message(msg) }
        else
          begin
            sid = subscribe_core(subject) { |msg| process_cancel_listener_message(msg) }
            LOGGER.info "Cancel listener active: backend=#{@nats_backend}, subject=#{subject}"
            wait_until_canceled(listener_task)
          ensure
            unsubscribe_core(sid)
          end
        end
      end
    end
  end

  def release_pending(request_id)
    @pending_requests.delete(request_id)
  end

  def close
    @dispatch_queue.get&.close
    @response_listener_task&.stop
    @request_listener_task&.stop
    @dispatcher_task&.stop
    @upstream_session_listener_task&.stop
    @downstream_session_listener_task&.stop
    @cancel_listener_task&.stop
    set_bridge_state(ready: false)
  end

  private

  def listener_alive?(task_handle)
    !!task_handle && !task_handle.finished?
  rescue NoMethodError
    false
  end

  def subject_for_request(request_id)
    "#{@request_subject_root}.requests.#{@service_id}.#{request_id}"
  end

  def subject_for_response(request_id)
    "#{response_scope}.#{request_id}"
  end

  def response_scope
    "#{@response_subject_root}.responses.#{@service_id}"
  end

  def session_upstream_subject(receiver_service_id, session_id)
    receiver_id = receiver_service_id.to_s
    raise ArgumentError, "Missing receiver_service_id for session_id=#{session_id}" if receiver_id.empty?

    "#{@request_subject_root}.sessions.upstream.#{receiver_id}.#{session_id}"
  end

  def session_downstream_subject(target_service_id, session_id)
    "#{@response_subject_root}.sessions.downstream.#{target_service_id}.#{session_id}"
  end

  def upstream_session_listen_subject
    "#{@request_subject_root}.sessions.upstream.#{@service_id}.>"
  end

  def downstream_session_listen_subject
    "#{@response_subject_root}.sessions.downstream.#{@service_id}.>"
  end

  def subject_for_cancel(receiver_service_id, request_id)
    "#{@request_subject_root}.cancel.#{receiver_service_id}.#{request_id}"
  end

  def cancel_listen_subject
    "#{@request_subject_root}.cancel.#{@service_id}.>"
  end

  def publish_raw?
    @nats_backend != :jetstream
  end

  def publish_message(message, subject, headers: nil)
    @nats_client.publish(message, subject, nil, raw: publish_raw?, headers:)
  end

  def trace_headers(headers)
    BridgeProtocol.normalize_headers(headers).each_with_object({}) do |(key, value), forwarded|
      forwarded[key] = value if %w[traceparent tracestate].include?(key)
    end
  end

  def publish_event(reply_to, event, raw:, headers: {})
    return unless reply_to

    request_id = reply_to.to_s.split('.').last
    @collector.record_response_event(request_id:, subject: reply_to, event:)
    @nats_client.publish(event.to_json, reply_to, nil, raw:, headers:)
  end

  def publish_error(reply_to, status:, error:, headers: {})
    start_event = {
      'type' => BridgeProtocol::RESPONSE_START,
      'status' => status.to_i,
      'headers' => { 'content-type' => 'application/json' },
      'content_type' => 'application/json',
      'streaming' => false,
      'receiver_service_id' => @service_id
    }
    publish_event(reply_to, start_event, raw: publish_raw?, headers:)
    publish_event(reply_to, BridgeProtocol.chunk_event({ error: }.to_json), raw: publish_raw?, headers:)
    publish_event(reply_to, BridgeProtocol.end_event, raw: publish_raw?, headers:)
  end

  def publish_cancel_diagnostic(reply_to, reason:, headers: {})
    publish_event(reply_to, BridgeProtocol.error_event("stream canceled: #{reason}"), raw: publish_raw?, headers:)
    publish_event(reply_to, BridgeProtocol.end_event, raw: publish_raw?, headers:)
  end

  def publish_cancel_signal(request_id, context, reason:)
    payload = BridgeProtocol.cancel_envelope(request_id:, service_id: @service_id, reason:)
    routing_mode = 'fallback'
    subject = context.request_subject

    unless context.receiver_service_id.to_s.empty?
      routing_mode = 'owner'
      subject = subject_for_cancel(context.receiver_service_id, request_id)
    end

    raise ArgumentError, "Missing cancel subject for request_id=#{request_id}" if subject.to_s.empty?

    publish_message(payload, subject)
    @collector.record_cancel_published(request_id:, reason:, subject:, routing_mode:, cancel_envelope: payload)
    LOGGER.info "Published cancel signal: request_id=#{request_id}, subject=#{subject}, routing_mode=#{routing_mode}, backend=#{@nats_backend}"
    true
  rescue => e
    LOGGER.error "Failed to publish cancel signal: request_id=#{request_id}, reason=#{reason}, error=#{e.class} - #{e.message}"
    false
  end

  def normalize_reply_subject(subject)
    subject.to_s.sub(/\Afrom\./, '')
  end

  def parse_bridge_message(msg)
    JSON.parse(msg.data)
  end

  def parse_cancel_message(data)
    return nil unless data.is_a?(Hash)

    cancel = data['cancel']
    return nil unless cancel.is_a?(Hash)

    request_id = data['request_id'].to_s.empty? ? cancel['request_id'].to_s : data['request_id'].to_s
    return nil if request_id.empty?

    {
      request_id:,
      reason: cancel['reason'].to_s.empty? ? 'downstream_disconnect' : cancel['reason'].to_s,
      service_id: cancel['service_id'],
      timestamp: cancel['timestamp']
    }
  end

  def process_cancel_message(data, subject:, raw:)
    cancel = parse_cancel_message(data)
    unless cancel
      LOGGER.warn "Ignoring invalid cancel message: subject=#{subject}, backend=#{@nats_backend}"
      return
    end

    context = @active_streams[cancel[:request_id]]
    unless context
      LOGGER.info "Ignoring late cancel (no active stream): request_id=#{cancel[:request_id]}, reason=#{cancel[:reason]}, backend=#{@nats_backend}"
      return
    end

    transitioned = context.observe_cancel!(reason: cancel[:reason])

    unless transitioned
      LOGGER.info "Ignoring duplicate/late cancel for receiver stream: request_id=#{cancel[:request_id]}, reason=#{cancel[:reason]}, backend=#{@nats_backend}"
      return
    end

    LOGGER.info "Receiver cancel observed: request_id=#{cancel[:request_id]}, reason=#{cancel[:reason]}, source_service_id=#{cancel[:service_id]}, subject=#{subject}, backend=#{@nats_backend}"
    @collector.record_cancel_observed(
      request_id: cancel[:request_id],
      reason: cancel[:reason],
      source_service_id: cancel[:service_id],
      subject:,
      nats_body: raw
    )
    true
  end

  def process_cancel_listener_message(msg)
    process_cancel_message(parse_bridge_message(msg), subject: msg.subject, raw: msg.data)
  rescue JSON::ParserError
    LOGGER.warn "Ignoring invalid cancel JSON: subject=#{msg.subject}, backend=#{@nats_backend}"
  end

  def reject_invalid_request(reply_to, error:, status: 400, nats_headers: {})
    publish_error(reply_to, status:, error:, headers: nats_headers)
    raise ArgumentError, error unless publish_raw?
  end

  def validate_request_envelope(msg, data)
    unless data.is_a?(Hash)
      reply_to = reply_subject(msg)
      raise ArgumentError, 'Invalid request envelope' if reply_to.to_s.empty?

      LOGGER.warn "Rejected malformed bridge request: error=Invalid request envelope, backend=#{@nats_backend}"
      reject_invalid_request(reply_to, error: 'Invalid request envelope', nats_headers: trace_headers(msg.header))
      return nil
    end

    return :cancel if data['type'] == 'cancel'

    if !data['type'].to_s.empty? && data['type'] != 'request'
      reply_to = normalize_reply_subject(reply_subject(msg, data))
      raise ArgumentError, 'Invalid request envelope type' if reply_to.to_s.empty?

      LOGGER.warn "Rejected malformed bridge request: error=Invalid request envelope type, backend=#{@nats_backend}"
      reject_invalid_request(reply_to, error: 'Invalid request envelope type',
        nats_headers: trace_headers(msg.header || data['headers']))
      return nil
    end

    request_id = data['request_id']
    reply_to = normalize_reply_subject(reply_subject(msg, data))
    nats_headers = trace_headers(msg.header || data.dig('payload', 'headers'))
    operation = data['operation'].to_s
    payload = data['payload']

    validation_error =
      if request_id.to_s.empty? then 'Missing request_id'
      elsif reply_to.to_s.empty? then 'Missing reply_to'
      elsif operation.empty? then 'Missing operation'
      elsif payload.nil? then 'Missing payload'
      end

    if validation_error
      raise ArgumentError, validation_error if reply_to.to_s.empty?

      LOGGER.warn "Rejected malformed bridge request: error=#{validation_error}, backend=#{@nats_backend}"
      reject_invalid_request(reply_to, error: validation_error, nats_headers:)
      return nil
    end

    { request_id:, reply_to:, nats_headers:, operation:, payload: }
  end

  def process_bridge_request_data(msg, data, on_event: nil)
    result = validate_request_envelope(msg, data)
    return if result.nil?
    return process_cancel_message(data, subject: msg.subject, raw: msg.data) if result == :cancel

    request_id = result[:request_id]
    reply_to = result[:reply_to]
    nats_headers = result[:nats_headers]
    operation = result[:operation]
    payload = result[:payload]

    handler = @operation_handlers[operation]

    unless handler
      error_message = "No handler registered for operation=#{operation}"
      return publish_error(reply_to, status: 503, error: error_message, headers: nats_headers) if publish_raw?

      raise BridgeProtocol::UpstreamUnavailableError, error_message
    end

    LOGGER.info "Processing bridge request: request_id=#{request_id}, operation=#{operation}, backend=#{@nats_backend}, reply_to=#{reply_to}"
    @collector.record_request_published(
      request_id: request_id.to_s,
      subject: msg.subject.to_s,
      method: payload['method'],
      path: payload['path'],
      nats_body: msg.data
    )

    context = RequestContext.new(request_id:, initial_state: 'active')
    @active_streams[request_id] = context
    context.upstream_queue = Async::Queue.new if operation == 'tcp_stream'
    detached_lifecycle = false
    session_close_emitted = false

    complete_stream = lambda do |proxy_outcome|
      if proxy_outcome == BridgeProtocol::OUTCOME_CANCELED
        if operation == 'tcp_stream'
          unless session_close_emitted
            publish_event(reply_to, BridgeProtocol.session_close_event(reason: context.cancel_reason || 'cancel_requested'), raw: publish_raw?, headers: nats_headers)
          end
        else
          cancel_reason = context.cancel_reason.to_s
          cancel_reason = 'cancel_requested' if cancel_reason.empty?
          publish_cancel_diagnostic(reply_to, reason: cancel_reason, headers: nats_headers)
        end
      end

      final_outcome = context.finalize!(fallback_outcome: proxy_outcome || BridgeProtocol::OUTCOME_COMPLETED)
      LOGGER.info "Receiver stream finished: request_id=#{request_id}, outcome=#{final_outcome}, cancel_reason=#{context.cancel_reason || '-'}, backend=#{@nats_backend}"
    ensure
      @active_streams.delete(request_id)
    end

    handler_kwargs = {
      payload:,
      cancel_check: -> { context.cancel_requested? },
      emit_failure_response: publish_raw?,
      request_id: request_id,
      upstream_queue: context.upstream_queue
    }

    if operation == 'tcp_stream'
      handler_kwargs[:detached] = true
      handler_kwargs[:task_parent] = @session_barrier.get
      handler_kwargs[:on_complete] = complete_stream
    end

    proxy_outcome = handler.call(**handler_kwargs) do |event|
      next unless context.allow_event_after_cancel?(event['type'], allow_end: true)
      session_close_emitted = true if event['type'] == BridgeProtocol::SESSION_CLOSE

      enriched =
        if [BridgeProtocol::RESPONSE_START, BridgeProtocol::SESSION_ESTABLISHED].include?(event['type'])
          enrich_owner_event(event, request_id:, operation:, payload:)
        else
          event
        end
      publish_event(reply_to, enriched, raw: publish_raw?, headers: nats_headers)
      on_event&.call(enriched)
    end

    if proxy_outcome == BridgeProtocol::OUTCOME_DETACHED
      detached_lifecycle = true
      return proxy_outcome
    end

    complete_stream.call(proxy_outcome)
  rescue BridgeProtocol::InvalidRequestError => e
    reply_to ||= normalize_reply_subject(reply_subject(msg))
    raise if reply_to.to_s.empty?

    LOGGER.warn "Rejected malformed bridge request: error=#{e.message}, backend=#{@nats_backend}"
    reject_invalid_request(reply_to, error: e.message, nats_headers: nats_headers || trace_headers(msg.header))
  ensure
    @active_streams.delete(request_id) if defined?(request_id) && request_id && !detached_lifecycle
  end

  def dispatch_bridge_message(msg, queue:, manual_ack:)
    data = parse_bridge_message(msg)

    if data.is_a?(Hash) && data['type'] == 'cancel'
      cancel = parse_cancel_message(data)
      if cancel
        process_cancel_message(data, subject: msg.subject, raw: msg.data)
        msg.ack if manual_ack
        return
      end
    end

    enqueue_dispatch_job(queue, { msg:, data:, manual_ack: })
  rescue JSON::ParserError
    enqueue_dispatch_job(queue, { msg:, data: :parse_error, manual_ack: })
  end

  def dispatch_session_data(msg)
    session_id = msg.subject.to_s.split('.').last
    frame_type = msg.header&.dig('Nats-Frame-Type')

    case frame_type
    when 'session_close'
      context = @active_streams[session_id]
      unless context&.upstream_queue
        LOGGER.warn "Dropping session close for unknown/non-session stream: session_id=#{session_id}, backend=#{@nats_backend}"
        return
      end
      context.upstream_queue.push(:session_close)
      LOGGER.info "Session close received via upstream: session_id=#{session_id}, backend=#{@nats_backend}"
    when 'session_data', 'session_data_upstream', nil
      context = @active_streams[session_id]
      unless context&.upstream_queue
        LOGGER.warn "Dropping upstream session data for unknown stream: session_id=#{session_id}, backend=#{@nats_backend}"
        return
      end
      @collector.record_session_chunk(request_id: session_id, subject: msg.subject, direction: 'upstream')
      context.upstream_queue.push(msg.data)
    when 'session_data_downstream'
      context = @pending_requests[session_id]
      unless context&.tunnel_data_queue
        LOGGER.warn "Dropping downstream session data for unknown stream: session_id=#{session_id}, backend=#{@nats_backend}"
        return
      end
      @collector.record_session_chunk(request_id: session_id, subject: msg.subject, direction: 'downstream')
      context.tunnel_data_queue.push(msg.data)
    else
      LOGGER.warn "Dropping session frame with unknown type: session_id=#{session_id}, frame_type=#{frame_type}, backend=#{@nats_backend}"
    end
  end

  def enrich_owner_event(event, request_id:, operation:, payload:)
    enriched = event.merge('receiver_service_id' => @service_id)

    case event['type']
    when BridgeProtocol::RESPONSE_START
      enriched['request_id'] = request_id.to_s
      enriched['flow_kind'] = event['streaming'] ? 'http_stream' : operation.to_s
    when BridgeProtocol::SESSION_ESTABLISHED
      enriched['session_id'] = request_id.to_s if enriched['session_id'].to_s.empty?
      enriched['flow_kind'] = payload['ingress_kind'].to_s == 'socks5' ? 'socks5_stream' : 'tcp_stream'
    end

    enriched
  end

  def start_receiver_progress_task(task, msg, interval:)
    msg.in_progress
    return nil if interval.to_f <= 0

    task.async(transient: true) do
      loop do
        task.sleep interval
        msg.in_progress
      end
    rescue => e
      LOGGER.warn "JetStream in_progress heartbeat failed: #{e.class} - #{e.message}"
    end
  end

  def run_request_job(job, in_progress_interval:)
    msg = job.fetch(:msg)
    data = job[:data]
    manual_ack = job.fetch(:manual_ack)
    acknowledged = false

    task = Async::Task.current
    heartbeat = start_receiver_progress_task(task, msg, interval: in_progress_interval) if manual_ack

    if data == :parse_error
      raise ArgumentError, 'Invalid JSON'
    elsif data
      event_callback = nil
      if manual_ack && data.is_a?(Hash) && data['operation'] == 'tcp_stream'
        event_callback = lambda do |event|
          next unless event.is_a?(Hash) && event['type'] == BridgeProtocol::SESSION_ESTABLISHED && !acknowledged

          msg.ack
          acknowledged = true
        end
      end

      process_bridge_request_data(msg, data, on_event: event_callback)
    else
      process_bridge_request(msg)
    end

    if manual_ack && !acknowledged
      msg.ack
      acknowledged = true
    end
  rescue ArgumentError => e
    LOGGER.warn "Invalid message, terminating: #{e.message}\n#{e.backtrace&.first(20)&.join("\n")}"
    msg.term if manual_ack && !acknowledged
  rescue => e
    LOGGER.error "Processing error, will retry: #{e.class} - #{e.message}"
    msg.nak if manual_ack && !acknowledged
  ensure
    heartbeat&.stop
  end

  def start_dispatcher(task, queue, in_progress_interval:)
    task.async(annotation: "bridge-dispatcher-#{@service_id}") do |dispatcher_task|
      barrier = Async::Barrier.new(parent: dispatcher_task)
      session_barrier = Async::Barrier.new(parent: dispatcher_task)
      semaphore = Async::Semaphore.new(@max_inflight, parent: barrier)
      @session_barrier.set(session_barrier)

      @dispatcher_alive.make_true
      loop do
        job = queue.dequeue
        break unless job

        semaphore.async do
          @inflight.increment
          begin
            run_request_job(job, in_progress_interval:)
          ensure
            @inflight.decrement
          end
        rescue => e
          LOGGER.error "Dispatcher job failed: #{e.class} - #{e.message}"
          job[:msg]&.nak if job[:manual_ack] rescue nil
        end
      end
    ensure
      @session_barrier.set(nil)
      barrier&.cancel
      barrier&.wait
      session_barrier&.cancel
      session_barrier&.wait
      @dispatcher_alive.make_false
    end
  end

  def with_reconnect_loop(task, label)
    loop do
      yield
      set_bridge_state(ready: true)
    rescue Async::Stop
      raise
    rescue => e
      set_bridge_state(ready: false, error: "#{e.class}: #{e.message}")
      LOGGER.error "#{label} failed: #{e.class} - #{e.message}"
      task.sleep 5
    end
  end

  def set_bridge_state(ready:, error: nil)
    @bridge_ready.value = ready
    @bridge_last_error.set(error)
  end

  def wait_until_canceled(task)
    loop { task.sleep 60 }
  end

  def subscribe_core(subject, queue: nil, &block)
    @nats_client.subscribe(subject, queue:) do |msg|
      block.call(msg)
    rescue => e
      LOGGER.error "NATS subscribe callback error: #{e.class} - #{e.message}"
    end
  end

  def unsubscribe_core(sid)
    return unless sid

    @nats_client.unsubscribe(sid)
  rescue => e
    LOGGER.warn "NATS unsubscribe failed: #{e.class} - #{e.message}"
  end

  def run_pull_listener(subject:, stream:, consumer_name:, params:, manual_ack:, stale_ack: false, &block)
    normalized_subject = normalize_stream_subject(subject)
    default_config =
      if manual_ack
        {
          durable_name: consumer_name,
          deliver_policy: 'new',
          ack_policy: 'explicit',
          filter_subject: normalized_subject,
          max_ack_pending: 20,
          max_waiting: 3,
          ack_wait: 5
        }
      else
        {
          name: consumer_name,
          deliver_policy: 'new',
          ack_policy: 'explicit',
          filter_subject: normalized_subject,
          inactive_threshold: 600
        }
      end

    config = (default_config.merge(params[:config] || {})).transform_keys(&:to_sym)
    ensure_consumer!(stream, consumer_name, config)

    pull_subscription = @nats_client.jetstream.pull_subscribe(
      normalized_subject,
      stream:,
      consumer: consumer_name,
      config:,
      create: false
    )

    loop do
      messages = pull_subscription.fetch(batch: 1, timeout: 1)
      messages.each do |msg|
        msg = normalize_pull_message(msg)

        if manual_ack
          block.call(msg)
        else
          block.call(msg)
          msg.ack
        end
      rescue ArgumentError => e
        LOGGER.warn "Invalid message, terminating: #{e.message}\n#{e.backtrace&.first(20)&.join("\n")}"
        msg.term if manual_ack
      rescue => e
        if stale_ack
          LOGGER.error "Ephemeral consumer handler error, acking stale response: #{e.class} - #{e.message}"
          msg.ack rescue nil
        else
          LOGGER.error "Processing error, will retry: #{e.class} - #{e.message}"
          msg.nak if manual_ack
        end
      end
    end
  ensure
    pull_subscription&.unsubscribe
  end

  def ensure_consumer!(stream, consumer_name, desired_config)
    existing = @nats_client.jetstream.consumer_info(stream, consumer_name)
    current_config = normalize_consumer_config_for_compare(existing[:config] || {})
    comparable_desired_config = normalize_consumer_config_for_compare(desired_config)
    return if consumer_config_matches?(current_config, comparable_desired_config)

    @nats_client.jetstream.delete_consumer(stream, consumer_name)
    @nats_client.jetstream.add_consumer(stream, consumer_config_for_api(desired_config))
  rescue NatsAsync::JetStream::NotFound
    @nats_client.jetstream.add_consumer(stream, consumer_config_for_api(desired_config))
  end

  def consumer_config_matches?(current_config, desired_config)
    desired_config.keys.all? do |key|
      values_match?(key, current_config[key], desired_config[key])
    end
  end

  def normalize_consumer_config_for_compare(config)
    config.each_with_object({}) do |(key, value), memo|
      memo[key.to_sym] = value
    end
  end

  def values_match?(key, current_value, desired_value)
    return duration_values_match?(current_value, desired_value) if duration_config_key?(key)

    current_value == desired_value
  end

  def duration_config_key?(key)
    %i[ack_wait inactive_threshold].include?(key.to_sym)
  end

  def consumer_config_for_api(config)
    config.each_with_object({}) do |(key, value), serialized|
      serialized[key] =
        if duration_config_key?(key)
          duration_value_for_api(value)
        else
          value
        end
    end
  end

  def duration_values_match?(current_value, desired_value)
    return true if current_value == desired_value
    return false unless numeric_duration?(current_value) && numeric_duration?(desired_value)

    current_seconds = current_value.to_f
    desired_seconds = desired_value.to_f
    scale = 1_000_000_000

    current_seconds == desired_seconds * scale || current_seconds * scale == desired_seconds
  end

  def numeric_duration?(value)
    value.is_a?(Numeric)
  end

  def duration_value_for_api(value)
    return value unless numeric_duration?(value)

    (value.to_f * 1_000_000_000).to_i
  end

  def normalize_stream_subject(subject)
    subject.to_s.sub(/\A(?:to|from)\./, '')
  end

  def normalize_pull_message(message)
    original_subject = extract_headers(message)['Nats-Subject'] || extract_headers(message)['nats-subject']
    return message if original_subject.to_s.empty?

    PullMessage.new(message, subject: original_subject)
  end

  def enqueue_dispatch_job(queue, job)
    if queue.respond_to?(:enqueue)
      queue.enqueue(job)
    else
      queue.push(job)
    end
  end

  def reply_subject(msg, data = nil)
    headers = extract_headers(msg)
    payload_reply_to = data.is_a?(Hash) ? (data['reply_to'] || data['reply-to']) : nil
    reply = headers['Reply-To'] || headers['reply-to'] || headers['Reply_To'] || payload_reply_to
    return reply unless reply.to_s.empty?

    candidate = msg.respond_to?(:reply) ? msg.reply : nil
    return nil if candidate.to_s.start_with?('$JS.ACK.')

    candidate
  end

  def extract_headers(msg)
    if msg.respond_to?(:headers) && msg.headers
      msg.headers
    elsif msg.respond_to?(:header) && msg.header
      msg.header
    else
      {}
    end
  end
end
