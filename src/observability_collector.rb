require 'json'
require 'time'

class ObservabilityCollector
  REQUEST_ID_LIMIT = 10
  FLOW_LIMIT = 10_000
  METRIC_BUCKET_SECONDS = 60

  def initialize(service_id:, role:, backend:)
    @service_id = service_id
    @role = role
    @backend = backend.to_s
    @events = []
    @mutex = Mutex.new
  end

  def record_request_published(request_id:, subject:, method:, path:, nats_body: nil)
    append_event(
      'request_published',
      request_id:,
      subject:,
      meta: { method:, path: },
      nats_payload: encode_nats_wire(nats_body)
    )
  end

  def record_cancel_published(request_id:, reason:, subject: nil, routing_mode: nil, cancel_envelope: nil)
    nats_payload = cancel_envelope ? encode_nats_wire(cancel_envelope) : nil
    append_event('cancel_published', request_id:, subject:, meta: { reason:, routing_mode: }, nats_payload:)
  end

  def record_cancel_observed(request_id:, reason:, source_service_id:, subject: nil, nats_body: nil)
    append_event(
      'cancel_observed',
      request_id:,
      subject:,
      meta: { reason:, source_service_id: },
      nats_payload: encode_nats_wire(nats_body)
    )
  end

  def record_response_event(request_id:, subject:, event:, nats_body: nil)
    return unless event.is_a?(Hash)

    event_type = event['type'].to_s
    return if event_type.empty?

    normalized =
      case event_type
      when 'response_start'
        {
          status: event['status'].to_i,
          streaming: !!event['streaming'],
          content_type: event['content_type']
        }
      when 'response_chunk'
        {
          body: event['body'],
          body_base64: event['body_base64'],
          body_encoding: event['body_encoding']
        }
      when 'response_error'
        { error: event['error'] }
      else
        {}
      end

    wire_source = nats_body.nil? ? event : nats_body
    append_event(event_type, request_id:, subject:, meta: normalized, nats_payload: encode_nats_wire(wire_source))
  end

  def record_session_chunk(request_id:, subject:, direction:)
    append_event('session_chunk', request_id:, subject:, meta: { direction: direction.to_s }, nats_payload: nil)
  end

  def flow_events(filters = {})
    query = extract_filters(filters)
    items = filter_events(query)
    {
      schema_version: '1.0.0',
      ts: iso8601(now),
      feed_health: feed_health,
      events: items.map { |event| serialize_event(event, include_nats_payload: query[:include_nats_payload]) }
    }
  end

  def flow_cases(filters = {})
    query = extract_filters(filters)

    events = synchronized { @events.dup }
    events = events.select { |event| event[:at] >= query[:from] } if query[:from]
    events = events.select { |event| event[:at] <= query[:to] } if query[:to]
    events = events.select { |event| event[:request_id] == query[:request_id] } if present?(query[:request_id])

    grouped = events.group_by { |event| event[:request_id] }
    cases = grouped.map do |rid, request_events|
      request_events = request_events.sort_by { |event| event[:at] }
      terminal = request_events.reverse.find { |event| terminal_event?(event[:type]) }
      first = request_events.first
      last = request_events.last
      request_published = request_events.find { |event| event[:type] == 'request_published' }
      response_start = request_events.find { |event| event[:type] == 'response_start' }
      chunk_count = request_events.count { |event| %w[response_chunk session_chunk].include?(event[:type]) }
      derived_status, derived_outcome = case_status_and_outcome(request_events)

      {
        request_id: rid,
        events: request_events,
        status: derived_status,
        outcome: derived_outcome,
        in_progress: derived_status == 'in_progress',
        streaming: !!response_start&.dig(:meta, :streaming),
        started_at: first&.dig(:at),
        finished_at: terminal&.dig(:at),
        last_event_at: last&.dig(:at),
        duration_ms: duration_ms(first&.dig(:at), terminal&.dig(:at)),
        events_total: request_events.size,
        chunks_total: chunk_count,
        subject: request_published&.dig(:subject) || response_start&.dig(:subject) || first&.dig(:subject),
        method: request_published&.dig(:meta, :method),
        path: request_published&.dig(:meta, :path),
        terminal_type: terminal&.dig(:type),
        terminal_error: terminal&.dig(:meta, :error)
      }
    end

    cases = cases.select { |item| item[:subject].to_s.include?(query[:subject].to_s) } if present?(query[:subject])
    cases = cases.select { |item| item[:events].any? { |event| event[:service_id] == query[:service_id] } } if present?(query[:service_id])
    cases = cases.select { |item| item[:events].any? { |event| event[:type] == query[:event_type] } } if present?(query[:event_type])
    cases = cases.select { |item| item[:outcome] == query[:outcome] } if present?(query[:outcome])
    cases = cases.select { |item| item[:status] == query[:status] } if present?(query[:status])
    cases = cases.sort_by { |item| item[:last_event_at] || Time.at(0) }.reverse.first(query[:limit])

    {
      schema_version: '1.0.0',
      ts: iso8601(now),
      feed_health: feed_health,
      cases: cases.map do |item|
        {
          request_id: item[:request_id],
          status: item[:status],
          outcome: item[:outcome],
          in_progress: item[:in_progress],
          streaming: item[:streaming],
          started_at: iso8601(item[:started_at]),
          finished_at: iso8601(item[:finished_at]),
          last_event_at: iso8601(item[:last_event_at]),
          duration_ms: item[:duration_ms],
          events_total: item[:events_total],
          chunks_total: item[:chunks_total],
          subject: item[:subject],
          method: item[:method],
          path: item[:path],
          terminal_type: item[:terminal_type],
          terminal_error: item[:terminal_error]
        }
      end
    }
  end

  def metrics(window_sec: METRIC_BUCKET_SECONDS)
    recent = synchronized { @events.select { |event| event[:at] >= now - window_sec } }
    request_count = recent.count { |event| event[:type] == 'request_published' }
    response_end_count = recent.count { |event| %w[response_end session_close].include?(event[:type]) }
    response_error_count = recent.count { |event| event[:type] == 'response_error' }
    cancel_count = recent.count { |event| %w[cancel_published cancel_observed].include?(event[:type]) }

    {
      schema_version: '1.0.0',
      ts: iso8601(now),
      feed_health: feed_health,
      rates: {
        requests_rps: ratio(request_count, window_sec),
        responses_rps: ratio(response_end_count, window_sec),
        errors_rps: ratio(response_error_count, window_sec),
        cancels_rps: ratio(cancel_count, window_sec)
      },
      reconstruction_quality: {
        success_ratio: request_count.zero? ? 1.0 : (response_end_count.to_f / request_count).round(4),
        failed_ratio: request_count.zero? ? 0.0 : (response_error_count.to_f / request_count).round(4)
      }
    }
  end

  def nats_runtime_payload(
    nats_client:,
    service_id:,
    role:,
    backend_mode:,
    stream:,
    consumer:,
    js_api_prefix:
  )
    snapshot = safe_nats_value { runtime_snapshot(nats_client) } || {}
    stats = {
      sent_pings: snapshot_value(snapshot, :sent_pings),
      received_pings: snapshot_value(snapshot, :received_pings),
      received_pongs: snapshot_value(snapshot, :received_pongs)
    }
    server_info = normalize_object(snapshot_value(snapshot, :server_info))

    {
      ts: iso8601(now),
      service_id: service_id,
      role: role,
      backend: backend_mode,
      connection: {
        status: snapshot_value(snapshot, :status),
        connected: snapshot_value(snapshot, :connected),
        disconnected: snapshot_value(snapshot, :disconnected),
        closed: snapshot_value(snapshot, :closed),
        draining: snapshot_value(snapshot, :draining),
        last_error: snapshot_value(snapshot, :last_error)&.to_s,
        server_info:
      },
      stats: stats
    }.merge(
      nats_mode_specific_payload(
        nats_client:,
        backend_mode:,
        stream:,
        consumer:,
        js_api_prefix:
      )
    )
  end

  private

  def safe_nats_value
    yield
  rescue => e
    "error: #{e.class}: #{e.message}"
  end

  def nats_mode_specific_payload(nats_client:, backend_mode:, stream:, consumer:, js_api_prefix:)
    mode = backend_mode.to_s.downcase.to_sym
    unless mode == :jetstream
      return {
        mode_details: {
          backend_mode: mode,
          jetstream_available: false
        }
      }
    end

    info = safe_nats_value { jetstream_runtime_info(nats_client, stream:, consumer:) } || {}
    unless info.is_a?(Hash)
      return {
        mode_details: {
          stream: stream,
          consumer: consumer,
          js_api_prefix: js_api_prefix,
          jetstream_available: false,
          inspection_error: info.to_s
        }
      }
    end

    stream_info = info[:stream_info] || info['stream_info']
    consumer_info = info[:consumer_info] || info['consumer_info']

    stream_hash = normalize_object(stream_info)
    consumer_hash = normalize_object(consumer_info)

    unless stream_hash && consumer_hash
      return {
        mode_details: {
          stream: stream,
          consumer: consumer,
          js_api_prefix: js_api_prefix,
          jetstream_available: false,
          stream_info: normalize_object(stream_info),
          consumer_info: normalize_object(consumer_info)
        }
      }
    end

    stream_state = stream_hash['state'] || {}
    stream_config = stream_hash['config'] || {}
    stream_name = stream_config['name'] || stream_hash['name'] || stream
    consumer_name = consumer_hash['name'] || consumer
    consumer_pending = consumer_hash['num_pending']

    {
      mode_details: {
        stream: stream_name,
        consumer: consumer_name,
        js_api_prefix: js_api_prefix,
        jetstream_available: true,
        stream_messages: stream_state['messages'],
        stream_bytes: stream_state['bytes'],
        stream_first_seq: stream_state['first_seq'],
        stream_last_seq: stream_state['last_seq'],
        consumer_num_pending: consumer_pending,
        stream_info: stream_hash,
        consumer_info: consumer_hash
      }
    }
  end

  def runtime_snapshot(nats_client)
    return nats_client.connection_snapshot if nats_client.respond_to?(:connection_snapshot)

    nc = nats_client.nats
    {
      status: nc.status,
      connected: nc.connected?,
      disconnected: nc.disconnected?,
      closed: nc.closed?,
      draining: nc.draining?,
      last_error: nc.last_error,
      server_info: nc.server_info,
      sent_pings: nc.sent_pings,
      received_pings: nc.received_pings,
      received_pongs: nc.received_pongs
    }
  end

  def jetstream_runtime_info(nats_client, stream:, consumer:)
    return nats_client.jetstream_info(stream:, consumer:) if nats_client.respond_to?(:jetstream_info)

    {
      stream_info: nats_client.js.stream_info(stream),
      consumer_info: nats_client.js.consumer_info(stream, consumer)
    }
  end

  def snapshot_value(snapshot, key)
    return snapshot[key] if snapshot.respond_to?(:key?) && snapshot.key?(key)

    snapshot[key.to_s]
  end

  def normalize_object(value)
    return nil if value.nil?

    deep_normalize(value)
  rescue
    nil
  end

  def deep_normalize(value)
    case value
    when Struct
      value.to_h.to_h { |k, v| [k.to_s, deep_normalize(v)] }
    when Hash
      value.to_h { |k, v| [k.to_s, deep_normalize(v)] }
    when Array
      value.map { |v| deep_normalize(v) }
    when Symbol
      value.to_s
    else
      value
    end
  end

  def append_event(type, request_id:, subject:, meta: {}, nats_payload: nil)
    synchronized do
      @events << {
        at: now,
        type: type.to_s,
        request_id: request_id.to_s,
        subject: subject.to_s.empty? ? nil : subject.to_s,
        service_id: @service_id,
        role: @role,
        backend: @backend,
        meta:,
        nats_payload: normalize_nats_payload_string(nats_payload)
      }
      trim_by_request_id_limit!
    end
  end

  def filter_events(filters)
    query = filters
    events = synchronized { @events.dup }
    events = events.select { |event| event[:request_id] == query[:request_id] } if present?(query[:request_id])
    events = events.select { |event| event[:subject].to_s.include?(query[:subject].to_s) } if present?(query[:subject])
    events = events.select { |event| event[:service_id] == query[:service_id] } if present?(query[:service_id])
    events = events.select { |event| event[:type] == query[:event_type] } if present?(query[:event_type])
    events = events.select { |event| event_outcome(event) == query[:outcome] } if present?(query[:outcome])
    events = events.select { |event| event[:at] >= query[:from] } if query[:from]
    events = events.select { |event| event[:at] <= query[:to] } if query[:to]
    events.sort_by { |event| event[:at] }.last(query[:limit])
  end

  def serialize_event(event, include_nats_payload: false)
    {
      event_id: "#{event[:request_id]}:#{event[:type]}:#{event[:at].to_f}",
      request_id: event[:request_id],
      type: event[:type],
      ts: iso8601(event[:at]),
      service_id: event[:service_id],
      role: event[:role],
      backend: event[:backend],
      subject: event[:subject],
      outcome: event_outcome(event),
      meta: event[:meta],
      nats_payload: include_nats_payload ? event[:nats_payload] : nil
    }
  end

  def event_outcome(event)
    case event[:type]
    when 'response_error'
      error_text = event.dig(:meta, :error).to_s.downcase
      if error_text.include?('timeout') || error_text.include?('gateway timeout')
        'timeout'
      else
        'error'
      end
    when 'response_end', 'session_close'
      'success'
    when 'cancel_published', 'cancel_observed'
      'canceled'
    else
      'in_progress'
    end
  end

  def terminal_event?(event_type)
    %w[response_end session_close response_error cancel_published cancel_observed].include?(event_type)
  end

  def case_status_and_outcome(request_events)
    has_request = request_events.any? { |event| event[:type] == 'request_published' }
    has_start = request_events.any? { |event| %w[response_start session_established].include?(event[:type]) }
    has_end = request_events.any? { |event| %w[response_end session_close].include?(event[:type]) }
    has_cancel = request_events.any? { |event| %w[cancel_published cancel_observed].include?(event[:type]) }
    error_event = request_events.reverse.find { |event| event[:type] == 'response_error' }
    timeout_error = error_event && event_outcome(error_event) == 'timeout'

    return ['canceled', 'canceled'] if has_cancel
    return ['timed_out', 'timeout'] if timeout_error
    return ['failed', 'error'] if error_event
    return ['completed', 'success'] if has_end
    return ['in_progress', 'in_progress'] if has_start
    return ['queued', 'in_progress'] if has_request

    ['queued', 'in_progress']
  end

  def trim_by_request_id_limit!
    request_ids = {}
    @events.reverse_each do |event|
      request_ids[event[:request_id]] = true
      break if request_ids.size >= REQUEST_ID_LIMIT
    end
    @events.select! { |event| request_ids.key?(event[:request_id]) }
  end

  def feed_health
    last = synchronized { @events.last }
    stale_after_ms = 5000
    stale = last.nil? || ((now - last[:at]) * 1000.0 > stale_after_ms)
    {
      state: stale ? 'stale' : 'healthy',
      stale:,
      stale_after_ms:,
      last_success_ts: iso8601(last&.dig(:at))
    }
  end

  def synchronized(&block)
    @mutex.synchronize(&block)
  end

  def parse_time(value)
    return nil if value.nil? || value.to_s.empty?

    Time.iso8601(value.to_s)
  rescue ArgumentError
    nil
  end

  def present?(value)
    !value.nil? && !value.to_s.empty?
  end

  def truthy?(value)
    %w[1 true yes on].include?(value.to_s.downcase)
  end

  def parse_limit(value)
    limit = value.to_i
    limit <= 0 ? FLOW_LIMIT : limit
  end

  def extract_filters(filters)
    {
      request_id: filters['request_id'] || filters[:request_id],
      subject: filters['subject'] || filters[:subject],
      service_id: filters['service_id'] || filters[:service_id],
      event_type: filters['event_type'] || filters[:event_type],
      outcome: filters['outcome'] || filters[:outcome],
      status: filters['status'] || filters[:status],
      from: parse_time(filters['from'] || filters[:from]),
      to: parse_time(filters['to'] || filters[:to]),
      include_nats_payload: truthy?(filters['include_nats_payload'] || filters[:include_nats_payload]),
      limit: parse_limit(filters['limit'] || filters[:limit] || FLOW_LIMIT)
    }
  end

  def ratio(count, window_sec)
    return 0.0 if window_sec.to_i <= 0

    (count.to_f / window_sec).round(4)
  end

  def duration_ms(from, to)
    return nil unless from && to

    ((to - from) * 1000.0).round
  end

  def iso8601(time)
    return nil unless time

    time.utc.iso8601(3)
  end

  def normalize_nats_payload_string(value)
    return nil if value.nil?

    s = value.to_s.b
    s.force_encoding(Encoding::UTF_8)
    return s if s.valid_encoding?

    s.encode(Encoding::UTF_8, Encoding::BINARY, invalid: :replace, undef: :replace)
  end

  def encode_nats_wire(value)
    return nil if value.nil?

    str = value.is_a?(String) ? value : value.to_json
    normalize_nats_payload_string(str)
  end

  def now
    Time.now
  end
end
