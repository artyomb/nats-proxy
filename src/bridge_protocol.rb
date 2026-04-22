require 'base64'
require 'json'
require 'time'

module BridgeProtocol
  RESPONSE_START = 'response_start'.freeze
  RESPONSE_CHUNK = 'response_chunk'.freeze
  RESPONSE_END = 'response_end'.freeze
  RESPONSE_ERROR = 'response_error'.freeze

  OUTCOME_COMPLETED = 'completed'.freeze
  OUTCOME_CANCELED = 'canceled'.freeze
  OUTCOME_TIMEOUT = 'timeout'.freeze
  OUTCOME_UPSTREAM_ERROR = 'upstream_error'.freeze
  OUTCOME_PROTOCOL_ERROR = 'protocol_error'.freeze
  OUTCOME_SESSION_ERROR = 'session_error'.freeze

  SESSION_ESTABLISHED = 'session_established'.freeze
  SESSION_CLOSE = 'session_close'.freeze

  class ProtocolError < StandardError; end
  class InvalidRequestError < ArgumentError; end

  class ResponseStreamError < StandardError
    attr_reader :event

    def initialize(event)
      @event = event
      super(event['error'] || 'Response stream failed')
    end
  end

  class UpstreamUnavailableError < StandardError; end
  class StreamCanceledError < StandardError; end

  module_function

  def parse_event(payload)
    return payload if payload.is_a?(Hash)

    data = JSON.parse(payload)
    data.is_a?(Hash) ? data : nil
  rescue JSON::ParserError
    nil
  end

  def normalize_headers(headers)
    (headers || {}).each_with_object({}) do |(key, value), normalized|
      next if key.nil?

      normalized[key.to_s.downcase] = normalize_header_value(value)
    end
  end

  def chunk_event(body)
    bytes = body.to_s.b
    utf8 = bytes.dup.force_encoding(Encoding::UTF_8)

    if utf8.valid_encoding?
      { 'type' => RESPONSE_CHUNK, 'body' => utf8 }
    else
      {
        'type' => RESPONSE_CHUNK,
        'body_encoding' => 'base64',
        'body_base64' => Base64.strict_encode64(bytes)
      }
    end
  end

  def end_event
    { 'type' => RESPONSE_END }
  end

  def error_event(message)
    { 'type' => RESPONSE_ERROR, 'error' => message.to_s }
  end

  def session_established_event(session_id:, receiver_service_id: nil, bind_host: nil, bind_port: nil, ingress_kind: nil)
    event = {
      'type' => SESSION_ESTABLISHED,
      'session_id' => session_id.to_s
    }
    event['receiver_service_id'] = receiver_service_id if receiver_service_id
    event['bind_host'] = bind_host if bind_host
    event['bind_port'] = bind_port.to_i if bind_port
    event['ingress_kind'] = ingress_kind if ingress_kind
    event
  end

  def session_close_event(reason: 'normal')
    { 'type' => SESSION_CLOSE, 'reason' => reason.to_s }
  end

  def cancel_payload(request_id:, service_id:, reason:, timestamp: nil)
    {
      'request_id' => request_id.to_s,
      'service_id' => service_id.to_s,
      'reason' => reason.to_s,
      'timestamp' => (timestamp || Time.now.utc.iso8601)
    }
  end

  def request_envelope(request_id:, reply_to:, operation:, payload:)
    {
      'type' => 'request',
      'request_id' => request_id.to_s,
      'reply_to' => reply_to,
      'operation' => operation.to_s,
      'payload' => payload
    }
  end

  def cancel_envelope(request_id:, service_id:, reason:, timestamp: nil)
    {
      'type' => 'cancel',
      'request_id' => request_id.to_s,
      'cancel' => cancel_payload(request_id:, service_id:, reason:, timestamp:)
    }
  end

  def chunk_body(event)
    if event['body_encoding'] == 'base64'
      Base64.strict_decode64(event.fetch('body_base64'))
    else
      event.fetch('body', '').to_s.b
    end
  end

  def wait_for_start_event(queue, timeout:)
    event = pop_event(queue, timeout: timeout)
    return nil unless event

    raise ProtocolError, "Expected #{RESPONSE_START}, got #{event['type'] || 'unknown'}" unless event['type'] == RESPONSE_START

    event
  end

  def wait_for_session_established(queue, timeout:)
    event = pop_event(queue, timeout: timeout)
    return nil unless event

    unless event['type'] == SESSION_ESTABLISHED
      raise ProtocolError, "Expected #{SESSION_ESTABLISHED}, got #{event['type'] || 'unknown'}"
    end

    event
  end

  def collect_non_streaming_body(queue, timeout:)
    body = String.new.b

    loop do
      event = pop_event(queue, timeout: timeout)
      return nil unless event

      case event['type']
      when RESPONSE_CHUNK
        body << chunk_body(event)
      when RESPONSE_END
        return body
      when RESPONSE_ERROR
        raise ProtocolError, "Unexpected event #{RESPONSE_ERROR}"
      else
        raise ProtocolError, "Unexpected event #{event['type']}"
      end
    end
  end

  def each_stream_chunk(queue, timeout:)
    loop do
      event = pop_event(queue, timeout: timeout)
      return :timeout unless event

      case event['type']
      when RESPONSE_CHUNK
        yield chunk_body(event)
      when RESPONSE_END, SESSION_CLOSE
        return :finished
      when RESPONSE_ERROR
        return event
      else
        raise ProtocolError, "Unexpected event #{event['type']}"
      end
    end
  end

  def pop_event(queue, timeout:)
    payload = queue.pop(timeout: timeout)
    return nil unless payload

    parse_event(payload) || raise(ProtocolError, 'Invalid response event payload')
  end
  private_class_method :pop_event

  def normalize_header_value(value)
    case value
    when Array then value.compact.map(&:to_s)
    when nil then ''
    else value.to_s
    end
  end
  private_class_method :normalize_header_value
end
