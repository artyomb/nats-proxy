require_relative "../spec_helper"
require_relative "../../bridge_core"

RSpec.describe BridgeCore do
  MessageDouble = Struct.new(:data, :header, :reply, :subject)

  class AckMessageDouble < MessageDouble
    attr_reader :ack_count, :nak_count, :term_count, :in_progress_count

    def initialize(data, header = {}, reply = nil, subject = nil)
      super
      @ack_count = 0
      @nak_count = 0
      @term_count = 0
      @in_progress_count = 0
    end

    def ack
      @ack_count += 1
    end

    def nak
      @nak_count += 1
    end

    def term
      @term_count += 1
    end

    def in_progress
      @in_progress_count += 1
    end
  end

  class QueueDouble
    attr_reader :jobs

    def initialize
      @jobs = []
    end

    def enqueue(job)
      @jobs << job
    end

    def push(job)
      @jobs << job
    end
  end

  class FakeNatsClient
    attr_reader :published

    def initialize
      @published = []
    end

    def publish(message, subject, _reply = nil, raw: false, headers: nil)
      @published << { message: message, subject: subject, raw: raw, headers: headers }
    end

    def subscribe(*) = 1
    def unsubscribe(*) = true
    def jetstream = raise "jetstream not expected"
  end

  let(:collector) do
    double(
      "collector",
      record_request_published: nil,
      record_response_event: nil,
      record_session_chunk: nil,
      record_cancel_published: nil,
      record_cancel_observed: nil
    )
  end

  def build_core(backend: :core, nats_client: FakeNatsClient.new)
    described_class.new(
      nats_client: nats_client,
      service_id: "srv-test",
      collector: collector,
      nats_backend: backend,
      config: {
        request_subject_root: "to.proxy",
        response_subject_root: "from.proxy",
        listen_subject: "to.proxy.requests.>",
        nats_stream: "proxy",
        consumer_name: "nats-proxy",
        queue_group: "nats-proxy",
        response_timeout: 1,
        stream_timeout: 1,
        max_inflight: 2,
        queue_size: 4
      }
    )
  end

  def decode_published_messages(client)
    client.published.map do |publish|
      message = publish.fetch(:message)
      message.is_a?(String) ? JSON.parse(message) : message
    end
  end

  it "publishes request envelopes to per-request subjects" do
    client = FakeNatsClient.new
    core = build_core(nats_client: client)

    context = core.bridge_request(request_id: "req-1", payload: { "method" => "GET", "path" => "/api/echo", "headers" => { "traceparent" => "tp" }, "body" => nil })

    expect(context.request_subject).to eq("to.proxy.requests.srv-test.req-1")
    expect(client.published.first[:message]).to include("type" => "request", "reply_to" => "from.proxy.responses.srv-test.req-1")
    expect(client.published.first[:headers]).to eq("traceparent" => "tp")
  end

  it "routes valid response events back into the pending request queue" do
    core = build_core
    context = core.bridge_request(request_id: "req-2", payload: { "method" => "GET", "path" => "/ok", "headers" => {}, "body" => nil })

    msg = MessageDouble.new({ "type" => "response_start", "status" => 200, "headers" => {}, "content_type" => "application/json", "streaming" => false }.to_json, {}, nil, "from.proxy.responses.srv-test.req-2")
    core.process_response_event(msg)

    expect(BridgeProtocol.wait_for_start_event(context.response_queue, timeout: 0.1)).to include("status" => 200)
  end

  it "publishes cancel signals only once" do
    client = FakeNatsClient.new
    core = build_core(nats_client: client)
    context = RequestContext.new(request_id: "req-3")
    context.request_subject = "to.proxy.requests.srv-test.req-3"

    expect(core.cancel_request(context, reason: "downstream_disconnect")).to be(true)
    expect(core.cancel_request(context, reason: "downstream_disconnect")).to be(false)
    expect(client.published.count { |publish| publish[:message].is_a?(Hash) && publish[:message]["type"] == "cancel" }).to eq(1)
  end

  it "routes downstream session data into the tunnel queue" do
    core = build_core
    context = RequestContext.new(request_id: "sess-1")
    context.tunnel_data_queue = Async::Queue.new
    core.instance_variable_get(:@pending_requests)["sess-1"] = context

    core.send(:dispatch_session_data, MessageDouble.new("pong".b, { "Nats-Frame-Type" => "session_data_downstream" }, nil, "from.proxy.sessions.downstream.srv-test.sess-1"))

    expect(context.tunnel_data_queue.dequeue(timeout: 0.1)).to eq("pong".b)
  end

  it "dispatches cancel envelopes immediately without enqueueing a worker job" do
    core = build_core(backend: :jetstream)
    context = RequestContext.new(request_id: "req-cancel", initial_state: "active")
    core.instance_variable_get(:@active_streams)["req-cancel"] = context
    queue = QueueDouble.new
    msg = AckMessageDouble.new(
      BridgeProtocol.cancel_envelope(request_id: "req-cancel", service_id: "srv-requester", reason: "downstream_disconnect").to_json,
      {},
      nil,
      "to.proxy.requests.srv-requester.req-cancel"
    )

    core.send(:dispatch_bridge_message, msg, queue:, manual_ack: true)

    expect(queue.jobs).to be_empty
    expect(context.cancel_requested?).to be(true)
    expect(context.cancel_reason).to eq("downstream_disconnect")
    expect(msg.ack_count).to eq(1)
    expect(msg.nak_count).to eq(0)
    expect(msg.term_count).to eq(0)
  end

  it "terminates malformed JetStream jobs instead of acking or retrying them" do
    core = build_core(backend: :jetstream)
    msg = AckMessageDouble.new("not-json", {}, nil, "to.proxy.requests.srv-requester.req-parse")

    Sync do
      core.send(:run_request_job, { msg:, data: :parse_error, manual_ack: true }, in_progress_interval: 0)
    end

    expect(msg.in_progress_count).to eq(1)
    expect(msg.term_count).to eq(1)
    expect(msg.ack_count).to eq(0)
    expect(msg.nak_count).to eq(0)
  end

  it "naks JetStream jobs when the handler raises a retryable error" do
    core = build_core(backend: :jetstream)
    core.register_handler("http_request") { raise StandardError, "boom" }
    data = BridgeProtocol.request_envelope(
      request_id: "req-nak",
      reply_to: "from.proxy.responses.srv-requester.req-nak",
      operation: "http_request",
      payload: { "method" => "GET", "path" => "/retry", "headers" => {}, "body" => nil }
    )
    msg = AckMessageDouble.new(data.to_json, {}, nil, "to.proxy.requests.srv-requester.req-nak")

    Sync do
      core.send(:run_request_job, { msg:, data:, manual_ack: true }, in_progress_interval: 0)
    end

    expect(msg.in_progress_count).to eq(1)
    expect(msg.nak_count).to eq(1)
    expect(msg.ack_count).to eq(0)
    expect(msg.term_count).to eq(0)
  end

  it "acks tcp_stream JetStream jobs immediately after session establishment and only once" do
    client = FakeNatsClient.new
    core = build_core(backend: :jetstream, nats_client: client)
    core.register_handler("tcp_stream") do |request_id:, **_kwargs, &emit|
      emit.call(BridgeProtocol.session_established_event(session_id: request_id))
      emit.call(BridgeProtocol.session_close_event(reason: "target_closed"))
      BridgeProtocol::OUTCOME_COMPLETED
    end
    data = BridgeProtocol.request_envelope(
      request_id: "sess-ack",
      reply_to: "from.proxy.responses.srv-requester.sess-ack",
      operation: "tcp_stream",
      payload: {
        "method" => "CONNECT",
        "host" => "127.0.0.1",
        "port" => 443,
        "requester_service_id" => "srv-requester"
      }
    )
    msg = AckMessageDouble.new(data.to_json, {}, nil, "to.proxy.requests.srv-requester.sess-ack")

    Sync do
      core.send(:run_request_job, { msg:, data:, manual_ack: true }, in_progress_interval: 0)
    end

    published_types = decode_published_messages(client).map { |event| event["type"] }
    expect(msg.ack_count).to eq(1)
    expect(msg.nak_count).to eq(0)
    expect(msg.term_count).to eq(0)
    expect(published_types).to eq(%w[session_established session_close])
  end

  it "publishes invalid envelope diagnostics for non-hash payloads" do
    client = FakeNatsClient.new
    core = build_core(nats_client: client)
    msg = MessageDouble.new(
      '"oops"',
      { "Reply-To" => "from.proxy.responses.srv-requester.req-invalid" },
      nil,
      "to.proxy.requests.srv-requester.req-invalid"
    )

    core.send(:process_bridge_request_data, msg, "oops")

    published_messages = decode_published_messages(client)

    expect(published_messages.map { |event| event["type"] }).to eq(%w[response_start response_chunk response_end])
    expect(BridgeProtocol.chunk_body(published_messages[1])).to include("Invalid request envelope")
  end

  it "publishes invalid envelope diagnostics for partial request payloads" do
    client = FakeNatsClient.new
    core = build_core(nats_client: client)
    msg = MessageDouble.new(
      {
        "type" => "request",
        "request_id" => "req-partial",
        "reply_to" => "from.proxy.responses.srv-requester.req-partial",
        "operation" => "http_request"
      }.to_json,
      {},
      nil,
      "to.proxy.requests.srv-requester.req-partial"
    )
    data = {
      "type" => "request",
      "request_id" => "req-partial",
      "reply_to" => "from.proxy.responses.srv-requester.req-partial",
      "operation" => "http_request"
    }

    core.send(:process_bridge_request_data, msg, data)

    published_messages = decode_published_messages(client)

    expect(published_messages.map { |event| event["type"] }).to eq(%w[response_start response_chunk response_end])
    expect(BridgeProtocol.chunk_body(published_messages[1])).to include("Missing payload")
  end

  it "serializes JetStream consumer duration fields as nanoseconds for the API" do
    core = build_core(backend: :jetstream)

    serialized = core.send(:consumer_config_for_api, ack_wait: 5, inactive_threshold: 61.5, max_ack_pending: 20)

    expect(serialized).to include(
      ack_wait: 5_000_000_000,
      inactive_threshold: 61_500_000_000,
      max_ack_pending: 20
    )
  end

  it "publishes cancel diagnostics for canceled http_request handlers" do
    client = FakeNatsClient.new
    core = build_core(nats_client: client)
    core.register_handler("http_request") do |**_kwargs, &emit|
      emit.call(
        "type" => BridgeProtocol::RESPONSE_START,
        "status" => 200,
        "headers" => { "content-type" => "application/json" },
        "content_type" => "application/json",
        "streaming" => false
      )
      BridgeProtocol::OUTCOME_CANCELED
    end
    msg = MessageDouble.new(
      "",
      {},
      nil,
      "to.proxy.requests.srv-requester.req-http-cancel"
    )
    data = BridgeProtocol.request_envelope(
      request_id: "req-http-cancel",
      reply_to: "from.proxy.responses.srv-requester.req-http-cancel",
      operation: "http_request",
      payload: { "method" => "GET", "path" => "/cancel", "headers" => {}, "body" => nil }
    )

    core.send(:process_bridge_request_data, msg, data)

    published_messages = decode_published_messages(client)

    expect(published_messages.map { |event| event["type"] }).to eq(%w[response_start response_error response_end])
    expect(published_messages.first).to include("receiver_service_id" => "srv-test")
    expect(published_messages[1]["error"]).to eq("stream canceled: cancel_requested")
  end

  it "publishes session_close for canceled tcp_stream handlers" do
    client = FakeNatsClient.new
    core = build_core(nats_client: client)
    core.register_handler("tcp_stream") do |request_id:, **_kwargs, &emit|
      emit.call(BridgeProtocol.session_established_event(session_id: request_id))
      BridgeProtocol::OUTCOME_CANCELED
    end
    msg = MessageDouble.new(
      "",
      {},
      nil,
      "to.proxy.requests.srv-requester.sess-cancel"
    )
    data = BridgeProtocol.request_envelope(
      request_id: "sess-cancel",
      reply_to: "from.proxy.responses.srv-requester.sess-cancel",
      operation: "tcp_stream",
      payload: {
        "method" => "CONNECT",
        "host" => "127.0.0.1",
        "port" => 443,
        "requester_service_id" => "srv-requester"
      }
    )

    core.send(:process_bridge_request_data, msg, data)

    published_messages = decode_published_messages(client)

    expect(published_messages.map { |event| event["type"] }).to eq(%w[session_established session_close])
    expect(published_messages.first).to include(
      "session_id" => "sess-cancel",
      "receiver_service_id" => "srv-test"
    )
    expect(published_messages.last).to include("reason" => "cancel_requested")
  end
end
