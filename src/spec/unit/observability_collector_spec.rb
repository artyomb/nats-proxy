require_relative "../spec_helper"
require_relative "../../nats_async_runtime"
require_relative "../../observability_collector"

RSpec.describe ObservabilityCollector do
  subject(:collector) { described_class.new(service_id: "srv-test", role: "requester", backend: "core") }

  it "reconstructs completed request cases" do
    collector.record_request_published(request_id: "req-1", subject: "to.req-1", method: "POST", path: "/api/echo")
    collector.record_response_event(request_id: "req-1", subject: "from.req-1", event: { "type" => "response_start", "status" => 200, "streaming" => false, "content_type" => "application/json" })
    collector.record_response_event(request_id: "req-1", subject: "from.req-1", event: { "type" => "response_chunk", "body" => "ok" })
    collector.record_response_event(request_id: "req-1", subject: "from.req-1", event: { "type" => "response_end" })

    row = collector.flow_cases("request_id" => "req-1").fetch(:cases).first

    expect(row).to include(status: "completed", outcome: "success", chunks_total: 1, method: "POST", path: "/api/echo")
  end

  it "derives canceled and timeout outcomes" do
    collector.record_cancel_observed(request_id: "req-cancel", reason: "client_closed", source_service_id: "srv-2", subject: "to.proxy.cancel.receiver-1.req-cancel")
    collector.record_response_event(request_id: "req-timeout", subject: "from.req-timeout", event: { "type" => "response_error", "error" => "Gateway Timeout" })

    cancel_event = collector.flow_events("outcome" => "canceled").fetch(:events).find { |event| event[:request_id] == "req-cancel" }
    expect(cancel_event).to include(subject: "to.proxy.cancel.receiver-1.req-cancel")
    expect(collector.flow_events("outcome" => "timeout").fetch(:events).map { |event| event[:request_id] }).to include("req-timeout")
  end

  it "aggregates flow-control events in cases and metrics" do
    collector.record_request_published(request_id: "req-flow", subject: "to.req-flow", method: "GET", path: "/stream")
    collector.record_flow_credit_sent(request_id: "req-flow", subject: "to.proxy.control.receiver.req-flow", direction: "response", bytes: 100)
    collector.record_flow_credit_received(request_id: "req-flow", subject: "to.proxy.control.receiver.req-flow", direction: "response", bytes: 100)
    collector.record_flow_credit_wait(request_id: "req-flow", direction: "response")
    collector.record_flow_credit_timeout(request_id: "req-flow", direction: "response")

    row = collector.flow_cases("request_id" => "req-flow").fetch(:cases).first
    metrics = collector.metrics

    expect(row).to include(credits_total: 2, credit_bytes_total: 200, flow_waits_total: 1, flow_timeouts_total: 1)
    expect(metrics.fetch(:flow_control)).to include(credits_total: 2, credit_bytes_total: 200, flow_waits_total: 1, flow_timeouts_total: 1)
  end

  it "retains only the ten most recently active request ids" do
    12.times do |index|
      collector.record_request_published(request_id: "req-#{index}", subject: "to.req-#{index}", method: "GET", path: "/#{index}")
    end

    request_ids = collector.flow_events.fetch(:events).map { |event| event[:request_id] }

    expect(request_ids).to eq((2..11).map { |index| "req-#{index}" })
  end

  it "makes a retained request id recent when it receives another event" do
    10.times do |index|
      collector.record_request_published(request_id: "req-#{index}", subject: "to.req-#{index}", method: "GET", path: "/#{index}")
    end
    collector.record_response_event(request_id: "req-0", subject: "from.req-0", event: { "type" => "response_chunk", "body" => "still active" })
    collector.record_request_published(request_id: "req-10", subject: "to.req-10", method: "GET", path: "/10")

    request_ids = collector.flow_events.fetch(:events).map { |event| event[:request_id] }.uniq

    expect(request_ids).to contain_exactly("req-0", *(2..10).map { |index| "req-#{index}" })
  end

  it "preserves retained event order and content when an older request id is evicted" do
    collector.record_request_published(request_id: "keep", subject: "to.keep", method: "POST", path: "/stream")
    collector.record_request_published(request_id: "evict", subject: "to.evict", method: "GET", path: "/old")
    8.times do |index|
      collector.record_request_published(request_id: "filler-#{index}", subject: "to.filler-#{index}", method: "GET", path: "/#{index}")
    end
    collector.record_response_event(
      request_id: "keep",
      subject: "from.keep",
      event: { "type" => "response_start", "status" => 201, "streaming" => true, "content_type" => "text/event-stream" }
    )
    collector.record_request_published(request_id: "new", subject: "to.new", method: "PUT", path: "/new")

    events = collector.flow_events.fetch(:events)

    expect(events.map { |event| event[:request_id] }).to eq([
      "keep", *(0..7).map { |index| "filler-#{index}" }, "keep", "new"
    ])
    expect(events.first).to include(type: "request_published", subject: "to.keep", meta: { method: "POST", path: "/stream" })
    expect(events[-2]).to include(
      type: "response_start",
      subject: "from.keep",
      meta: { status: 201, streaming: true, content_type: "text/event-stream" }
    )
    expect(events.last).to include(type: "request_published", subject: "to.new", meta: { method: "PUT", path: "/new" })
  end

  it "includes jetstream inspection failure as structured observability output" do
    nats_client = instance_double(
      "NatsAsyncRuntime",
      connection_snapshot: { status: :connected, connected: true, disconnected: false, closed: false, draining: false, last_error: nil, server_info: {} },
      jetstream_info: "error: consumer missing"
    )

    payload = collector.nats_runtime_payload(
      nats_client: nats_client,
      service_id: "srv-test",
      role: "requester",
      backend_mode: :jetstream,
      stream: "proxy",
      consumer: "nats-proxy",
      js_api_prefix: "$JS.API"
    )

    expect(payload.dig(:mode_details, :jetstream_available)).to be(false)
    expect(payload.dig(:mode_details, :inspection_error)).to include("consumer missing")
  end
end
