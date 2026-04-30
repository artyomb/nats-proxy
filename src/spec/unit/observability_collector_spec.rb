require_relative "../spec_helper"
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
