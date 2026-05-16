require "faraday"
require_relative "../spec_helper"
require_relative "../../bridge_core"
require_relative "../../http_gateway"

RSpec.describe HttpGateway do
  FakeEnv = Struct.new(:status, :response_headers)
  FakeResponse = Struct.new(:status, :headers, :body)

  class FakeOptions
    attr_accessor :on_data
  end

  class FakeRequestWriter
    attr_reader :headers, :options
    attr_accessor :body

    def initialize
      @headers = {}
      @options = FakeOptions.new
    end
  end

  def build_connection(response:, chunks: [], error: nil, capture: nil)
    instance_double("Faraday::Connection").tap do |connection|
      allow(connection).to receive(:run_request) do |_method, _path, _body, _headers, &block|
        request = FakeRequestWriter.new
        capture[:request] = request if capture
        block.call(request)
        chunks.each { |chunk, env| request.options.on_data.call(chunk, chunk.bytesize, env) }
        raise error if error

        response
      end
    end
  end

  let(:core) do
    instance_double("BridgeCore", bridge_outbound?: false, bridge_request: nil, release_pending: nil, cancel_request: true, send_response_credit: nil)
  end

  it "uses direct upstream when bridge listener is absent and upstream is configured" do
    gateway = described_class.new(
      core: core,
      upstream_url: "http://upstream.test",
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 1,
      stream_response_timeout: 1
    )

    allow(gateway).to receive(:direct_upstream_request).and_call_original
    allow(gateway).to receive(:handle_bridge_request) do |**_kwargs, &emit|
      emit.call("type" => "response_start", "status" => 200, "headers" => { "content-type" => "application/json" }, "content_type" => "application/json", "streaming" => false)
      emit.call(BridgeProtocol.chunk_event("{\"ok\":true}"))
      emit.call(BridgeProtocol.end_event)
    end

    app = FakeRackApp.new(request: FakeRackRequest.new(method: "GET", path: "/api/echo"))
    body = gateway.dispatch_http_request(app: app, method: "GET")

    expect(body).to eq("{\"ok\":true}")
    expect(app.status).to eq(200)
  end

  it "renders streaming SSE responses and preserves chunks" do
    gateway = described_class.new(
      core: core,
      upstream_url: nil,
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 1,
      stream_response_timeout: 1
    )
    context = RequestContext.new(request_id: "req-stream")
    context.response_queue.enqueue({ "type" => "response_start", "status" => 200, "headers" => { "content-type" => "text/event-stream" }, "content_type" => "text/event-stream", "streaming" => true })
    context.response_queue.enqueue(BridgeProtocol.chunk_event("data: one\n\n"))
    context.response_queue.enqueue(BridgeProtocol.end_event)

    app = FakeRackApp.new(request: FakeRackRequest.new(method: "GET", path: "/stream"))
    gateway.send(:render_response, app: app, context: context)

    expect(app.status).to eq(200)
    expect(app.stream_output).to eq("data: one\n\n")
  end

  it "converts upstream absolute-form URL into a dedicated proxy connection target" do
    gateway = described_class.new(
      core: core,
      upstream_url: "http://default.test",
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 1,
      stream_response_timeout: 1
    )

    upstream_connection = instance_double("Faraday::Connection")
    allow(gateway).to receive(:build_upstream_connection).with("http://api.example.test:8080").and_return(upstream_connection)

    connection, path, original = gateway.send(:resolve_upstream_target, "http://api.example.test:8080/v1/models?limit=5", instance_double("Faraday::Connection"))

    expect(connection).to eq(upstream_connection)
    expect(path).to eq("/v1/models?limit=5")
    expect(original).to eq("http://api.example.test:8080/v1/models?limit=5")
  end

  it "reconstructs an absolute-form proxy target from protocol.http.request" do
    gateway = described_class.new(
      core: core,
      upstream_url: nil,
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 1,
      stream_response_timeout: 1
    )
    protocol_request = Struct.new(:scheme, :authority, :path, :request_target, :absolute_form_target).new(
      "http",
      "api.example.test:8080",
      "/v1/models?limit=5",
      "http://api.example.test:8080/v1/models?limit=5",
      true
    )
    request = FakeRackRequest.new(
      method: "GET",
      path: "/v1/models?limit=5"
    )
    request.env["protocol.http.request"] = protocol_request

    expect(gateway.proxy_forward_request?(request)).to eq(true)
    expect(gateway.send(:request_target, request)).to eq("http://api.example.test:8080/v1/models?limit=5")
  end

  it "reconstructs an absolute-form proxy target from proxy request headers" do
    gateway = described_class.new(
      core: core,
      upstream_url: nil,
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 1,
      stream_response_timeout: 1
    )
    request = FakeRackRequest.new(
      method: "GET",
      path: "/v1/models?limit=5"
    )
    request.env["HTTP_PROXY_CONNECTION"] = "keep-alive"
    request.env["HTTP_HOST"] = "api.example.test:8080"

    expect(gateway.proxy_forward_request?(request)).to eq(true)
    expect(gateway.send(:request_target, request)).to eq("http://api.example.test:8080/v1/models?limit=5")
  end

  it "emits response_error after streaming starts and upstream later fails" do
    env = FakeEnv.new(200, { "Content-Type" => "text/event-stream" })
    response = FakeResponse.new(200, { "Content-Type" => "text/event-stream" }, "")
    connection = build_connection(
      response: response,
      chunks: [["data: a\n\n", env]],
      error: Faraday::TimeoutError.new("timeout")
    )
    gateway = described_class.new(
      core: core,
      upstream_url: nil,
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 1,
      stream_response_timeout: 1
    )

    events = []
    outcome = gateway.proxy_upstream_request(connection: connection, method: "GET", path: "/stream") { |event| events << event }

    expect(outcome).to eq(BridgeProtocol::OUTCOME_UPSTREAM_ERROR)
    expect(events.map { |event| event["type"] }).to eq(%w[response_start response_chunk response_error response_end])
  end

  it "does not emit streaming response chunks before response credit is available" do
    env = FakeEnv.new(200, { "Content-Type" => "text/event-stream" })
    response = FakeResponse.new(200, { "Content-Type" => "text/event-stream" }, "")
    connection = build_connection(response: response, chunks: [["data: a\n\n", env]])
    gateway = described_class.new(
      core: core,
      upstream_url: nil,
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 1,
      stream_response_timeout: 1
    )
    window = FlowCreditWindow.new(max_bytes: 1024)
    events = []

    Sync do |task|
      worker = task.async do
        gateway.proxy_upstream_request(
          connection: connection,
          method: "GET",
          path: "/stream",
          request_id: "req-flow",
          response_credit_window: window
        ) { |event| events << event }
      end
      wait_until(timeout: 1) { events.any? }

      expect(events.map { |event| event["type"] }).to eq(%w[response_start])
      window.grant(32)
      worker.wait
    end

    expect(events.map { |event| event["type"] }).to eq(%w[response_start response_chunk response_end])
  end

  it "reports streaming response credit timeouts as timeout outcomes" do
    env = FakeEnv.new(200, { "Content-Type" => "text/event-stream" })
    response = FakeResponse.new(200, { "Content-Type" => "text/event-stream" }, "")
    connection = build_connection(response: response, chunks: [["data: a\n\n", env]])
    gateway = described_class.new(
      core: core,
      upstream_url: nil,
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 1,
      stream_response_timeout: 0.01
    )
    window = FlowCreditWindow.new(max_bytes: 1024)
    events = []

    Sync do
      outcome = gateway.proxy_upstream_request(
        connection: connection,
        method: "GET",
        path: "/stream",
        request_id: "req-flow-timeout",
        response_credit_window: window
      ) { |event| events << event }

      expect(outcome).to eq(BridgeProtocol::OUTCOME_TIMEOUT)
    end

    expect(events.map { |event| event["type"] }).to eq(%w[response_start response_error response_end])
  end

  it "sends response credit after writing streaming chunks downstream" do
    gateway = described_class.new(
      core: core,
      upstream_url: nil,
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 1,
      stream_response_timeout: 1
    )
    context = RequestContext.new(request_id: "req-stream")
    context.receiver_service_id = "receiver-1"
    context.response_queue.enqueue({ "type" => "response_start", "status" => 200, "headers" => { "content-type" => "text/event-stream" }, "content_type" => "text/event-stream", "streaming" => true, "receiver_service_id" => "receiver-1" })
    context.response_queue.enqueue(BridgeProtocol.chunk_event("data: one\n\n"))
    context.response_queue.enqueue(BridgeProtocol.end_event)

    app = FakeRackApp.new(request: FakeRackRequest.new(method: "GET", path: "/stream"))
    gateway.send(:render_response, app: app, context: context)

    expect(core).to have_received(:send_response_credit).with("req-stream", "receiver-1", 1_048_576)
    expect(core).to have_received(:send_response_credit).with("req-stream", "receiver-1", "data: one\n\n".bytesize)
  end
end
