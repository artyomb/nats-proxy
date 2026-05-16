require_relative "../spec_helper"
require_relative "../../bridge_core"
require_relative "../../nats_async_runtime"
require_relative "../../request_context"
require_relative "../../tcp_tunnel_bridge"

RSpec.describe TcpTunnelBridge do
  let(:core) do
    instance_double(
      "BridgeCore",
      bridge_outbound?: true,
      bridge_session_open: nil,
      release_pending: nil,
      close_session: nil,
      send_session_data: nil,
      send_session_downstream: nil,
      send_session_credit_downstream: nil,
      send_session_credit_upstream: nil,
      cancel_request: nil
    )
  end
  let(:nats_client) { instance_double("NatsAsyncRuntime", max_payload: 1_048_576) }
  let(:bridge) do
    described_class.new(
      core: core,
      nats_client: nats_client,
      nats_backend: :core,
      service_id: "srv-test",
      nats_response_timeout: 0.2,
      stream_response_timeout: 0.2
    )
  end

  it "returns 400 for invalid CONNECT target" do
    status, = bridge.dispatch_connect_request(env: { "REQUEST_URI" => "" })

    expect(status).to eq(400)
  end

  it "returns 501 when rack hijack is unavailable after session establishment" do
    context = RequestContext.new(request_id: "sess-1")
    context.response_queue.enqueue(BridgeProtocol.session_established_event(session_id: "sess-1", receiver_service_id: "receiver-1").to_json)
    allow(SecureRandom).to receive(:hex).and_return("sess-1")
    allow(core).to receive(:bridge_session_open).and_return(context)

    status, = bridge.dispatch_connect_request(env: { "REQUEST_URI" => "example.com:443" })

    expect(status).to eq(501)
    expect(core).to have_received(:close_session).with("sess-1", reason: "hijack_not_supported", receiver_service_id: "receiver-1")
  end

  it "emits session_established for receiver-side happy path" do
    upstream_queue = Async::Queue.new
    fake_local = instance_double("Addrinfo", ip_address: "127.0.0.1", ip_port: 12345)
    fake_socket = instance_double("TCPSocket", close: nil, local_address: fake_local)
    allow(bridge).to receive(:connect_target).and_return(fake_socket)
    allow(bridge).to receive(:pump_receiver_tunnel).and_return(BridgeProtocol::OUTCOME_COMPLETED)

    events = []
    outcome = bridge.handle_stream_request(
      payload: { "host" => "127.0.0.1", "port" => 9000, "requester_service_id" => "srv-requester", "ingress_kind" => "http_connect" },
      cancel_check: -> { false },
      emit_failure_response: true,
      request_id: "sess-2",
      upstream_queue: upstream_queue
    ) { |event| events << event }

    expect(outcome).to eq(BridgeProtocol::OUTCOME_COMPLETED)
    expect(events.first).to include("type" => "session_established", "session_id" => "sess-2", "bind_port" => 12345)
    expect(core).to have_received(:send_session_credit_upstream).with("sess-2", "srv-requester", 1_048_576)
  end

  it "returns downstream credit only after writing tunnel data to the client" do
    context = RequestContext.new(request_id: "sess-down")
    context.receiver_service_id = "receiver-1"
    context.tunnel_data_queue = Async::Queue.new
    context.tunnel_data_queue.enqueue("abc")
    context.response_queue.enqueue(BridgeProtocol.session_close_event(reason: "target_closed"))
    client_io = StringIO.new

    Sync do
      expect(bridge.send(:tunnel_writer_loop, client_io, context)).to eq(:finished)
    end

    expect(client_io.string).to eq("abc")
    expect(core).to have_received(:send_session_credit_downstream).with("sess-down", "receiver-1", 3)
  end

  it "does not close a CONNECT tunnel just because downstream data is temporarily idle" do
    context = RequestContext.new(request_id: "sess-idle")
    context.receiver_service_id = "receiver-1"
    context.tunnel_data_queue = Async::Queue.new
    client_io = StringIO.new

    Sync do |task|
      finished = false
      cancel = false
      runner = task.async do
        bridge.send(:tunnel_writer_loop, client_io, context, cancel_check: -> { cancel })
        finished = true
      end

      expect do
        task.with_timeout(0.2) { runner.wait }
      end.to raise_error(Async::TimeoutError)
      expect(finished).to be(false)

      cancel = true
      runner.wait
      expect(finished).to be(true)
    end
  end
end
