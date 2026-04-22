require_relative "../spec_helper"
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
    context.response_queue.enqueue(BridgeProtocol.session_established_event(session_id: "sess-1").to_json)
    allow(SecureRandom).to receive(:hex).and_return("sess-1")
    allow(core).to receive(:bridge_session_open).and_return(context)

    status, = bridge.dispatch_connect_request(env: { "REQUEST_URI" => "example.com:443" })

    expect(status).to eq(501)
    expect(core).to have_received(:close_session).with("sess-1", reason: "hijack_not_supported")
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
  end
end
