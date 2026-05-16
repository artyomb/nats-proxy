require_relative "../spec_helper"
require_relative "../../bridge_core"
require_relative "../../observability_collector"
require_relative "../../nats_async_runtime"
require_relative "../../proxy_auth"
require_relative "../../service_runtime"

RSpec.describe ServiceRuntime do
  let(:config) do
    {
      service_id: "srv-test",
      role: "requester",
      nats_url: "nats://127.0.0.1:4222",
      nats_mode: "core",
      nats_js_api_prefix: nil,
      request_subject_root: "to.proxy",
      response_subject_root: "from.proxy",
      listen_subject: "to.proxy.requests.>",
      nats_consumer_name: "nats-proxy",
      nats_queue_group: "nats-proxy",
      nats_stream: "proxy",
      nats_response_timeout: 5,
      stream_response_timeout: 5,
      max_inflight: 2,
      queue_size: 4,
      upstream_url: nil,
      socks5_enabled: false,
      socks5_listen_host: "127.0.0.1",
      socks5_listen_port: 1080
    }
  end
  let(:proxy_auth) { instance_double("ProxyAuth") }
  let(:nats_service) { instance_double("NatsAsyncRuntime", start: true, backend: :core, close: true, max_payload: 1_048_576) }
  let(:core) do
    instance_double(
      "BridgeCore",
      register_handler: nil,
      start_response_listener: nil,
      start_downstream_session_listener: nil,
      start_request_listener: nil,
      start_upstream_session_listener: nil,
      start_cancel_listener: nil,
      start_control_listener: nil,
      bridge_inbound?: false,
      bridge_outbound?: true,
      close: true
    )
  end
  let(:http_gateway) do
    Object.new.tap do |gateway|
      def gateway.handle_bridge_request(**_kwargs, &_block) = nil
    end
  end
  let(:tcp_bridge) do
    Object.new.tap do |bridge|
      def bridge.handle_stream_request(**_kwargs, &_block) = nil
    end
  end

  it "boots requester listeners and exposes ready boot status" do
    runtime = described_class.new(config: config, proxy_auth: proxy_auth)
    allow(NatsAsyncRuntime).to receive(:new).and_return(nats_service)
    allow(described_class).to receive(:new).and_call_original
    runtime.instance_variable_set(:@nats_service, nats_service)
    allow(runtime).to receive(:build_core).and_return(core)
    allow(runtime).to receive(:build_http_gateway).and_return(http_gateway)
    allow(runtime).to receive(:build_tcp_tunnel_bridge).and_return(tcp_bridge)
    allow(runtime).to receive(:build_socks5_server).and_return(nil)

    Sync { |task| runtime.boot_once(task) }

    expect(core).to have_received(:start_response_listener)
    expect(core).to have_received(:start_downstream_session_listener)
    expect(runtime.boot_status_payload).to include(state: :ready, role: "requester", backend: :core, error: nil)
  end

  it "builds requester observability consumer suffix for jetstream" do
    runtime = described_class.new(config: config, proxy_auth: proxy_auth)
    runtime.instance_variable_set(:@backend, :jetstream)

    expect(runtime.observability_consumer).to eq("nats-proxy-responses-srv-test")
  end

  it "derives flow window settings from the NATS max payload" do
    runtime = described_class.new(config: config, proxy_auth: proxy_auth)
    runtime.instance_variable_set(:@nats_service, nats_service)

    expect(runtime.send(:flow_window_config)).to include(
      flow_chunk_size: 32_768,
      flow_initial_window_bytes: FlowCreditWindow.default_initial_bytes(chunk_size: 32_768),
      flow_credit_batch_bytes: FlowCreditWindow.default_batch_bytes(chunk_size: 32_768),
      flow_max_window_bytes: FlowCreditWindow.default_max_bytes(chunk_size: 32_768),
      flow_credit_wait_timeout: 5.0
    )
  end
end
