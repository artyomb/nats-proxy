require_relative "../spec_helper"
require_relative "../../bridge_core"
require_relative "../../proxy_auth"
require_relative "../../tcp_tunnel_bridge"
require_relative "../../socks5_server"

RSpec.describe Socks5Server do
  let(:core) { instance_double("BridgeCore", bridge_session_open: nil, release_pending: nil) }
  let(:tcp_tunnel_bridge) { instance_double("TcpTunnelBridge", run_requester_tunnel: nil) }

  it "selects no-auth method when proxy auth is disabled" do
    server, client = Socket.pair(:UNIX, :STREAM, 0)
    auth = ProxyAuth.new(enabled: false, users_json: nil)
    socks = described_class.new(core: core, tcp_tunnel_bridge: tcp_tunnel_bridge, host: "127.0.0.1", port: 0, nats_response_timeout: 0.2, proxy_auth: auth)

    worker = Thread.new { socks.send(:handle_client, server) }
    client.write([0x05, 0x01, 0x00].pack("C3"))

    expect(client.read(2).bytes).to eq([0x05, 0x00])
    worker.kill
    worker.join(1)
  ensure
    client.close unless client.closed?
    server.close unless server.closed?
  end

  it "rejects access during proxy auth safety lock" do
    server, client = Socket.pair(:UNIX, :STREAM, 0)
    auth = ProxyAuth.new(enabled: true, users_json: "{")
    socks = described_class.new(core: core, tcp_tunnel_bridge: tcp_tunnel_bridge, host: "127.0.0.1", port: 0, nats_response_timeout: 0.2, proxy_auth: auth)

    worker = Thread.new { socks.send(:handle_client, server) }
    client.write([0x05, 0x01, 0x02].pack("C3"))

    expect(client.read(2).bytes).to eq([0x05, 0xFF])
    worker.join(1)
  ensure
    client.close unless client.closed?
    server.close unless server.closed?
  end

  it "returns command-not-supported for unsupported SOCKS5 commands" do
    server, client = Socket.pair(:UNIX, :STREAM, 0)
    auth = ProxyAuth.new(enabled: false, users_json: nil)
    socks = described_class.new(core: core, tcp_tunnel_bridge: tcp_tunnel_bridge, host: "127.0.0.1", port: 0, nats_response_timeout: 0.2, proxy_auth: auth)

    worker = Thread.new { socks.send(:handle_client, server) }
    client.write([0x05, 0x01, 0x00].pack("C3"))
    expect(client.read(2).bytes).to eq([0x05, 0x00])

    client.write([0x05, 0x02, 0x00, 0x01, 127, 0, 0, 1, 0x1F, 0x90].pack("C4C4n"))
    reply = client.read(10)

    expect(reply.getbyte(1)).to eq(0x07)
    worker.join(1)
  ensure
    client.close unless client.closed?
    server.close unless server.closed?
  end
end
