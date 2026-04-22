require_relative "../spec_helper"

RSpec.describe "SOCKS5 system", :system do
  shared_examples "SOCKS5 bridge flow" do |mode|
    it "establishes a no-auth SOCKS5 tunnel through requester and receiver in #{mode} mode" do
      upstream = SystemHttpServer.new
      echo_server = SystemEchoServer.new

      with_service_cluster(mode:, upstream_url: upstream.base_url, socks5: true) do |cluster|
        socket = open_socks5_tunnel(
          host: "127.0.0.1",
          port: cluster.fetch(:socks5_port),
          target_host: "localhost",
          target_port: echo_server.port
        )

        socket.write("ping-over-socks5")
        echoed = socket.readpartial(64)
        socket.close

        expect(echoed).to eq("ping-over-socks5")
      end
    ensure
      upstream&.stop
      echo_server&.stop
    end

    it "establishes an authenticated SOCKS5 tunnel with valid credentials in #{mode} mode" do
      upstream = SystemHttpServer.new
      echo_server = SystemEchoServer.new
      users_json = bcrypt_users_json(username: "alice", password: "secret")

      with_service_cluster(
        mode:,
        upstream_url: upstream.base_url,
        socks5: true,
        proxy_auth_users_json: users_json
      ) do |cluster|
        socket = open_socks5_tunnel(
          host: "127.0.0.1",
          port: cluster.fetch(:socks5_port),
          target_host: "localhost",
          target_port: echo_server.port,
          username: "alice",
          password: "secret"
        )

        socket.write("ping-authenticated")
        expect(socket.readpartial(64)).to eq("ping-authenticated")
        socket.close
      end
    ensure
      upstream&.stop
      echo_server&.stop
    end

    it "rejects invalid username/password credentials in #{mode} mode" do
      upstream = SystemHttpServer.new
      users_json = bcrypt_users_json(username: "alice", password: "secret")

      with_service_cluster(
        mode:,
        upstream_url: upstream.base_url,
        socks5: true,
        proxy_auth_users_json: users_json
      ) do |cluster|
        socket = TCPSocket.new("127.0.0.1", cluster.fetch(:socks5_port))
        socket.write([0x05, 0x01, 0x02].pack("C3"))
        expect(socket.read(2).bytes).to eq([0x05, 0x02])
        socket.write([0x01, 0x05].pack("C2") + "alice" + [0x05].pack("C") + "wrong")
        expect(socket.read(2).bytes).to eq([0x01, 0x01])
        socket.close
      end
    ensure
      upstream&.stop
    end
  end

  include_examples "SOCKS5 bridge flow", :core
  include_examples "SOCKS5 bridge flow", :jetstream

  it "returns command-not-supported for unsupported SOCKS5 commands at the wire level" do
    upstream = SystemHttpServer.new

    with_service_cluster(mode: :core, upstream_url: upstream.base_url, socks5: true) do |cluster|
      socket = TCPSocket.new("127.0.0.1", cluster.fetch(:socks5_port))
      negotiate_socks5(socket)

      reply = send_socks5_connect_request(
        socket,
        target_host: "127.0.0.1",
        target_port: 80,
        address_type: 0x01,
        command: 0x02
      )

      expect(reply.getbyte(1)).to eq(0x07)
      socket.close
    end
  ensure
    upstream&.stop
  end

  it "returns address-type-not-supported for unsupported SOCKS5 address types" do
    upstream = SystemHttpServer.new

    with_service_cluster(mode: :core, upstream_url: upstream.base_url, socks5: true) do |cluster|
      socket = TCPSocket.new("127.0.0.1", cluster.fetch(:socks5_port))
      negotiate_socks5(socket)

      socket.write([0x05, 0x01, 0x00, 0x09].pack("C4"))
      reply = socket.read(22)

      expect(reply.getbyte(1)).to eq(0x08)
      socket.close
    end
  ensure
    upstream&.stop
  end
end
