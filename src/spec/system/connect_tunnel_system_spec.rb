require "digest"
require_relative "../spec_helper"

RSpec.describe "CONNECT tunnel system", :system do
  class ClosingOnceServer
    attr_reader :port

    def initialize(payload:)
      @payload = payload
      @server = TCPServer.new("127.0.0.1", 0)
      @port = @server.addr[1]
      @thread = Thread.new { accept_loop }
    end

    def stop
      @server.close
      @thread.join(1)
    rescue IOError
      nil
    end

    private

    def accept_loop
      loop do
        socket = @server.accept
        Thread.new(socket) do |client|
          client.write(@payload)
          client.flush
          client.close
        rescue IOError, Errno::ECONNRESET, Errno::EPIPE
          nil
        end
      end
    rescue IOError, Errno::EBADF
      nil
    end
  end

  shared_examples "CONNECT bridge flow" do |mode|
    it "establishes a bidirectional CONNECT tunnel through requester and receiver in #{mode} mode" do
      upstream = SystemHttpServer.new
      echo_server = SystemEchoServer.new

      with_service_cluster(mode:, upstream_url: upstream.base_url) do |cluster|
        socket = open_connect_tunnel(
          host: "127.0.0.1",
          port: cluster.fetch(:requester).port,
          target: "127.0.0.1:#{echo_server.port}"
        )

        socket.write("ping-over-connect")
        echoed = socket.readpartial(64)
        socket.close

        expect(echoed).to eq("ping-over-connect")

        cases = wait_until(timeout: 5) do
          payload = http_get_json(cluster.fetch(:requester_url), "/observability/cases?limit=5")
          payload.fetch("cases").find { |item| item["path"] == "127.0.0.1:#{echo_server.port}" }
        end

        expect(cases.fetch("streaming")).to eq(false)
        expect(cases.fetch("subject")).to include(".requests.")
        expect(cases.fetch("credits_total")).to be > 0
      end
    ensure
      upstream&.stop
      echo_server&.stop
    end

    it "returns a controlled upstream failure when receiver cannot connect to target in #{mode} mode" do
      upstream = SystemHttpServer.new

      with_service_cluster(mode:, upstream_url: upstream.base_url, response_timeout: 0.5) do |cluster|
        socket = open_http_socket(
          host: "127.0.0.1",
          port: cluster.fetch(:requester).port,
          request_text: "CONNECT 127.0.0.1:1 HTTP/1.1\r\nHost: 127.0.0.1:1\r\n\r\n"
        )

        head, body = read_http_response(socket)

        expect(head).to include("HTTP/1.1 504")
        expect(body).to include("Session establishment timeout")
      end
    ensure
      upstream&.stop
    end

    it "propagates target-side close after the CONNECT tunnel has been established in #{mode} mode" do
      upstream = SystemHttpServer.new
      closing_server = ClosingOnceServer.new(payload: "server-bye")

      with_service_cluster(mode:, upstream_url: upstream.base_url, stream_timeout: 1) do |cluster|
        socket = open_connect_tunnel(
          host: "127.0.0.1",
          port: cluster.fetch(:requester).port,
          target: "127.0.0.1:#{closing_server.port}"
        )

        expect(socket.readpartial(64)).to eq("server-bye")
        expect { socket.readpartial(64) }.to raise_error(EOFError)
        socket.close

        closed_case = wait_until(timeout: 5) do
          payload = http_get_json(cluster.fetch(:requester_url), "/observability/cases?limit=10")
          payload.fetch("cases").find do |item|
            item["path"] == "127.0.0.1:#{closing_server.port}" && item["outcome"] == "success"
          end
        rescue JSON::ParserError
          nil
        end

        expect(closed_case).to include("status" => "completed", "outcome" => "success")
      end
    ensure
      upstream&.stop
      closing_server&.stop
    end
  end

  include_examples "CONNECT bridge flow", :core
  include_examples "CONNECT bridge flow", :jetstream

  shared_examples "large CONNECT bridge flow" do |mode|
    it "echoes a CONNECT payload larger than the initial credit window in #{mode} mode" do
      upstream = SystemHttpServer.new
      echo_server = SystemEchoServer.new
      socket = nil
      payload = 20.times.map { |index| "connect-flow-#{format('%04d', index)}:" + ("x" * 65_512) }.join

      with_service_cluster(mode:, upstream_url: upstream.base_url, stream_timeout: 5) do |cluster|
        socket = open_connect_tunnel(
          host: "127.0.0.1",
          port: cluster.fetch(:requester).port,
          target: "127.0.0.1:#{echo_server.port}"
        )

        writer = Thread.new do
          socket.write(payload)
          socket.flush
        end
        echoed = read_socket_bytes(socket, bytes: payload.bytesize, timeout: 10, chunk_size: 8_192, pause: 0.002)
        expect(writer.join(2)).to eq(writer)
        socket.close

        expect(echoed.bytesize).to eq(payload.bytesize)
        expect(Digest::SHA256.hexdigest(echoed)).to eq(Digest::SHA256.hexdigest(payload))

        event_case = observability_case_for(
          cluster.fetch(:requester_url),
          path: "127.0.0.1:#{echo_server.port}",
          timeout: 10
        )
        expect(event_case.fetch("outcome")).not_to eq("timeout")

        flow_events = wait_until(timeout: 5) do
          events = observability_flow_events(cluster.fetch(:requester_url), request_id: event_case.fetch("request_id"), limit: 200)
          directions = events.map { |event| event.dig("meta", "direction") }.compact
          credit_bytes = events.sum { |event| %w[flow_credit_sent flow_credit_received].include?(event["type"]) ? event.dig("meta", "bytes").to_i : 0 }
          events if directions.include?("upstream") && directions.include?("downstream") && credit_bytes >= payload.bytesize
        rescue JSON::ParserError
          nil
        end
        expect(flow_events.any? { |event| event["type"] == "flow_credit_timeout" }).to be(false)
      end
    ensure
      socket&.close unless socket&.closed?
      upstream&.stop
      echo_server&.stop
    end
  end

  include_examples "large CONNECT bridge flow", :core
  include_examples "large CONNECT bridge flow", :jetstream

  it "rejects malformed CONNECT targets with HTTP 400" do
    upstream = SystemHttpServer.new

    with_service_cluster(mode: :core, upstream_url: upstream.base_url) do |cluster|
      socket = open_http_socket(
        host: "127.0.0.1",
        port: cluster.fetch(:requester).port,
        request_text: "CONNECT :443 HTTP/1.1\r\nHost: :443\r\n\r\n"
      )

      head, body = read_http_response(socket)

      expect(head).to include("HTTP/1.1 400")
      expect(body).to include("Invalid CONNECT target")
    end
  ensure
    upstream&.stop
  end

end
