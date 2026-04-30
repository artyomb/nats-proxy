require_relative "../spec_helper"

RSpec.describe "Multi-instance bridge system", :system do
  def with_multi_service_cluster(mode:, upstream_url:, receiver_count:, requester_count:)
    with_leaf_topology_nats do |nats_context|
      stream_name = "multi-#{mode}-#{SecureRandom.hex(3)}"
      if mode == :jetstream
        bootstrap_jetstream!(nats_url: nats_context.fetch(:proxy_url), stream: stream_name)
        wait_for_jetstream_stream!(
          nats_url: nats_context.fetch(:proxy_url),
          stream: stream_name,
          js_api_prefix: "$JS.API"
        )
        wait_for_jetstream_stream!(
          nats_url: nats_context.fetch(:local_url),
          stream: stream_name,
          js_api_prefix: "JS.PROXY.API"
        )
      end

      receiver_consumer = "multi-#{mode}-receivers-#{SecureRandom.hex(3)}"
      receiver_queue = "multi-#{mode}-receivers-#{SecureRandom.hex(3)}"
      requester_consumer = "multi-#{mode}-requesters-#{SecureRandom.hex(3)}"
      requester_queue = "multi-#{mode}-requesters-#{SecureRandom.hex(3)}"

      receivers = Array.new(receiver_count) do |index|
        SystemHelpers::ExternalServiceProcess.new(
          name: "receiver-#{index}",
          port: free_port,
          env: build_service_env(
            role: "receiver",
            nats_url: nats_context.fetch(:proxy_url),
            mode:,
            service_id: "receiver-#{index}",
            upstream_url:,
            request_subject_root: "proxy",
            response_subject_root: "proxy",
            listen_subject: "proxy.requests.>",
            stream: stream_name,
            consumer_name: receiver_consumer,
            queue_group: receiver_queue,
            response_timeout: 2,
            stream_timeout: 2,
            receiver_max_inflight: 20,
            js_api_prefix: mode == :jetstream ? "$JS.API" : nil
          ),
          workdir: src_path
        )
      end

      requesters = Array.new(requester_count) do |index|
        SystemHelpers::ExternalServiceProcess.new(
          name: "requester-#{index}",
          port: free_port,
          env: build_service_env(
            role: "requester",
            nats_url: nats_context.fetch(:local_url),
            mode:,
            service_id: "requester-#{index}",
            request_subject_root: "to.proxy",
            response_subject_root: "from.proxy",
            listen_subject: "to.proxy.requests.>",
            stream: stream_name,
            consumer_name: requester_consumer,
            queue_group: requester_queue,
            response_timeout: 2,
            stream_timeout: 2,
            receiver_max_inflight: 20,
            js_api_prefix: mode == :jetstream ? "JS.PROXY.API" : nil
          ),
          workdir: src_path
        )
      end

      services = receivers + requesters
      services.each(&:start)
      services.each { |service| wait_for_runtime!(service) }

      yield(
        nats: nats_context,
        receivers:,
        requesters:,
        requester_urls: requesters.map(&:base_url),
        receiver_urls: receivers.map(&:base_url)
      )
    ensure
      services&.reverse_each(&:stop)
    end
  end

  def flow_events(service, query)
    http_get_json(service.base_url, "/observability/flows?#{URI.encode_www_form(query)}").fetch("events")
  end

  def receiver_by_service_id(receivers, service_id)
    receivers.find { |receiver| receiver.env_fetch("SERVICE_ID") == service_id }
  end

  it "routes CONNECT continuations and post-owner cancel to owner instances in core mode" do
    upstream = SystemHttpServer.new
    disconnects = Queue.new
    echo_server = SystemEchoServer.new

    upstream.on("/events") do |request|
      request.socket.write("HTTP/1.1 200 OK\r\n")
      request.socket.write("Content-Type: text/event-stream\r\n")
      request.socket.write("Connection: close\r\n\r\n")
      request.socket.write("data: one\n\n")
      request.socket.flush

      100.times do |index|
        sleep 0.05
        request.socket.write("data: #{index + 2}\n\n")
        request.socket.flush
      end

      :handled
    rescue IOError, Errno::ECONNRESET, Errno::EPIPE
      disconnects << :closed
      :handled
    end

    with_multi_service_cluster(mode: :core, upstream_url: upstream.base_url, requester_count: 1, receiver_count: 5) do |cluster|
      requester = cluster.fetch(:requesters).first
      receivers = cluster.fetch(:receivers)

      tunnel = open_connect_tunnel(
        host: "127.0.0.1",
        port: requester.port,
        target: "127.0.0.1:#{echo_server.port}"
      )
      tunnel.write("ping-owner")
      expect(tunnel.readpartial(64)).to eq("ping-owner")
      tunnel.close

      upstream_chunk = wait_until(timeout: 5) do
        flow_events(requester, event_type: "session_chunk", limit: 50).find do |event|
          event.dig("meta", "direction") == "upstream" &&
            event.fetch("subject", "").include?(".sessions.upstream.")
        end
      end

      upstream_subject = upstream_chunk.fetch("subject")
      expect(upstream_subject).to match(/\Ato\.proxy\.sessions\.upstream\.receiver-\d+\.[a-f0-9]+\z/)
      upstream_tokens = upstream_subject.split(".")
      owner_receiver_id = upstream_tokens[-2]
      session_id = upstream_tokens[-1]

      downstream_chunk = wait_until(timeout: 5) do
        flow_events(requester, event_type: "session_chunk", limit: 50).find do |event|
          event.dig("meta", "direction") == "downstream" &&
            event.fetch("subject", "") == "from.proxy.sessions.downstream.requester-0.#{session_id}"
        end
      end
      expect(downstream_chunk).not_to be_nil

      owner_receiver = receiver_by_service_id(receivers, owner_receiver_id)
      owner_events = flow_events(owner_receiver, event_type: "session_chunk", limit: 50)
      expect(owner_events.map { |event| event.fetch("subject", nil) }).to include(
        "proxy.sessions.upstream.#{owner_receiver_id}.#{session_id}"
      )

      non_owner_events = (receivers - [owner_receiver]).flat_map do |receiver|
        flow_events(receiver, event_type: "session_chunk", limit: 50)
      end
      expect(non_owner_events.map { |event| event.fetch("subject", nil) }).not_to include(
        "proxy.sessions.upstream.#{owner_receiver_id}.#{session_id}"
      )

      stream = open_http_socket(
        host: "127.0.0.1",
        port: requester.port,
        request_text: "GET /events HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
      )
      expect(read_http_head(stream)).to include("200")
      expect(stream.readpartial(32)).to include("data: one")
      stream.close
      expect(wait_until(timeout: 5) { disconnects.pop(true) rescue nil }).to eq(:closed)

      cancel_event = wait_until(timeout: 5) do
        flow_events(requester, event_type: "cancel_published", limit: 50).find do |event|
          event.fetch("subject", "").match?(/\Ato\.proxy\.cancel\.receiver-\d+\.[a-f0-9]+\z/) &&
            event.dig("meta", "routing_mode") == "owner"
        end
      end

      _, _, _, cancel_owner_id, request_id = cancel_event.fetch("subject").split(".")
      cancel_owner = receiver_by_service_id(receivers, cancel_owner_id)
      cancel_observed = wait_until(timeout: 5) do
        flow_events(cancel_owner, event_type: "cancel_observed", limit: 50).find do |event|
          event.fetch("subject", "") == "proxy.cancel.#{cancel_owner_id}.#{request_id}"
        end
      end
      expect(cancel_observed.dig("meta", "source_service_id")).to eq("requester-0")
    end
  ensure
    upstream&.stop
    echo_server&.stop
  end

  it "keeps requester session scopes isolated and distributes initial requests with five requesters and three receivers in core mode" do
    upstream = SystemHttpServer.new
    echo_server = SystemEchoServer.new

    upstream.on("/api/echo") do |request|
      {
        status: 200,
        headers: { "Content-Type" => "application/json" },
        body: [{ requester: request.headers["x-requester-id"], body: request.body }.to_json]
      }
    end

    with_multi_service_cluster(mode: :core, upstream_url: upstream.base_url, requester_count: 5, receiver_count: 3) do |cluster|
      requesters = cluster.fetch(:requesters)
      receivers = cluster.fetch(:receivers)

      requesters.each do |requester|
        service_id = requester.env_fetch("SERVICE_ID")
        tunnel = open_connect_tunnel(
          host: "127.0.0.1",
          port: requester.port,
          target: "127.0.0.1:#{echo_server.port}"
        )
        tunnel.write("ping-#{service_id}")
        expect(tunnel.readpartial(64)).to eq("ping-#{service_id}")
        tunnel.close
      end

      requesters.each do |requester|
        service_id = requester.env_fetch("SERVICE_ID")

        downstream_chunk = wait_until(timeout: 5) do
          flow_events(requester, event_type: "session_chunk", limit: 50).find do |event|
            event.dig("meta", "direction") == "downstream" &&
              event.fetch("subject", "").include?(".sessions.downstream.#{service_id}.")
          end
        end

        upstream_chunk = wait_until(timeout: 5) do
          flow_events(requester, event_type: "session_chunk", limit: 50).find do |event|
            event.dig("meta", "direction") == "upstream" &&
              event.fetch("subject", "").match?(/\Ato\.proxy\.sessions\.upstream\.receiver-[0-2]\.[a-f0-9]+\z/)
          end
        end

        expect(downstream_chunk.fetch("subject")).to match(
          /\Afrom\.proxy\.sessions\.downstream\.#{Regexp.escape(service_id)}\.[a-f0-9]+\z/
        )
        expect(upstream_chunk.fetch("subject")).to match(
          /\Ato\.proxy\.sessions\.upstream\.receiver-[0-2]\.[a-f0-9]+\z/
        )
      end

      30.times do |index|
        requester = requesters[index % requesters.size]
        service_id = requester.env_fetch("SERVICE_ID")
        response = http_request(
          base_url: requester.base_url,
          method: "post",
          path: "/api/echo",
          body: "payload-#{index}",
          headers: {
            "Content-Type" => "text/plain",
            "X-Requester-Id" => service_id
          }
        )

        expect(response.code).to eq("200")
        expect(JSON.parse(response.body)).to include(
          "requester" => service_id,
          "body" => "payload-#{index}"
        )
      end

      request_counts_by_receiver = receivers.to_h do |receiver|
        events = wait_until(timeout: 5) do
          found = flow_events(receiver, event_type: "request_published", limit: 100).select do |event|
            event.dig("meta", "path") == "/api/echo"
          end
          found unless found.empty?
        rescue RuntimeError
          []
        end

        [receiver.env_fetch("SERVICE_ID"), events.size]
      end

      active_receivers = request_counts_by_receiver.select { |_service_id, count| count.positive? }
      expect(active_receivers.size).to be > 1
    end
  ensure
    upstream&.stop
    echo_server&.stop
  end

  it "routes CONNECT continuations to the owner receiver in jetstream mode" do
    upstream = SystemHttpServer.new
    echo_server = SystemEchoServer.new

    with_multi_service_cluster(mode: :jetstream, upstream_url: upstream.base_url, requester_count: 1, receiver_count: 3) do |cluster|
      requester = cluster.fetch(:requesters).first
      receivers = cluster.fetch(:receivers)

      tunnel = open_connect_tunnel(
        host: "127.0.0.1",
        port: requester.port,
        target: "127.0.0.1:#{echo_server.port}"
      )
      tunnel.write("ping-jetstream-owner")
      expect(tunnel.readpartial(64)).to eq("ping-jetstream-owner")
      tunnel.close

      upstream_chunk = wait_until(timeout: 5) do
        flow_events(requester, event_type: "session_chunk", limit: 50).find do |event|
          event.dig("meta", "direction") == "upstream" &&
            event.fetch("subject", "").match?(/\Ato\.proxy\.sessions\.upstream\.receiver-[0-2]\.[a-f0-9]+\z/)
        end
      end

      upstream_subject = upstream_chunk.fetch("subject")
      upstream_tokens = upstream_subject.split(".")
      owner_receiver_id = upstream_tokens[-2]
      session_id = upstream_tokens[-1]
      owner_receiver = receiver_by_service_id(receivers, owner_receiver_id)

      owner_events = wait_until(timeout: 5) do
        events = flow_events(owner_receiver, event_type: "session_chunk", limit: 50)
        events if events.any? { |event| event.fetch("subject", nil) == "proxy.sessions.upstream.#{owner_receiver_id}.#{session_id}" }
      end
      expect(owner_events).not_to be_empty
    end
  ensure
    upstream&.stop
    echo_server&.stop
  end
end
