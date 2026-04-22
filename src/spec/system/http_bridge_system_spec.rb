require_relative "../spec_helper"

RSpec.describe "HTTP bridge system", :system do
  it "proxies a non-streaming JSON request through core mode and exposes success in observability" do
    upstream = SystemHttpServer.new
    upstream.on("/api/echo") do |request|
      {
        status: 201,
        headers: { "Content-Type" => "application/json", "X-Upstream" => "ok" },
        body: [{ path: request.path, query: request.query, body: JSON.parse(request.body), trace: request.headers["traceparent"] }.to_json]
      }
    end

    with_service_cluster(mode: :core, upstream_url: upstream.base_url) do |cluster|
      response = http_request(
        base_url: cluster.fetch(:requester_url),
        method: "post",
        path: "/api/echo?foo=bar",
        body: { "hello" => "world" }.to_json,
        headers: { "Content-Type" => "application/json", "traceparent" => "00-abc-xyz-01" }
      )

      if response.code != "201"
        requester_cases = http_get_json(cluster.fetch(:requester_url), "/observability/cases?limit=10")
        receiver_cases = http_get_json(cluster.fetch(:receiver_url), "/observability/cases?limit=10")
        receiver_flows = http_get_json(cluster.fetch(:receiver_url), "/observability/flows?limit=5")
        raise <<~MSG
          unexpected response #{response.code}: #{response.body}
          upstream_requests=#{upstream.requests.map { |req| [req.method, req.path, req.query] }.inspect}
          requester_case=#{requester_cases.fetch("cases").first}
          receiver_case=#{receiver_cases.fetch("cases").first}
          receiver_last_subject=#{receiver_flows.fetch("events").last&.fetch("subject", nil)}
        MSG
      end

      expect(response.code).to eq("201")
      expect(response["x-upstream"]).to eq("ok")
      expect(JSON.parse(response.body)).to include(
        "path" => "/api/echo",
        "query" => "foo=bar",
        "body" => { "hello" => "world" },
        "trace" => "00-abc-xyz-01"
      )

      cases = http_get_json(cluster.fetch(:requester_url), "/observability/cases?limit=5")
      expect(cases.fetch("cases").first).to include(
        "status" => "completed",
        "outcome" => "success",
        "method" => "POST",
        "path" => "/api/echo?foo=bar"
      )
    end
  ensure
    upstream&.stop
  end

  it "proxies a non-streaming JSON request through jetstream mode" do
    upstream = SystemHttpServer.new
    upstream.on("/v1/models") do |_request|
      {
        status: 200,
        headers: { "Content-Type" => "application/json" },
        body: [{ data: [{ id: "qwen3:30b" }] }.to_json]
      }
    end

    with_service_cluster(mode: :jetstream, upstream_url: upstream.base_url) do |cluster|
      response = http_request(base_url: cluster.fetch(:requester_url), method: "get", path: "/v1/models")

      expect(response.code).to eq("200")
      expect(JSON.parse(response.body)).to eq("data" => [{ "id" => "qwen3:30b" }])

      runtime = http_get_json(cluster.fetch(:requester_url), "/observability/nats")
      expect(runtime.fetch("backend")).to eq("jetstream")
      expect(runtime.dig("mode_details", "jetstream_available")).to eq(true)
    end
  ensure
    upstream&.stop
  end

  it "reports cancel when the downstream client disconnects during SSE in jetstream mode" do
    upstream = SystemHttpServer.new
    disconnects = Queue.new

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

    with_service_cluster(mode: :jetstream, upstream_url: upstream.base_url, stream_timeout: 1) do |cluster|
      socket = open_http_socket(
        host: "127.0.0.1",
        port: cluster.fetch(:requester).port,
        request_text: "GET /events HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
      )

      head = read_http_head(socket)
      first_chunk = socket.readpartial(32)
      socket.close

      expect(head).to include("200")
      expect(first_chunk).to include("data: one")
      expect(wait_until(timeout: 5) { disconnects.pop(true) rescue nil }).to eq(:closed)

      cases = wait_until(timeout: 5) do
        payload = http_get_json(cluster.fetch(:requester_url), "/observability/cases?limit=5")
        found = payload.fetch("cases").find { |item| item["path"] == "/events" }
        found if found && found["outcome"] == "canceled"
      rescue JSON::ParserError
        nil
      end

      expect(cases).to include("status" => "canceled", "outcome" => "canceled")
    end
  ensure
    upstream&.stop
  end

  it "forwards standards-compliant proxy requests with an absolute-form request target" do
    upstream = SystemHttpServer.new

    upstream.on("/proxy-target") do |request|
      {
        status: 200,
        headers: { "Content-Type" => "application/json" },
        body: [{ path: request.path, query: request.query, via: "absolute-form" }.to_json]
      }
    end

    with_service_cluster(mode: :core, upstream_url: "http://default.invalid") do |cluster|
      socket = open_http_socket(
        host: "127.0.0.1",
        port: cluster.fetch(:requester).port,
        request_text: "GET http://127.0.0.1:#{upstream.port}/proxy-target?source=absolute HTTP/1.1\r\nHost: 127.0.0.1:#{upstream.port}\r\n\r\n"
      )

      head, body = read_http_response(socket)

      expect(head).to include("HTTP/1.1 200")
      expect(JSON.parse(body)).to eq(
        "path" => "/proxy-target",
        "query" => "source=absolute",
        "via" => "absolute-form"
      )

      proxy_case = wait_until(timeout: 5) do
        payload = http_get_json(cluster.fetch(:requester_url), "/observability/cases?limit=10")
        payload.fetch("cases").find { |item| item["path"] == "#{upstream.base_url}/proxy-target?source=absolute" }
      rescue JSON::ParserError
        nil
      end

      expect(proxy_case).to include("outcome" => "success", "status" => "completed", "method" => "GET")
    end
  ensure
    upstream&.stop
  end

  it "forwards legacy proxy requests detected via Host and Proxy-Connection headers" do
    upstream = SystemHttpServer.new

    upstream.on("/proxy-target") do |request|
      {
        status: 200,
        headers: { "Content-Type" => "application/json" },
        body: [{ path: request.path, query: request.query, via: "legacy-heuristic" }.to_json]
      }
    end

    with_service_cluster(mode: :core, upstream_url: "http://default.invalid") do |cluster|
      socket = open_http_socket(
        host: "127.0.0.1",
        port: cluster.fetch(:requester).port,
        request_text: "GET /proxy-target?source=legacy HTTP/1.1\r\nHost: 127.0.0.1:#{upstream.port}\r\nProxy-Connection: keep-alive\r\n\r\n"
      )

      head, body = read_http_response(socket)

      expect(head).to include("HTTP/1.1 200")
      expect(JSON.parse(body)).to eq(
        "path" => "/proxy-target",
        "query" => "source=legacy",
        "via" => "legacy-heuristic"
      )

      proxy_case = wait_until(timeout: 5) do
        payload = http_get_json(cluster.fetch(:requester_url), "/observability/cases?limit=10")
        payload.fetch("cases").find { |item| item["path"] == "#{upstream.base_url}/proxy-target?source=legacy" }
      rescue JSON::ParserError
        nil
      end

      expect(proxy_case).to include("outcome" => "success", "status" => "completed", "method" => "GET")
    end
  ensure
    upstream&.stop
  end

  it "returns 503 when accessed directly without bridge outbound or upstream availability" do
    with_service_cluster(mode: :core, upstream_url: nil) do |cluster|
      response = http_request(base_url: cluster.fetch(:receiver_url), method: "get", path: "/api/echo")

      expect(response.code).to eq("503")
      expect(JSON.parse(response.body)).to eq(
        "error" => "Service Unavailable",
        "message" => "No bridge or upstream available"
      )
    end
  end

  it "uses direct upstream fallback when the receiver is accessed directly over HTTP" do
    upstream = SystemHttpServer.new

    upstream.on("/api/direct") do |request|
      {
        status: 200,
        headers: { "Content-Type" => "application/json" },
        body: [{ mode: "direct", path: request.path, query: request.query }.to_json]
      }
    end

    with_service_cluster(mode: :core, upstream_url: upstream.base_url) do |cluster|
      response = http_request(base_url: cluster.fetch(:receiver_url), method: "get", path: "/api/direct?via=receiver")

      expect(response.code).to eq("200")
      expect(JSON.parse(response.body)).to eq(
        "mode" => "direct",
        "path" => "/api/direct",
        "query" => "via=receiver"
      )
    end
  ensure
    upstream&.stop
  end

  it "returns a controlled 503 response when upstream is unavailable before response_start" do
    with_service_cluster(mode: :core, upstream_url: "http://127.0.0.1:1", response_timeout: 1, stream_timeout: 1) do |cluster|
      response = http_request(base_url: cluster.fetch(:requester_url), method: "get", path: "/api/unavailable")

      expect(response.code).to eq("503")
      expect(JSON.parse(response.body).fetch("error")).to match(/\AUpstream unavailable:/)
    end
  end

  it "streams SSE events through requester and receiver until normal completion" do
    upstream = SystemHttpServer.new
    socket = nil

    upstream.on("/events") do |request|
      request.socket.write("HTTP/1.1 200 OK\r\n")
      request.socket.write("Content-Type: text/event-stream\r\n")
      request.socket.write("Cache-Control: no-cache\r\n")
      request.socket.write("Connection: close\r\n\r\n")
      request.socket.write("data: one\n\n")
      request.socket.write("data: two\n\n")
      request.socket.write("data: three\n\n")
      request.socket.flush
      :handled
    end

    with_service_cluster(mode: :core, upstream_url: upstream.base_url, stream_timeout: 1) do |cluster|
      socket = open_http_socket(
        host: "127.0.0.1",
        port: cluster.fetch(:requester).port,
        request_text: "GET /events HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
      )

      head = read_http_head(socket)
      body = +""
      Timeout.timeout(5) do
        body << socket.readpartial(1024) until body.include?("data: three\n\n")
      end

      expect(head).to include("HTTP/1.1 200")
      expect(head).to include("Content-Type: text/event-stream").or include("content-type: text/event-stream")
      expect(body).to include("data: one\n\n")
      expect(body).to include("data: two\n\n")
      expect(body).to include("data: three\n\n")

      event_case = wait_until(timeout: 5) do
        payload = http_get_json(cluster.fetch(:requester_url), "/observability/cases?limit=5")
        found = payload.fetch("cases").find { |item| item["path"] == "/events" }
        found if found && found["outcome"] == "success"
      rescue JSON::ParserError
        nil
      end

      expect(event_case).to include("status" => "completed", "outcome" => "success")
      socket.close
    end
  ensure
    socket&.close unless socket&.closed?
    upstream&.stop
  end

  it "streams NDJSON responses through requester and receiver until normal completion" do
    upstream = SystemHttpServer.new
    socket = nil

    upstream.on("/feed") do |request|
      request.socket.write("HTTP/1.1 200 OK\r\n")
      request.socket.write("Content-Type: application/x-ndjson\r\n")
      request.socket.write("Connection: close\r\n\r\n")
      request.socket.write({ token: "one" }.to_json << "\n")
      request.socket.write({ token: "two" }.to_json << "\n")
      request.socket.write({ token: "three" }.to_json << "\n")
      request.socket.flush
      :handled
    end

    with_service_cluster(mode: :core, upstream_url: upstream.base_url, stream_timeout: 1) do |cluster|
      socket = open_http_socket(
        host: "127.0.0.1",
        port: cluster.fetch(:requester).port,
        request_text: "GET /feed HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
      )

      head = read_http_head(socket)
      body = +""
      Timeout.timeout(5) do
        body << socket.readpartial(1024) until body.include?({ token: "three" }.to_json << "\n")
      end

      expect(head).to include("HTTP/1.1 200")
      expect(head).to include("application/x-ndjson")
      lines = body.lines.map(&:strip).select { |line| line.start_with?("{") }.map { |line| JSON.parse(line) }
      expect(lines).to eq(
        [
          { "token" => "one" },
          { "token" => "two" },
          { "token" => "three" }
        ]
      )

      feed_case = wait_until(timeout: 5) do
        payload = http_get_json(cluster.fetch(:requester_url), "/observability/cases?limit=5")
        found = payload.fetch("cases").find { |item| item["path"] == "/feed" }
        found if found && found["outcome"] == "success"
      rescue JSON::ParserError
        nil
      end

      expect(feed_case).to include("status" => "completed", "outcome" => "success")
      socket.close
    end
  ensure
    socket&.close unless socket&.closed?
    upstream&.stop
  end
end
