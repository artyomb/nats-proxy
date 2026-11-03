require "digest"
require_relative "../spec_helper"

RSpec.describe "HTTP bridge system", :system do
  shared_examples 'transparent HTTP bodies' do |mode|
    it "preserves small request bodies byte for byte in #{mode} mode" do
      upstream = SystemHttpServer.new
      upstream.on('/raw') do |request|
        { headers: { 'Content-Type' => 'text/plain' }, body: [Digest::SHA256.hexdigest(request.body)] }
      end
      bodies = ['false', 'null', '"hello"', " { \"value\": true }\n", '', 'ordinary text', '{"message":"Привет, мир! 🌍"}', 'обычный текст']
      with_service_cluster(mode:, upstream_url: upstream.base_url) do |cluster|
        bodies.each do |body|
          response = http_request(base_url: cluster.fetch(:requester_url), method: 'post', path: '/raw', body:, headers: { 'Content-Type' => 'application/json' })
          expect(response.code).to eq('200'), "body=#{body.inspect}: #{response.body}\n#{cluster.fetch(:requester).logs}\n#{cluster.fetch(:receiver).logs}"
          expect(response.body).to eq(Digest::SHA256.hexdigest(body)), "Changed HTTP body: #{body.inspect}"
        end
        expect(upstream.requests.map { |request| request.body.b }).to eq(bodies.map(&:b))
        events = http_get_json(cluster.fetch(:requester_url), '/observability/flows?event_type=request_published&include_nats_payload=true&limit=20').fetch('events')
        expect(events.map { |event| JSON.parse(event.fetch('nats_payload'))['operation'] }).to eq(['http_request'] * bodies.length)
      end
    ensure
      upstream&.stop
    end

    it "paces large ordinary responses within the encoded NATS limit in #{mode} mode" do
      upstream = SystemHttpServer.new
      body = "\0" * 1_200_000
      upstream.on('/large-response') do |_request|
        { headers: { 'Content-Type' => 'application/octet-stream' }, body: [body] }
      end
      with_service_cluster(mode:, upstream_url: upstream.base_url, nats_max_payload: 8192, response_timeout: 10, stream_timeout: 10) do |cluster|
        response = http_request(base_url: cluster.fetch(:requester_url), method: 'get', path: '/large-response')
        expect(response.code).to eq('200'), response.body.to_s.byteslice(0, 500)
        expect(response.body.bytesize).to eq(body.bytesize)
        expect(Digest::SHA256.hexdigest(response.body)).to eq(Digest::SHA256.hexdigest(body))
        row = observability_case_for(cluster.fetch(:requester_url), path: '/large-response', outcome: 'success')
        expect(row.fetch('credits_total')).to be > 0
        expect(row.fetch('flow_timeouts_total')).to eq(0)
        runtime = http_get_json(cluster.fetch(:requester_url), '/observability/nats')
        expect(runtime.dig('connection', 'last_error')).to be_nil
      end
    ensure
      upstream&.stop
    end
  end

  include_examples 'transparent HTTP bodies', :core
  include_examples 'transparent HTTP bodies', :jetstream

  shared_examples "large HTTP requests" do |mode|
    it "preserves upstream authentication defaults and client overrides in #{mode} mode" do
      upstream = SystemHttpServer.new
      basic_auth = 'Basic ' + Base64.strict_encode64('alice:fixture-password')
      upstream.on('/auth') do |request|
        auth = request.headers['authorization']
        { status: auth ? 200 : 401, body: [auth || 'missing'] }
      end
      upstream_url = upstream.base_url.sub('http://', 'http://alice:fixture-password@')

      with_service_cluster(mode:, upstream_url:, response_timeout: 10, stream_timeout: 10) do |cluster|
        ['{}', 'x' * 1_100_000].each do |body|
          [nil, 'Bearer client-token'].each do |authorization|
            headers = { 'Content-Type' => 'application/octet-stream' }
            headers['Authorization'] = authorization if authorization
            response = http_request(base_url: cluster.fetch(:requester_url), method: 'post', path: '/auth', body:, headers:)
            expect(response.code).to eq('200'), response.body
            expect(response.body).to eq(authorization || basic_auth)
          end
        end
        %i[requester_url receiver_url].each do |role_url|
          events = http_get_json(cluster.fetch(role_url), '/observability/flows?event_type=session_established&include_nats_payload=true&limit=10').fetch('events')
          expect(events).not_to be_empty
          captures = events.map { |event| JSON.parse(event.fetch('nats_payload')) }
          expect(captures.map { |event| event['http_headers'] }).to all(eq('[REDACTED]'))
          expect(events.to_json).not_to include(basic_auth)
        end
      end
    ensure
      upstream&.stop
    end

    it "limits concurrent HTTP work regardless of request transport in #{mode} mode" do
      upstream = SystemHttpServer.new
      entered = Queue.new
      release = Queue.new
      threads = []
      upstream.on('/concurrency') do |_request|
        entered << :started
        release.pop
        { body: ['done'] }
      end
      small = '{}'
      large = 'x' * 1_100_000

      with_service_cluster(mode:, upstream_url: upstream.base_url, receiver_max_inflight: 1, response_timeout: 10, stream_timeout: 10) do |cluster|
        [[small, small], [large, large], [large, small]].each do |bodies|
          threads = bodies.map do |body|
            Thread.new do
              http_request(base_url: cluster.fetch(:requester_url), method: 'post', path: '/concurrency', body:, headers: { 'Content-Type' => 'application/octet-stream' })
            end
          end
          wait_until(timeout: 5) { entered.size >= 1 }
          sleep 0.5
          expect(entered.size).to eq(1), "Overlapping HTTP requests: #{bodies.map(&:bytesize)}"
          entered.pop
          release << :done
          expect(wait_until(timeout: 5) { entered.pop(true) rescue nil }).to eq(:started)
          release << :done
          expect(Timeout.timeout(10) { threads.map(&:value).map(&:code) }).to eq(%w[200 200])
        end
      end
    ensure
      2.times { release << :done } if release
      threads&.each { |thread| thread.join(1) }
      upstream&.stop
    end

    it "paces a tunneled response until a paused HTTP client resumes reading in #{mode} mode" do
      socket = nil
      upstream = SystemHttpServer.new
      mutex = Mutex.new
      written = 0
      chunk = 'x' * 32_768
      total_bytes = chunk.bytesize * 512
      upstream.on('/pressure') do |request|
        request.socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF, 32_768)
        request.socket.write("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: #{total_bytes}\r\nConnection: close\r\n\r\n")
        512.times do
          request.socket.write(chunk)
          mutex.synchronize { written += chunk.bytesize }
        end
        :handled
      end

      with_service_cluster(mode:, upstream_url: upstream.base_url, response_timeout: 15, stream_timeout: 15) do |cluster|
        socket = TCPSocket.new('127.0.0.1', cluster.fetch(:requester).port)
        socket.write("POST /pressure HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/octet-stream\r\nContent-Length: 1100000\r\n\r\n#{'x' * 1_100_000}")
        last_written = 0
        last_change = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        paused_bytes = wait_until(timeout: 10, interval: 0.1) do
          current = mutex.synchronize { written }
          now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          last_change = now if current != last_written
          last_written = current
          current if current == total_bytes || (current.positive? && now - last_change >= 0.75)
        end
        expect(paused_bytes).to be < total_bytes

        head, body = Timeout.timeout(15) { read_http_response(socket) }
        expect(head).to include('200')
        expect(body.bytesize).to eq(total_bytes)
        expect(Digest::SHA256.hexdigest(body)).to eq(Digest::SHA256.hexdigest(chunk * 512))
      end
    ensure
      socket&.close unless socket&.closed?
      upstream&.stop
    end

    it "uses the TCP transport for large bodies and keeps small requests inline in #{mode} mode" do
      upstream = SystemHttpServer.new
      response_body = Random.new(42).bytes(1_200_000)
      upstream.on('/large') do |request|
        body = request.body.bytesize > 1_048_576 ? response_body : 'small'
        request.socket.write("HTTP/1.1 201 Created\r\nContent-Type: application/octet-stream\r\nX-Body-SHA256: #{Digest::SHA256.hexdigest(request.body)}\r\nContent-Length: #{body.bytesize}\r\nConnection: keep-alive\r\n\r\n")
        request.socket.write(body)
        request.socket.read(1)
        :handled
      end

      with_service_cluster(mode:, upstream_url: upstream.base_url, response_timeout: 10, stream_timeout: 10) do |cluster|
        bodies = ["{ \"image\": \"#{'A' * 1_200_000}\" }\n", Random.new(43).bytes(1_200_000), "field=#{'x' * 1_100_000}", '{"small":true}', "\xff\0".b]
        bodies.each_with_index do |body, index|
          content_type = index == 2 ? 'application/x-www-form-urlencoded' : 'application/octet-stream'
          response = http_request(
            base_url: cluster.fetch(:requester_url), method: 'post', path: '/large?original=query', body:,
            headers: { 'Content-Type' => content_type, 'Authorization' => 'Bearer test-token', 'traceparent' => '00-abc-xyz-01' }
          )
          expect(response.code).to eq('201'), response.body.to_s.byteslice(0, 500)
          expect(response['x-body-sha256']).to eq(Digest::SHA256.hexdigest(body))
          expected_body = body.bytesize > 1_048_576 ? response_body : 'small'
          expect(Digest::SHA256.hexdigest(response.body)).to eq(Digest::SHA256.hexdigest(expected_body))
        end
        requests = upstream.requests
        expect(requests.map(&:query)).to eq(['original=query'] * bodies.length)
        expect(requests.map(&:body)).to eq(bodies)
        expect(requests.map { |request| request.headers['authorization'] }).to eq(['Bearer test-token'] * bodies.length)
        events = http_get_json(cluster.fetch(:requester_url), '/observability/flows?event_type=request_published&include_nats_payload=true&limit=10')
        operations = events.fetch('events').map { |event| JSON.parse(event.fetch('nats_payload')).fetch('operation') }
        expect(operations).to eq(%w[tcp_stream tcp_stream tcp_stream http_request tcp_stream])
        completed = wait_until(timeout: 5) do
          cases = http_get_json(cluster.fetch(:requester_url), '/observability/cases?limit=10').fetch('cases')
          cases if cases.length == bodies.length && cases.all? { |row| row['status'] == 'completed' }
        end
        expect(completed.map { |row| row['outcome'] }).to all(eq('success'))
        metrics = http_get_json(cluster.fetch(:requester_url), '/observability/metrics')
        expect(metrics.dig('reconstruction_quality', 'success_ratio')).to eq(1.0)
        runtime = http_get_json(cluster.fetch(:requester_url), '/observability/nats')
        expect(runtime.dig('connection', 'server_info', 'max_payload')).to eq(1_048_576)
        expect(runtime.dig('connection', 'connected')).to eq(true)
      end
    ensure
      upstream&.stop
    end

    it "records a tunneled HTTP timeout on both sides and closes the upstream in #{mode} mode" do
      upstream = SystemHttpServer.new
      disconnects = Queue.new
      upstream.on('/large-timeout') do |request|
        request.socket.read(1)
        disconnects << :closed
        :handled
      end
      with_service_cluster(mode:, upstream_url: upstream.base_url, response_timeout: 1, stream_timeout: 1) do |cluster|
        response = http_request(base_url: cluster.fetch(:requester_url), method: 'post', path: '/large-timeout', body: 'x' * 1_100_000)
        expect(response.code).to eq('504'), response.body
        expect(wait_until(timeout: 5) { disconnects.pop(true) rescue nil }).to eq(:closed)
        %i[requester_url receiver_url].each do |role_url|
          row = observability_case_for(cluster.fetch(role_url), path: '/large-timeout', outcome: 'timeout')
          expect(row).to include('status' => 'timed_out', 'in_progress' => false)
        end
        metrics = http_get_json(cluster.fetch(:requester_url), '/observability/metrics')
        expect(metrics.dig('reconstruction_quality', 'success_ratio')).to eq(0.0)
      end
    ensure
      upstream&.stop
    end

    it "reports a missing receiver upstream without hanging in #{mode} mode" do
      with_service_cluster(mode:, upstream_url: nil, response_timeout: 5, stream_timeout: 5) do |cluster|
        ['{}', 'x' * 1_100_000].each do |body|
          response = http_request(base_url: cluster.fetch(:requester_url), method: 'post', path: '/missing', body:)
          expect(response.code).to eq('503'), "#{response.body}\nReceiver:\n#{cluster.fetch(:receiver).logs}\nRequester:\n#{cluster.fetch(:requester).logs}"
          expect(response.body).to include('Receiver is missing UPSTREAM_URL')
        end
      end
    end

    it "cancels the tunneled upstream when a streaming HTTP client disconnects in #{mode} mode" do
      socket = nil
      upstream = SystemHttpServer.new
      disconnects = Queue.new
      upstream.on('/large-events') do |request|
        request.socket.write("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n")
        loop do
          chunk = "data: #{'x' * 4096}\n\n"
          request.socket.write("#{chunk.bytesize.to_s(16)}\r\n#{chunk}\r\n")
          sleep 0.01
        end
      rescue IOError, Errno::ECONNRESET, Errno::EPIPE
        disconnects << :closed
        :handled
      end
      with_service_cluster(mode:, upstream_url: upstream.base_url, response_timeout: 5, stream_timeout: 5) do |cluster|
        socket = open_http_socket(
          host: '127.0.0.1', port: cluster.fetch(:requester).port,
          request_text: "POST /large-events HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1100000\r\n\r\n#{'x' * 1_100_000}"
        )
        expect(read_http_head(socket)).to include('200')
        expect(socket.readpartial(8192)).to include('data: ')
        socket.close
        expect(wait_until(timeout: 8) { disconnects.pop(true) rescue nil }).to eq(:closed)
        row = observability_case_for(cluster.fetch(:requester_url), path: '/large-events', outcome: 'canceled')
        expect(row.fetch('in_progress')).to eq(false)
      end
    ensure
      socket&.close unless socket&.closed?
      upstream&.stop
    end
  end

  include_examples "large HTTP requests", :core
  include_examples "large HTTP requests", :jetstream

  it "forwards a large absolute-form HTTPS request through the binary tunnel" do
    socket = nil
    key = OpenSSL::PKey::RSA.new(2048)
    certificate = OpenSSL::X509::Certificate.new
    certificate.subject = certificate.issuer = OpenSSL::X509::Name.parse('/CN=localhost')
    certificate.public_key = key.public_key
    certificate.not_before = Time.now - 60
    certificate.not_after = Time.now + 3600
    certificate.sign(key, OpenSSL::Digest.new('SHA256'))
    ssl_context = OpenSSL::SSL::SSLContext.new
    ssl_context.cert, ssl_context.key = certificate, key
    upstream = SystemHttpServer.new(ssl_context:)
    upstream.on('/secure') do |request|
      { status: 201, headers: { 'Content-Type' => 'text/plain' }, body: [Digest::SHA256.hexdigest(request.body)] }
    end
    body = Random.new(44).bytes(1_100_000)
    with_service_cluster(mode: :core, upstream_url: nil, response_timeout: 10, stream_timeout: 10) do |cluster|
      socket = open_http_socket(
        host: '127.0.0.1', port: cluster.fetch(:requester).port,
        request_text: "POST #{upstream.base_url}/secure?source=proxy HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/octet-stream\r\nContent-Length: #{body.bytesize}\r\n\r\n#{body}"
      )
      head, response_body = read_http_response(socket)
      expect(head).to include('201'), response_body
      expect(response_body).to eq(Digest::SHA256.hexdigest(body))
      expect(upstream.requests.map(&:query)).to eq(['source=proxy'])
    end
  ensure
    socket&.close unless socket&.closed?
    upstream&.stop
  end

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

  shared_examples "large streaming response bridge flow" do |mode|
    it "streams a response larger than the initial credit window in #{mode} mode" do
      upstream = SystemHttpServer.new
      socket = nil
      marker = "END-FLOW\n"
      payload = 20.times.map { |index| "flow-#{format('%04d', index)}:" + ("x" * 65_520) }.join << marker

      upstream.on("/large-flow") do |request|
        request.socket.write("HTTP/1.1 200 OK\r\n")
        request.socket.write("Content-Type: application/x-ndjson\r\n")
        request.socket.write("Connection: close\r\n\r\n")
        payload.bytes.each_slice(65_536) do |slice|
          request.socket.write(slice.pack("C*"))
          request.socket.flush
        end
        :handled
      end

      with_service_cluster(mode:, upstream_url: upstream.base_url, stream_timeout: 5) do |cluster|
        socket = open_http_socket(
          host: "127.0.0.1",
          port: cluster.fetch(:requester).port,
          request_text: "GET /large-flow HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
        )

        head, body = read_http_response(socket)

        expect(head).to include("HTTP/1.1 200")
        expect(head).to include("application/x-ndjson")
        expect(body.bytesize).to eq(payload.bytesize)
        expect(Digest::SHA256.hexdigest(body)).to eq(Digest::SHA256.hexdigest(payload))

        event_case = observability_case_for(cluster.fetch(:requester_url), path: "/large-flow", outcome: "success")
        expect(event_case.fetch("credits_total")).to be > 1
        expect(event_case.fetch("credit_bytes_total")).to be >= payload.bytesize
        expect(event_case.fetch("flow_timeouts_total")).to eq(0)

        receiver_events = wait_until(timeout: 5) do
          events = observability_flow_events(cluster.fetch(:receiver_url), request_id: event_case.fetch("request_id"), limit: 200)
          events if events.any? { |event| event["type"] == "flow_credit_received" && event.dig("meta", "direction") == "response" }
        rescue JSON::ParserError
          nil
        end
        expect(receiver_events.any? { |event| event["type"] == "flow_credit_timeout" }).to be(false)

        socket.close
      end
    ensure
      socket&.close unless socket&.closed?
      upstream&.stop
    end
  end

  include_examples "large streaming response bridge flow", :core
  include_examples "large streaming response bridge flow", :jetstream

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
