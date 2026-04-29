# frozen_string_literal: true

require "base64"
require "json"
require "net/http"
require "thread"
require "uri"
require "securerandom"
require "bcrypt"
require "timeout"
require "ipaddr"
require "tmpdir"
require "nats-async"
require_relative "nats_server_helper"

class SystemHttpServer
  Request = Struct.new(:method, :target, :path, :query, :headers, :body, :socket, keyword_init: true)

  attr_reader :port

  def initialize
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.addr[1]
    @handlers = {}
    @requests = Queue.new
    @thread = Thread.new { accept_loop }
  end

  def base_url = "http://127.0.0.1:#{port}"

  def on(path, &block)
    @handlers[path] = block
  end

  def stop
    @server.close
    @thread.join(1)
  rescue IOError
    nil
  end

  def requests
    items = []
    items << @requests.pop(true) while true
  rescue ThreadError
    items
  end

  private

  def accept_loop
    loop do
      socket = @server.accept
      Thread.new(socket) { |client| handle_client(client) }
    end
  rescue IOError, Errno::EBADF
    nil
  end

  def handle_client(socket)
    request_line = socket.gets
    return unless request_line

    method, target, = request_line.split(" ", 3)
    headers = {}
    while (line = socket.gets)
      line = line.chomp
      break if line.empty?

      key, value = line.split(":", 2)
      headers[key.downcase] = value.to_s.strip
    end

    body = read_body(socket, headers)
    uri = URI.parse(target)
    path = uri.path.to_s.empty? ? "/" : uri.path
    request = Request.new(method:, target:, path:, query: uri.query.to_s, headers:, body:, socket:)
    @requests << request
    handler = @handlers.fetch(path)
    response = handler.call(request)
    return if response == :handled

    status = response.fetch(:status, 200)
    response_headers = response.fetch(:headers, {})
    parts = Array(response.fetch(:body, []))
    socket.write("HTTP/1.1 #{status} OK\r\n")
    response_headers.each { |key, value| socket.write("#{key}: #{value}\r\n") }
    socket.write("Content-Length: #{parts.sum { |part| part.to_s.bytesize }}\r\n") unless response_headers.keys.any? { |key| key.casecmp("content-length").zero? }
    socket.write("\r\n")
    parts.each { |part| socket.write(part.to_s) }
  rescue EOFError, IOError, Errno::ECONNRESET, Errno::EPIPE
    nil
  ensure
    socket.close unless socket.closed?
  end

  def read_body(socket, headers)
    size = headers["content-length"].to_i
    return "" if size <= 0

    socket.read(size)
  end
end

class SystemEchoServer
  attr_reader :port

  def initialize
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
        begin
          loop do
            client.write(client.readpartial(4096))
          end
        rescue EOFError, IOError, Errno::ECONNRESET, Errno::EPIPE
          nil
        ensure
          client.close unless client.closed?
        end
      end
    end
  rescue IOError, Errno::EBADF
    nil
  end
end

module SystemHelpers
  class ExternalServiceProcess
    attr_reader :name, :port, :log_path

    def initialize(name:, port:, env:, workdir:)
      @name = name
      @port = port
      @env = env
      @workdir = workdir
      @pid = nil
      @log_path = File.join(Dir.tmpdir, "#{name}-service-#{SecureRandom.hex(4)}.log")
    end

    def base_url = "http://127.0.0.1:#{port}"

    def start
      log_file = File.open(log_path, "a")
      @pid = Process.spawn(
        @env,
        "bundle", "exec", "rackup", "-s", "falcon", "-o", "127.0.0.1", "-p", port.to_s, "config.ru",
        chdir: @workdir,
        out: log_file,
        err: log_file
      )
      log_file.close
      self
    end

    def pid = @pid

    def alive?
      poll_exit_status
      !@pid.nil? && @exit_status.nil?
    end

    def exit_summary
      poll_exit_status
      return "still running" if @exit_status.nil?

      @exit_summary ||= begin
        termsig = @exit_status.termsig
        exits = @exit_status.exitstatus
        if termsig
          "signal=#{termsig}"
        else
          "exit=#{exits}"
        end
      end
    end

    def stop
      return unless @pid

      poll_exit_status
      return unless @exit_status.nil?

      Process.kill("TERM", @pid)
      Timeout.timeout(5) { Process.wait(@pid) }
    rescue Errno::ESRCH, Process::Waiter::Error, Timeout::Error
      Process.kill("KILL", @pid) rescue nil
      Process.wait(@pid) rescue nil
    ensure
      @exit_status = nil
      @exit_summary = nil
      @pid = nil
    end

    def logs
      File.exist?(log_path) ? File.read(log_path) : ""
    end

    private

    def poll_exit_status
      return @exit_status if @pid.nil? || @exit_status

      waited_pid = Process.waitpid(@pid, Process::WNOHANG)
      return nil unless waited_pid

      @exit_status = $?
    rescue Errno::ECHILD, Process::Waiter::Error
      @exit_status ||= $?
    end
  end

  def src_path = File.expand_path("../..", __dir__)

  def wait_until(timeout: 10, interval: 0.05)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      result = yield
      return result if result
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep interval
    end
  end

  def http_request(base_url:, method:, path:, body: nil, headers: {})
    uri = URI.join(base_url, path)
    request_class = Net::HTTP.const_get(method.capitalize)
    request = request_class.new(uri)
    headers.each { |key, value| request[key] = value }
    request.body = body if body
    Net::HTTP.start(uri.host, uri.port) { |http| http.request(request) }
  end

  def http_get_json(base_url, path)
    uri = URI.join(base_url, path)
    response = Net::HTTP.get_response(uri)
    raise "unexpected #{response.code} for #{path}: #{response.body}" unless response.is_a?(Net::HTTPSuccess)

    JSON.parse(response.body)
  end

  def open_http_socket(host:, port:, request_text:)
    socket = TCPSocket.new(host, port)
    socket.write(request_text)
    socket
  end

  def read_http_head(socket)
    lines = []
    while (line = socket.gets)
      lines << line
      break if line == "\r\n"
    end
    lines.join
  end

  def read_http_response(socket)
    head = read_http_head(socket)
    headers = parse_http_headers(head)
    body =
      if chunked_response?(headers)
        read_chunked_body(socket)
      elsif headers["content-length"]
        read_fixed_body(socket, headers["content-length"].to_i)
      else
        read_until_eof(socket)
      end

    [head, body]
  ensure
    socket.close unless socket.closed?
  end

  def parse_http_headers(head)
    head.lines.drop(1).each_with_object({}) do |line, memo|
      stripped = line.strip
      next if stripped.empty?

      key, value = stripped.split(":", 2)
      memo[key.downcase] = value.to_s.strip if key
    end
  end

  def chunked_response?(headers)
    headers.fetch("transfer-encoding", "").downcase.split(",").map(&:strip).include?("chunked")
  end

  def read_fixed_body(socket, content_length)
    return "" if content_length <= 0

    body = +""
    while body.bytesize < content_length
      body << socket.readpartial([content_length - body.bytesize, 1024].min)
    end
    body
  end

  def read_until_eof(socket)
    body = +""
    loop do
      body << socket.readpartial(1024)
    end
  rescue EOFError
    body
  end

  def read_chunked_body(socket)
    body = +""

    loop do
      size_line = socket.gets
      raise EOFError, "unexpected EOF while reading chunk size" unless size_line

      size = size_line.strip.split(";", 2).first.to_i(16)
      break if size.zero?

      chunk = read_fixed_body(socket, size)
      body << chunk
      socket.read(2)
    end

    while (line = socket.gets)
      break if line == "\r\n"
    end

    body
  end

  def open_connect_tunnel(host:, port:, target:, headers: {})
    socket = TCPSocket.new(host, port)
    request_headers = { "Host" => target }.merge(headers)
    socket.write("CONNECT #{target} HTTP/1.1\r\n")
    request_headers.each { |key, value| socket.write("#{key}: #{value}\r\n") }
    socket.write("\r\n")
    head = read_http_head(socket)
    raise "CONNECT failed: #{head}" unless head.start_with?("HTTP/1.1 200")

    socket
  end

  def open_socks5_tunnel(host:, port:, target_host:, target_port:, username: nil, password: nil)
    socket = TCPSocket.new(host, port)

    negotiate_socks5(socket, username:, password:)
    reply = send_socks5_connect_request(
      socket,
      target_host:,
      target_port:,
      address_type: 0x03
    )
    raise "SOCKS5 CONNECT rejected: #{reply&.bytes&.inspect}" unless reply && reply.getbyte(1) == 0x00

    socket
  end

  def negotiate_socks5(socket, username: nil, password: nil)
    if username && password
      socket.write([0x05, 0x01, 0x02].pack("C3"))
      method_reply = socket.read(2).bytes
      raise "SOCKS5 auth method failed: #{method_reply.inspect}" unless method_reply == [0x05, 0x02]

      socket.write([0x01, username.bytesize].pack("C2") + username + [password.bytesize].pack("C") + password)
      auth_reply = socket.read(2).bytes
      raise "SOCKS5 auth rejected: #{auth_reply.inspect}" unless auth_reply == [0x01, 0x00]
    else
      socket.write([0x05, 0x01, 0x00].pack("C3"))
      method_reply = socket.read(2).bytes
      raise "SOCKS5 no-auth failed: #{method_reply.inspect}" unless method_reply == [0x05, 0x00]
    end
  end

  def send_socks5_connect_request(socket, target_host:, target_port:, address_type: 0x03, command: 0x01)
    request =
      case address_type
      when 0x01
        [0x05, command, 0x00, 0x01].pack("C4") + IPAddr.new(target_host).hton + [target_port].pack("n")
      when 0x03
        host_bytes = target_host.b
        [0x05, command, 0x00, 0x03, host_bytes.bytesize].pack("C5") + host_bytes + [target_port].pack("n")
      else
        [0x05, command, 0x00, address_type].pack("C4")
      end

    socket.write(request)
    read_socks5_reply(socket)
  end

  def read_socks5_reply(socket)
    head = socket.read(4)
    return nil unless head && head.bytesize == 4

    atyp = head.getbyte(3)
    address_bytes =
      case atyp
      when 0x01 then socket.read(4)
      when 0x04 then socket.read(16)
      when 0x03
        length_bin = socket.read(1)
        return head unless length_bin && length_bin.bytesize == 1

        length = length_bin.getbyte(0)
        length_bin + socket.read(length).to_s
      else
        +""
      end

    port_bytes = socket.read(2).to_s
    head + address_bytes.to_s + port_bytes
  end

  def bcrypt_users_json(username:, password:)
    { username => BCrypt::Password.create(password) }.to_json
  end

  def proxy_authorization(username:, password:)
    "Basic #{Base64.strict_encode64("#{username}:#{password}")}"
  end

  def bootstrap_jetstream!(nats_url:, stream:)
    Sync do |task|
      client = NatsAsync::Client.new(url: nats_url, verbose: false)
      client.start(task: task)
      client.jetstream.add_stream(stream, subjects: ["proxy.>"])
      client.jetstream.stream_info(stream)
      client.close
    end
  end

  def wait_for_jetstream_stream!(nats_url:, stream:, js_api_prefix:, timeout: 10)
    wait_until(timeout:, interval: 0.1) do
      Sync do |task|
        client = NatsAsync::Client.new(url: nats_url, verbose: false, js_api_prefix: js_api_prefix)
        client.start(task: task)
        client.jetstream.stream_info(stream)
        client.close
        true
      end
    rescue StandardError
      nil
    end
  end

  def build_service_env(
    *,
    role:,
    nats_url:,
    mode:,
    service_id:,
    upstream_url: nil,
    request_subject_root: "to.proxy",
    response_subject_root: "from.proxy",
    listen_subject: "#{request_subject_root}.requests.>",
    stream: "proxy",
    consumer_name: "nats-proxy",
    queue_group: consumer_name,
    response_timeout: 2,
    stream_timeout: 2,
    receiver_max_inflight: 20,
    socks5: false,
    socks5_port: nil,
    proxy_auth_users_json: nil,
    js_api_prefix: nil
  )
    {
      "APP_ENV" => "system",
      "NO_RT_DEBUG" => "1",
      "QUIET" => "true",
      "CONSOLE_LEVEL" => "error",
      "RUBYOPT" => [ENV["RUBYOPT"], "-r./spec/support/simplecov_subprocess"].compact.join(" ").strip,
      "SIMPLECOV_CHILD" => "1",
      "SIMPLECOV_COMMAND_NAME" => "rspec-child-#{service_id}",
      "SERVICE_ROLE" => role,
      "SERVICE_ID" => service_id,
      "UPSTREAM_URL" => upstream_url,
      "NATS_URL" => nats_url,
      "NATS_MODE" => mode.to_s,
      "NATS_STREAM" => stream,
      "NATS_REQUEST_SUBJECT_ROOT" => request_subject_root,
      "NATS_RESPONSE_SUBJECT_ROOT" => response_subject_root,
      "LISTEN_SUBJECT" => listen_subject,
      "NATS_CONSUMER_NAME" => consumer_name,
      "NATS_QUEUE_GROUP" => queue_group,
      "NATS_JS_API_PREFIX" => js_api_prefix,
      "NATS_RESPONSE_TIMEOUT" => response_timeout.to_s,
      "STREAM_RESPONSE_TIMEOUT" => stream_timeout.to_s,
      "RECEIVER_MAX_INFLIGHT" => receiver_max_inflight.to_s,
      "SOCKS5_ENABLED" => socks5 ? "true" : "false",
      "SOCKS5_LISTEN_HOST" => "127.0.0.1",
      "SOCKS5_LISTEN_PORT" => socks5_port.to_s,
      "PROXY_AUTH_ENABLED" => proxy_auth_users_json ? "true" : "false",
      "PROXY_AUTH_USERS_JSON" => proxy_auth_users_json
    }.compact
  end

  def wait_for_runtime!(service)
    last_payload = nil

    wait_until(timeout: 15) do
      unless service.alive?
        raise <<~MSG
          #{service.name} exited before readiness check completed (#{service.exit_summary})
          --- #{service.name} log ---
          #{service.logs}
          --- end log ---
        MSG
      end

      payload = http_get_json(service.base_url, "/observability/nats")
      last_payload = payload
      if payload["state"] == "ready"
        bridge_ready =
          case payload["role"]
          when "requester"
            payload["bridge_outbound"] == true
          when "receiver"
            payload["bridge_inbound"] == true
          else
            true
          end

        return payload if bridge_ready
      end

      if payload["state"] == "failed"
        raise <<~MSG
          #{service.name} failed to boot
          payload=#{payload}
          --- #{service.name} log ---
          #{service.logs}
          --- end log ---
        MSG
      end

      nil
    rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH, EOFError, JSON::ParserError
      unless service.alive?
        raise <<~MSG
          #{service.name} exited before exposing observability endpoint (#{service.exit_summary})
          --- #{service.name} log ---
          #{service.logs}
          --- end log ---
        MSG
      end

      nil
    end
  rescue StandardError => e
    raise <<~MSG
      service failed to become ready on port #{service.port}: #{e.message}
      last_payload=#{last_payload.inspect}
      --- #{service.name} log ---
      #{service.logs}
      --- end log ---
    MSG
  end

  def with_service_process(mode:, role:, nats_url:, upstream_url: nil, socks5: false, proxy_auth_users_json: nil, response_timeout: 2, stream_timeout: 2, receiver_max_inflight: 20, js_api_prefix: nil, request_subject_root: "to.proxy", response_subject_root: "from.proxy", listen_subject: "#{request_subject_root}.requests.>")
    service = ExternalServiceProcess.new(
      name: role,
      port: free_port,
      env: build_service_env(
        role:,
        nats_url:,
        mode:,
        service_id: "#{role}-#{SecureRandom.hex(3)}",
        upstream_url:,
        request_subject_root:,
        response_subject_root:,
        listen_subject:,
        response_timeout:,
        stream_timeout:,
        receiver_max_inflight:,
        socks5:,
        socks5_port: socks5 ? free_port : nil,
        proxy_auth_users_json:,
        js_api_prefix:,
        consumer_name: "spec-#{mode}-#{role}-#{SecureRandom.hex(3)}",
        queue_group: "spec-#{mode}-#{role}-#{SecureRandom.hex(3)}",
        stream: "spec-#{mode}-#{SecureRandom.hex(3)}"
      ),
      workdir: src_path
    )

    service.start
    wait_for_runtime!(service)
    yield(service)
  ensure
    service&.stop
  end

  def with_service_cluster(mode:, upstream_url:, socks5: false, proxy_auth_users_json: nil, response_timeout: 2, stream_timeout: 2, receiver_max_inflight: 20)
    with_leaf_topology_nats do |nats_context|
      stream_name = "system-#{mode}-#{SecureRandom.hex(3)}"
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

      receiver = ExternalServiceProcess.new(
        name: "receiver",
        port: free_port,
        env: build_service_env(
          role: "receiver",
          nats_url: nats_context.fetch(:proxy_url),
          mode:,
          service_id: "receiver-#{SecureRandom.hex(3)}",
          upstream_url:,
          request_subject_root: "proxy",
          response_subject_root: "proxy",
          listen_subject: "proxy.requests.>",
          stream: stream_name,
          consumer_name: "spec-#{mode}-receiver-#{SecureRandom.hex(3)}",
          queue_group: "spec-#{mode}-receiver-#{SecureRandom.hex(3)}",
          response_timeout:,
          stream_timeout:,
          receiver_max_inflight:,
          proxy_auth_users_json:,
          js_api_prefix: mode == :jetstream ? "$JS.API" : nil
        ),
        workdir: src_path
      )

      requester = ExternalServiceProcess.new(
        name: "requester",
        port: free_port,
        env: build_service_env(
          role: "requester",
          nats_url: nats_context.fetch(:local_url),
          mode:,
          service_id: "requester-#{SecureRandom.hex(3)}",
          request_subject_root: "to.proxy",
          response_subject_root: "from.proxy",
          listen_subject: "to.proxy.requests.>",
          stream: stream_name,
          consumer_name: "spec-#{mode}-requester-#{SecureRandom.hex(3)}",
          queue_group: "spec-#{mode}-requester-#{SecureRandom.hex(3)}",
          response_timeout:,
          stream_timeout:,
          receiver_max_inflight:,
          socks5:,
          socks5_port: socks5 ? free_port : nil,
          proxy_auth_users_json:,
          js_api_prefix: mode == :jetstream ? "JS.PROXY.API" : nil
        ),
        workdir: src_path
      )

      receiver.start
      requester.start
      wait_for_runtime!(receiver)
      wait_for_runtime!(requester)

      yield(
        nats: nats_context,
        requester: requester,
        receiver: receiver,
        requester_url: requester.base_url,
        receiver_url: receiver.base_url,
        socks5_port: requester.env_fetch("SOCKS5_LISTEN_PORT")&.to_i
      )
    ensure
      requester&.stop
      receiver&.stop
    end
  end
end

class SystemHelpers::ExternalServiceProcess
  def env_fetch(key) = @env.fetch(key)
end

if defined?(RSpec)
  RSpec.configure do |config|
    config.include SystemHelpers, :system
    config.include SystemHelpers, file_path: %r{spec/contracts/}
  end
end
