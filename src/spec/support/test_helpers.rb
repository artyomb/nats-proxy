require "uri"

class FakeRackRequest
  attr_reader :env, :body, :path_info, :query_string

  def initialize(method:, path:, headers: {}, body: nil)
    uri = URI.parse(path)
    request_path = path
    @path_info = uri.path.to_s.empty? ? path : uri.path
    @query_string = uri.query.to_s
    @body = StringIO.new(body.to_s)
    @env = {
      "REQUEST_METHOD" => method,
      "REQUEST_PATH" => request_path,
      "REQUEST_URI" => request_path,
      "PATH_INFO" => @path_info,
      "QUERY_STRING" => @query_string,
      "rack.url_scheme" => "http",
      "HTTP_HOST" => "example.test"
    }

    headers.each do |key, value|
      normalized = "HTTP_#{key.to_s.upcase.tr('-', '_')}"
      @env[normalized] = value
    end

    @env["CONTENT_TYPE"] = headers["Content-Type"] if headers["Content-Type"]
    @env["CONTENT_LENGTH"] = body.to_s.bytesize.to_s if body
  end
end

class FakeStreamOut
  attr_reader :chunks

  def initialize(disconnect_after_writes: nil)
    @chunks = []
    @disconnect_after_writes = disconnect_after_writes
    @callback = nil
    @errback = nil
    @closed = false
  end

  def callback(&block)
    @callback = block
  end

  def errback(&block)
    @errback = block
  end

  def <<(chunk)
    @chunks << chunk
    if @disconnect_after_writes && @chunks.size >= @disconnect_after_writes && @callback
      callback = @callback
      @callback = nil
      callback.call
    end

    chunk
  end

  def close
    @closed = true
  end

  def closed?
    @closed
  end
end

class FakeRackApp
  attr_reader :request, :response, :stream_output

  def initialize(request:, disconnect_after_writes: nil)
    @request = request
    @response = Struct.new(:headers).new({})
    @disconnect_after_writes = disconnect_after_writes
    @status = nil
  end

  def status(value = nil)
    @status = value unless value.nil?
    @status
  end

  def stream(_mode)
    out = FakeStreamOut.new(disconnect_after_writes: @disconnect_after_writes)
    yield out
    @stream_output = out.chunks.join
    out
  end
end

class TestHttpServer
  Response = Struct.new(:status, :headers, :body_parts, keyword_init: true)

  attr_reader :port

  def initialize
    @handlers = {}
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.addr[1]
    @stop = false
    @thread = Thread.new { accept_loop }
  end

  def base_url
    "http://127.0.0.1:#{port}"
  end

  def on(path, &block)
    @handlers[path] = block
  end

  def stop
    @stop = true
    @server.close
    @thread.join(1)
  rescue IOError
    nil
  end

  private

  def accept_loop
    until @stop
      socket = @server.accept
      Thread.new(socket) { |client| handle_client(client) }
    end
  rescue IOError
    nil
  end

  def handle_client(socket)
    request_line = socket.gets
    return unless request_line

    method, path, = request_line.split(" ", 3)
    headers = {}
    while (line = socket.gets)
      line = line.chomp
      break if line.empty?

      key, value = line.split(":", 2)
      headers[key.downcase] = value.to_s.strip
    end

    body = read_body(socket, headers)
    route = URI.parse(path)
    handler = @handlers.fetch(route.path) do
      proc { Response.new(status: 404, headers: { "Content-Type" => "text/plain" }, body_parts: ["not found"]) }
    end
    response = handler.call(method:, path:, headers:, body:, socket:)
    return if response == :handled

    write_response(socket, response)
  ensure
    socket.close unless socket.closed?
  end

  def read_body(socket, headers)
    size = headers["content-length"].to_i
    return nil if size <= 0

    socket.read(size)
  end

  def write_response(socket, response)
    status = response.status || 200
    headers = response.headers || {}
    body_parts = Array(response.body_parts)
    content_length = body_parts.sum { |part| part.to_s.b.bytesize }
    socket.write("HTTP/1.1 #{status} OK\r\n")
    headers.each { |key, value| socket.write("#{key}: #{value}\r\n") }
    socket.write("Content-Length: #{content_length}\r\n") unless headers.keys.any? { |key| key.casecmp("Content-Length").zero? }
    socket.write("\r\n")
    body_parts.each { |part| socket.write(part.to_s) }
  end
end

class EchoTcpServer
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
          while (data = client.readpartial(4096))
            client.write(data)
          end
        rescue EOFError, IOError, Errno::ECONNRESET, Errno::EPIPE
          nil
        ensure
          client.close unless client.closed?
        end
      end
    end
  rescue IOError
    nil
  end
end
