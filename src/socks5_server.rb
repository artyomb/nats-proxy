require 'ipaddr'
require 'io/endpoint/host_endpoint'
require 'securerandom'
require 'socket'
require 'async'
require_relative 'bridge_protocol'

class Socks5Server
  REPLY_SUCCEEDED = 0x00
  REPLY_GENERAL_FAILURE = 0x01
  REPLY_COMMAND_NOT_SUPPORTED = 0x07
  REPLY_ADDRESS_TYPE_NOT_SUPPORTED = 0x08
  AUTH_VERSION = 0x01
  AUTH_STATUS_SUCCESS = 0x00
  AUTH_STATUS_FAILURE = 0x01

  def initialize(core:, tcp_tunnel_bridge:, host:, port:, nats_response_timeout:, proxy_auth:)
    @core = core
    @tcp_tunnel_bridge = tcp_tunnel_bridge
    @host = host
    @port = port
    @nats_response_timeout = nats_response_timeout
    @proxy_auth = proxy_auth
    @server = nil
    @listener_task = nil
  end

  def start(task:)
    return if @listener_task && !@listener_task.finished?

    endpoint = IO::Endpoint.tcp(@host, @port)
    @server = endpoint.bind.first
    @server.listen(Socket::SOMAXCONN)
    @listener_task = task.async(annotation: "socks5-listener-#{@host}:#{@port}") { accept_loop }
    LOGGER.info "SOCKS5 listener started: host=#{@host}, port=#{listener_port}"
  end

  def stop
    @listener_task&.stop
    @server&.close
    @listener_task = nil
    @server = nil
  rescue IOError, Errno::EBADF
    true
  end

  private

  def listener_port
    @server&.local_address&.ip_port || @port
  rescue StandardError
    @port
  end

  def accept_loop
    loop do
      socket, = @server.accept
      Async do
        handle_client(socket)
      rescue StandardError => e
        LOGGER.error "SOCKS5 session failed: #{e.class} - #{e.message}"
      ensure
        socket.close unless socket.closed?
      end
    end
  rescue Async::Stop, IOError, Errno::EBADF
    nil
  end

  def handle_client(socket)
    return deny_client(socket) unless negotiate_method(socket)
    return deny_client(socket) unless authenticate_client(socket)

    target = parse_connect_request(socket)
    return unless target

    host, port = target
    session_id = SecureRandom.hex(16)
    context = @core.bridge_session_open(
      session_id:,
      payload: {
        'host' => host,
        'port' => port,
        'ingress_kind' => 'socks5',
        'method' => 'SOCKS5_CONNECT'
      }
    )

    established = BridgeProtocol.wait_for_session_established(
      context.response_queue, timeout: @nats_response_timeout
    )
    unless established
      @core.release_pending(session_id)
      send_reply(socket, REPLY_GENERAL_FAILURE)
      return
    end

    send_reply(
      socket,
      REPLY_SUCCEEDED,
      bind_host: established['bind_host'],
      bind_port: established['bind_port']
    )
    @tcp_tunnel_bridge.run_requester_tunnel(io: socket, context:, session_id:)
  rescue BridgeProtocol::ProtocolError => e
    LOGGER.error "SOCKS5 protocol error: #{e.message}"
    send_reply(socket, REPLY_GENERAL_FAILURE) rescue nil
  end

  def negotiate_method(socket)
    header = read_exact(socket, 2)
    return false unless header

    version = header.getbyte(0)
    methods_size = header.getbyte(1)
    return false unless version == 5

    methods = read_exact(socket, methods_size)
    return false unless methods

    selected_method = @proxy_auth.socks5_auth_method
    if selected_method && methods.bytes.include?(selected_method)
      socket.write([0x05, selected_method].pack('C2'))
      true
    else
      socket.write([0x05, 0xFF].pack('C2')) rescue nil
      if @proxy_auth.enabled?
        if selected_method.nil?
          LOGGER.error("Proxy access denied by safety lock: protocol=socks5, reason=#{@proxy_auth.failure_reason}")
        else
          LOGGER.warn('Proxy authentication failed: protocol=socks5')
        end
      end
      false
    end
  end

  def authenticate_client(socket)
    result = @proxy_auth.authorize_socks5_credentials(*parse_username_password_auth(socket))

    case result
    when :authorized, :disabled
      socket.write([AUTH_VERSION, AUTH_STATUS_SUCCESS].pack('C2')) if @proxy_auth.enabled?
      true
    when :blocked
      socket.write([AUTH_VERSION, AUTH_STATUS_FAILURE].pack('C2')) rescue nil
      LOGGER.error("Proxy access denied by safety lock: protocol=socks5, reason=#{@proxy_auth.failure_reason}")
      false
    else
      socket.write([AUTH_VERSION, AUTH_STATUS_FAILURE].pack('C2')) rescue nil
      LOGGER.warn('Proxy authentication failed: protocol=socks5')
      false
    end
  rescue StandardError => e
    socket.write([AUTH_VERSION, AUTH_STATUS_FAILURE].pack('C2')) rescue nil
    LOGGER.error("Proxy access denied by safety lock: protocol=socks5, reason=#{@proxy_auth.failure_reason}")
    LOGGER.error "SOCKS5 auth negotiation failed: #{e.class} - #{e.message}"
    false
  end

  def parse_username_password_auth(socket)
    return [nil, nil] unless @proxy_auth.enabled?

    header = read_exact(socket, 2)
    raise BridgeProtocol::ProtocolError, 'Missing SOCKS5 auth header' unless header

    version = header.getbyte(0)
    username_length = header.getbyte(1)
    raise BridgeProtocol::ProtocolError, 'Unsupported SOCKS5 auth version' unless version == AUTH_VERSION
    raise BridgeProtocol::ProtocolError, 'Empty SOCKS5 username' if username_length.to_i <= 0

    username = read_exact(socket, username_length)&.force_encoding(Encoding::UTF_8)
    raise BridgeProtocol::ProtocolError, 'Missing SOCKS5 username' unless username

    password_length_bin = read_exact(socket, 1)
    raise BridgeProtocol::ProtocolError, 'Missing SOCKS5 password length' unless password_length_bin

    password_length = password_length_bin.getbyte(0)
    raise BridgeProtocol::ProtocolError, 'Empty SOCKS5 password' if password_length.to_i <= 0

    password = read_exact(socket, password_length)&.force_encoding(Encoding::UTF_8)
    raise BridgeProtocol::ProtocolError, 'Missing SOCKS5 password' unless password

    [username, password]
  end

  def parse_connect_request(socket)
    header = read_exact(socket, 4)
    return nil unless header

    version = header.getbyte(0)
    command = header.getbyte(1)
    address_type = header.getbyte(3)

    unless version == 5
      send_reply(socket, REPLY_GENERAL_FAILURE)
      return nil
    end

    if command != 0x01
      send_reply(socket, REPLY_COMMAND_NOT_SUPPORTED)
      return nil
    end

    host =
      case address_type
      when 0x01 then parse_ipv4(socket)
      when 0x03 then parse_domain(socket)
      when 0x04 then parse_ipv6(socket)
      else
        send_reply(socket, REPLY_ADDRESS_TYPE_NOT_SUPPORTED)
        return nil
      end

    return nil unless host

    port_bin = read_exact(socket, 2)
    return nil unless port_bin

    [host, port_bin.unpack1('n')]
  end

  def parse_ipv4(socket)
    raw = read_exact(socket, 4)
    return nil unless raw

    IPAddr.new_ntoh(raw).to_s
  end

  def parse_ipv6(socket)
    raw = read_exact(socket, 16)
    return nil unless raw

    IPAddr.new_ntoh(raw).to_s
  end

  def parse_domain(socket)
    length_bin = read_exact(socket, 1)
    return nil unless length_bin

    length = length_bin.getbyte(0)
    return nil if length <= 0

    read_exact(socket, length)&.force_encoding(Encoding::UTF_8)
  end

  def send_reply(socket, code, bind_host: nil, bind_port: nil)
    host = bind_host.to_s
    port = bind_port.to_i
    port = 0 if port.negative? || port > 65_535

    address, address_type =
      begin
        ip = IPAddr.new(host)
        if ip.ipv4?
          [ip.hton, 0x01]
        else
          [ip.hton, 0x04]
        end
      rescue StandardError
        ["\x00\x00\x00\x00".b, 0x01]
      end

    socket.write([0x05, code, 0x00, address_type].pack('C4') + address + [port].pack('n'))
  end

  def read_exact(socket, size)
    data = socket.read(size)
    return nil unless data
    return nil unless data.bytesize == size

    data
  end

  def deny_client(socket)
    socket.close unless socket.closed?
    false
  end
end
