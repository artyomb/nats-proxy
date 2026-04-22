require 'base64'
require 'bcrypt'
require 'json'
require 'rack/request'

class ProxyAuth
  DENY_MESSAGE = 'Not Found'.freeze

  def initialize(enabled:, users_json:)
    @enabled = enabled
    @users = {}
    @available = true
    @failure_reason = nil
    load_users(users_json)
  end

  def enabled? = @enabled

  def available? = @available

  attr_reader :failure_reason

  def proxy_specific_http_request?(env, gateway:)
    return false if gateway.local_passthrough_path?(env['PATH_INFO'].to_s)
    return true if env['REQUEST_METHOD'] == 'CONNECT'

    gateway.proxy_forward_request?(Rack::Request.new(env))
  rescue => e
    LOGGER.error("Proxy request classification failed: #{e.class} - #{e.message}")
    true
  end

  def proxy_denied_response
    body = DENY_MESSAGE
    [
      404,
      {
        'content-type' => 'text/plain',
        'content-length' => body.bytesize.to_s
      },
      [body]
    ]
  end

  def authorize_http_proxy_request(env)
    return :disabled unless enabled?
    return :blocked unless available?

    credentials = extract_basic_credentials(env['HTTP_PROXY_AUTHORIZATION'])
    return :unauthorized unless credentials

    username, password = credentials
    valid_credentials?(username, password) ? :authorized : :unauthorized
  rescue => e
    deny_with_runtime_failure!("HTTP proxy auth verification failed: #{e.class} - #{e.message}")
    :blocked
  end

  def socks5_auth_method
    return 0x00 unless enabled?
    return nil unless available?

    0x02
  end

  def authorize_socks5_credentials(username, password)
    return :disabled unless enabled?
    return :blocked unless available?

    valid_credentials?(username, password) ? :authorized : :unauthorized
  rescue => e
    deny_with_runtime_failure!("SOCKS5 auth verification failed: #{e.class} - #{e.message}")
    :blocked
  end

  private

  def load_users(users_json)
    return unless enabled?

    parsed = JSON.parse(users_json.to_s)
    unless parsed.is_a?(Hash) && !parsed.empty?
      raise ArgumentError, 'PROXY_AUTH_USERS_JSON must be a non-empty JSON object'
    end

    @users = parsed.each_with_object({}) do |(username, hash), acc|
      user = username.to_s
      secret = hash.to_s
      raise ArgumentError, 'Proxy auth usernames must be non-empty' if user.empty?
      raise ArgumentError, "Missing bcrypt hash for proxy user #{user.inspect}" if secret.empty?

      BCrypt::Password.new(secret)
      acc[user] = secret
    end
  rescue JSON::ParserError => e
    set_startup_failure!("Invalid PROXY_AUTH_USERS_JSON: #{e.message}")
  rescue => e
    set_startup_failure!(e.message)
  end

  def extract_basic_credentials(header)
    value = header.to_s
    return nil if value.empty?

    scheme, encoded = value.split(/\s+/, 2)
    return nil unless scheme&.casecmp('Basic')&.zero?
    return nil if encoded.to_s.empty?

    decoded = Base64.strict_decode64(encoded)
    username, password = decoded.split(':', 2)
    return nil if username.to_s.empty? || password.nil?

    [username, password]
  rescue ArgumentError
    nil
  end

  def valid_credentials?(username, password)
    secret = @users[username.to_s]
    return false if secret.to_s.empty?

    BCrypt::Password.new(secret).is_password?(password.to_s)
  end

  def deny_with_runtime_failure!(message)
    @available = false
    @failure_reason = message
  end

  def set_startup_failure!(message)
    @available = false
    @failure_reason = message
    LOGGER.error("Proxy auth safety lock enabled: #{failure_reason}") if enabled?
  end
end

class ProxyAuthMiddleware
  def initialize(app, proxy_auth:, http_gateway: nil, gateway_resolver: nil, local_passthrough_path: nil)
    @app = app
    @proxy_auth = proxy_auth
    @http_gateway = http_gateway
    @gateway_resolver = gateway_resolver
    @local_passthrough_path = local_passthrough_path
  end

  def call(env)
    return @app.call(env) if env['REQUEST_METHOD'] == 'CONNECT'

    gateway = @gateway_resolver&.call || @http_gateway
    return @app.call(env) unless proxy_specific_http_request?(env, gateway)

    case @proxy_auth.authorize_http_proxy_request(env)
    when :authorized, :disabled
      @app.call(env)
    when :blocked
      LOGGER.error("Proxy access denied by safety lock: protocol=http_proxy, reason=#{@proxy_auth.failure_reason}")
      @proxy_auth.proxy_denied_response
    else
      LOGGER.warn('Proxy authentication failed: protocol=http_proxy')
      @proxy_auth.proxy_denied_response
    end
  end

  private

  def proxy_specific_http_request?(env, gateway)
    return fallback_proxy_request?(env) unless gateway

    @proxy_auth.proxy_specific_http_request?(env, gateway:)
  end

  def fallback_proxy_request?(env)
    path = env['PATH_INFO'].to_s
    return false if @local_passthrough_path&.call(path)

    true
  end
end
