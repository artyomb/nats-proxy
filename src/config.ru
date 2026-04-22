require 'async'
require 'async/queue'
require 'concurrent-ruby'
require 'dotenv'
require 'json'
require 'securerandom'
require 'sinatra'
require 'stack-service-base'
require_relative 'protocol_rack_connect_patch'
require_relative 'nats_async_runtime'
require_relative 'observability_collector'
require_relative 'bridge_core'
require_relative 'http_gateway'
require_relative 'tcp_tunnel_bridge'
require_relative 'proxy_auth'
require_relative 'service_runtime'

StackServiceBase.rack_setup self
Dotenv.load

UPSTREAM_URL            = ENV['UPSTREAM_URL']

NATS_URL                = ENV.fetch('NATS_URL', 'nats://localhost:4222')
REQUEST_SUBJECT_ROOT    = ENV.fetch('NATS_REQUEST_SUBJECT_ROOT', 'to.proxy')
RESPONSE_SUBJECT_ROOT   = ENV.fetch('NATS_RESPONSE_SUBJECT_ROOT', 'from.proxy')
NATS_CONSUMER_NAME      = ENV.fetch('NATS_CONSUMER_NAME', 'nats-proxy')
NATS_QUEUE_GROUP        = ENV.fetch('NATS_QUEUE_GROUP', NATS_CONSUMER_NAME)
NATS_STREAM             = ENV.fetch('NATS_STREAM', 'proxy')
LISTEN_SUBJECT          = ENV.fetch('LISTEN_SUBJECT', "#{REQUEST_SUBJECT_ROOT}.requests.>")
NATS_MODE               = ENV.fetch('NATS_MODE', 'auto')
NATS_JS_API_PREFIX      = ENV['NATS_JS_API_PREFIX']
SERVICE_ID              = ENV.fetch('SERVICE_ID', "srv-#{SecureRandom.hex(4)}")

NATS_RESPONSE_TIMEOUT   = ENV.fetch('NATS_RESPONSE_TIMEOUT', '30').to_i
STREAM_RESPONSE_TIMEOUT = ENV.fetch('STREAM_RESPONSE_TIMEOUT', '30').to_i
MAX_INFLIGHT            = ENV.fetch('RECEIVER_MAX_INFLIGHT', '20').to_i
QUEUE_SIZE              = MAX_INFLIGHT * 2

RESOLVED_ROLE           = ENV.fetch('SERVICE_ROLE', UPSTREAM_URL ? 'receiver' : 'requester')

SOCKS5_ENABLED          = ENV.fetch('SOCKS5_ENABLED', 'false') == 'true'
SOCKS5_LISTEN_HOST      = ENV.fetch('SOCKS5_LISTEN_HOST', '0.0.0.0')
SOCKS5_LISTEN_PORT      = ENV.fetch('SOCKS5_LISTEN_PORT', '1080').to_i

PROXY_AUTH_ENABLED      = ENV.fetch('PROXY_AUTH_ENABLED', 'true') != 'false'
PROXY_AUTH_USERS_JSON   = ENV['PROXY_AUTH_USERS_JSON']

unless %w[requester receiver].include?(RESOLVED_ROLE)
  raise "Invalid SERVICE_ROLE=#{RESOLVED_ROLE.inspect}. Allowed: requester, receiver"
end

PROXY_AUTH = ProxyAuth.new(
  enabled: PROXY_AUTH_ENABLED,
  users_json: PROXY_AUTH_USERS_JSON
)

SERVICE_RUNTIME = ServiceRuntime.new(
  config: {
    upstream_url: UPSTREAM_URL,
    nats_url: NATS_URL,
    request_subject_root: REQUEST_SUBJECT_ROOT,
    response_subject_root: RESPONSE_SUBJECT_ROOT,
    listen_subject: LISTEN_SUBJECT,
    nats_consumer_name: NATS_CONSUMER_NAME,
    nats_queue_group: NATS_QUEUE_GROUP,
    nats_stream: NATS_STREAM,
    nats_mode: NATS_MODE,
    nats_js_api_prefix: NATS_JS_API_PREFIX,
    service_id: SERVICE_ID,
    nats_response_timeout: NATS_RESPONSE_TIMEOUT,
    stream_response_timeout: STREAM_RESPONSE_TIMEOUT,
    max_inflight: MAX_INFLIGHT,
    queue_size: QUEUE_SIZE,
    role: RESOLVED_ROLE,
    socks5_enabled: SOCKS5_ENABLED,
    socks5_listen_host: SOCKS5_LISTEN_HOST,
    socks5_listen_port: SOCKS5_LISTEN_PORT
  },
  proxy_auth: PROXY_AUTH
)
OBSERVABILITY_COLLECTOR = SERVICE_RUNTIME.collector

module AsyncWarmup
  class << self
    def install!(&boot_callback)
      @boot_callback = boot_callback
      return if ENV['APP_ENV'] == 'test'
      return if @installed

      Async::Task.prepend Hook
      @installed = true
    end

    def boot_once(parent_task)
      return if @boot_started

      @boot_started = true
      parent_task.async(annotation: 'service-bootstrap') do |task|
        @boot_callback&.call(task)
      rescue => e
        $stderr.puts 'Bootstrapping failed:'
        $stderr.puts e.message, e.backtrace.join("\n")
      end
    end
  end

  module Hook
    def initialize(...)
      super
      return unless @parent.is_a?(Async::Reactor)

      AsyncWarmup.boot_once(self)
    end
  end
end

AsyncWarmup.install! { |task| SERVICE_RUNTIME.boot_once(task) }

module RuntimeResponses
  module_function

  def local_passthrough_path?(path)
    path = path.to_s
    path.start_with?('/observability') || %w[/health /healthcheck].include?(path)
  end

  def service_unavailable
    payload = SERVICE_RUNTIME.boot_status_payload
    body = {
      error: 'Service Unavailable',
      message: "Service runtime #{payload[:state]}",
      details: payload
    }.to_json

    [503, { 'content-type' => 'application/json', 'content-length' => body.bytesize.to_s }, [body]]
  end
end

class ConnectProxyMiddleware
  def initialize(app, runtime_resolver:, proxy_auth:)
    @app = app
    @runtime_resolver = runtime_resolver
    @proxy_auth = proxy_auth
  end

  def call(env)
    return @app.call(env) unless env['REQUEST_METHOD'] == 'CONNECT'

    case @proxy_auth.authorize_http_proxy_request(env)
    when :authorized, :disabled
      bridge = @runtime_resolver.call
      return RuntimeResponses.service_unavailable unless bridge

      bridge.dispatch_connect_request(env:)
    when :blocked
      LOGGER.error("Proxy access denied by safety lock: protocol=connect, reason=#{@proxy_auth.failure_reason}")
      @proxy_auth.proxy_denied_response
    else
      LOGGER.warn('Proxy authentication failed: protocol=connect')
      @proxy_auth.proxy_denied_response
    end
  end
end

helpers do
  def runtime = SERVICE_RUNTIME

  def runtime_gateway = SERVICE_RUNTIME.http_gateway

  def runtime_unavailable_response = RuntimeResponses.service_unavailable
end

use ProxyAuthMiddleware,
    proxy_auth: PROXY_AUTH,
    gateway_resolver: -> { SERVICE_RUNTIME.http_gateway },
    local_passthrough_path: ->(path) { RuntimeResponses.local_passthrough_path?(path) }
use ConnectProxyMiddleware, runtime_resolver: -> { SERVICE_RUNTIME.tcp_tunnel_bridge }, proxy_auth: PROXY_AUTH

get '/observability/flows' do
  content_type :json
  SERVICE_RUNTIME.collector.flow_events(params).to_json
end

get '/observability/cases' do
  content_type :json
  SERVICE_RUNTIME.collector.flow_cases(params).to_json
end

get '/observability/metrics' do
  content_type :json
  requested_window = (params['window_sec'] || 60).to_i
  window_sec = [[requested_window, 1].max, 300].min
  SERVICE_RUNTIME.collector.metrics(window_sec:).to_json
end

get '/observability/nats' do
  content_type :json
  SERVICE_RUNTIME.collector.nats_runtime_payload(
    nats_client: SERVICE_RUNTIME.nats_service,
    service_id: SERVICE_ID,
    role: RESOLVED_ROLE,
    backend_mode: SERVICE_RUNTIME.backend || NATS_MODE,
    stream: NATS_STREAM,
    consumer: SERVICE_RUNTIME.observability_consumer,
    js_api_prefix: NATS_JS_API_PREFIX
  ).merge(SERVICE_RUNTIME.boot_status_payload).to_json
end

get '/observability' do
  slim :index
end

get '/' do
  gateway = runtime_gateway
  gateway ? gateway.dispatch_http_request(app: self, method: 'GET') : runtime_unavailable_response
end

get '*' do
  gateway = runtime_gateway
  pass if RuntimeResponses.local_passthrough_path?(request.path_info) && !(gateway&.proxy_forward_request?(request))
  gateway ? gateway.dispatch_http_request(app: self, method: 'GET') : runtime_unavailable_response
end

head '*' do
  gateway = runtime_gateway
  pass if RuntimeResponses.local_passthrough_path?(request.path_info) && !(gateway&.proxy_forward_request?(request))
  gateway ? gateway.dispatch_http_request(app: self, method: 'HEAD') : runtime_unavailable_response
end

%w[post put patch delete options].each do |http_method|
  Sinatra::Application.send(http_method, '*') do
    gateway = runtime_gateway
    gateway ? gateway.dispatch_http_request(app: self, method: http_method.upcase) : runtime_unavailable_response
  end
end

at_exit do
  LOGGER.info "Shutting down bridge: service_id=#{SERVICE_ID}, backend=#{SERVICE_RUNTIME.backend || NATS_MODE}"
  SERVICE_RUNTIME.shutdown if defined?(SERVICE_RUNTIME)
end

run Sinatra::Application
