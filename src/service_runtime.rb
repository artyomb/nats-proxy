class ServiceRuntime
  BOOT_STATES = %i[idle booting ready failed stopped].freeze

  attr_reader :boot_state, :boot_error, :collector, :proxy_auth, :role, :backend, :nats_service
  attr_reader :core, :http_gateway, :tcp_tunnel_bridge, :socks5_server

  def initialize(config:, proxy_auth:)
    @config = config
    @proxy_auth = proxy_auth
    @role = config.fetch(:role)
    @collector = ObservabilityCollector.new(
      service_id: config.fetch(:service_id),
      role: @role,
      backend: config.fetch(:nats_mode)
    )

    @boot_state = :idle
    @boot_error = nil
    @boot_started = false
    @boot_task = nil
    @backend = nil
    @nats_service = NatsAsyncRuntime.new(
      config.fetch(:nats_url),
      backend_mode: config.fetch(:nats_mode),
      js_api_prefix: config.fetch(:nats_js_api_prefix)
    )
  end

  def boot_once(task)
    return self if @boot_started

    @boot_started = true
    @boot_state = :booting
    @boot_error = nil
    @boot_task = task

    boot!(task)
    self
  rescue => e
    @boot_state = :failed
    @boot_error = e
    LOGGER.error "Service bootstrap failed: #{e.class} - #{e.message}"
    LOGGER.error e.backtrace.first(20).join("\n") if e.backtrace
    raise
  end

  def boot_status_payload
    {
      state: @boot_state,
      role: @role,
      backend: @backend || @config[:nats_mode],
      error: @boot_error&.message,
      bridge_inbound: @core&.bridge_inbound? || false,
      bridge_outbound: @core&.bridge_outbound? || false
    }
  end

  def observability_consumer
    return @config[:nats_consumer_name] unless @backend == :jetstream && @role == 'requester'

    "#{@config[:nats_consumer_name]}-responses-#{@config[:service_id]}"
  end

  def shutdown
    @socks5_server&.stop
    @core&.close
    @nats_service&.close
    @boot_task&.stop if @boot_task && !@boot_task.finished?
    @boot_state = :stopped unless @boot_state == :failed
    true
  rescue Async::Stop
    true
  end

  private

  def boot!(task)
    @nats_service.start(task:, stream: @config.fetch(:nats_stream))
    @backend = @nats_service.backend

    @core = build_core
    @http_gateway = build_http_gateway
    @tcp_tunnel_bridge = build_tcp_tunnel_bridge
    @socks5_server = build_socks5_server

    @core.register_handler('http_request', &@http_gateway.method(:handle_bridge_request))
    @core.register_handler('tcp_stream', &@tcp_tunnel_bridge.method(:handle_stream_request))

    case @role.to_sym
    when :receiver
      @core.start_request_listener(task:)
      @core.start_upstream_session_listener(task:)
      @core.start_cancel_listener(task:)
    when :requester
      @core.start_response_listener(task:)
      @core.start_downstream_session_listener(task:)
      @socks5_server&.start(task:)
    else
      raise "Invalid SERVICE_ROLE=#{@role.inspect}. Allowed: requester, receiver"
    end

    @boot_state = :ready
  end

  def build_core
    BridgeCore.new(
      nats_client: @nats_service,
      service_id: @config.fetch(:service_id),
      collector: @collector,
      nats_backend: @backend,
      config: {
        request_subject_root: @config.fetch(:request_subject_root),
        response_subject_root: @config.fetch(:response_subject_root),
        listen_subject: @config.fetch(:listen_subject),
        nats_stream: @config.fetch(:nats_stream),
        consumer_name: @config.fetch(:nats_consumer_name),
        queue_group: @config.fetch(:nats_queue_group),
        response_timeout: @config.fetch(:nats_response_timeout),
        stream_timeout: @config.fetch(:stream_response_timeout),
        max_inflight: @config.fetch(:max_inflight),
        queue_size: @config.fetch(:queue_size)
      }
    )
  end

  def build_http_gateway
    HttpGateway.new(
      core: @core,
      upstream_url: @config[:upstream_url],
      nats_backend: @backend,
      service_id: @config.fetch(:service_id),
      nats_response_timeout: @config.fetch(:nats_response_timeout),
      stream_response_timeout: @config.fetch(:stream_response_timeout)
    )
  end

  def build_tcp_tunnel_bridge
    TcpTunnelBridge.new(
      core: @core,
      nats_client: @nats_service,
      nats_backend: @backend,
      service_id: @config.fetch(:service_id),
      nats_response_timeout: @config.fetch(:nats_response_timeout),
      stream_response_timeout: @config.fetch(:stream_response_timeout)
    )
  end

  def build_socks5_server
    return nil unless @config[:socks5_enabled]

    require_relative 'socks5_server'
    Socks5Server.new(
      core: @core,
      tcp_tunnel_bridge: @tcp_tunnel_bridge,
      host: @config.fetch(:socks5_listen_host),
      port: @config.fetch(:socks5_listen_port),
      nats_response_timeout: @config.fetch(:nats_response_timeout),
      proxy_auth: @proxy_auth
    )
  end
end
