require "open3"
require_relative "../spec_helper"

RSpec.describe "config.ru contract" do
  def run_config_ru(script, extra_env: {})
    env = {
      "APP_ENV" => "test",
      "PROXY_AUTH_ENABLED" => "false",
      "NO_RT_DEBUG" => "1",
      "QUIET" => "true",
      "CONSOLE_LEVEL" => "error"
    }.merge(extra_env)
    Open3.capture3(env, "bundle", "exec", "ruby", "-e", script, chdir: File.expand_path("../..", __dir__))
  end

  it "loads and exposes the service_unavailable runtime payload" do
    stdout, stderr, status = run_config_ru(<<~RUBY)
      require "logger"
      require "rack"
      LOGGER = Logger.new(File::NULL)
      Rack::Builder.parse_file("config.ru")
      puts "__RESULT__"
      puts RuntimeResponses.service_unavailable.first
      puts SERVICE_RUNTIME.boot_status_payload[:state]
    RUBY

    expect(status.exitstatus).to eq(0), stderr
    result = stdout.split("__RESULT__\n", 2).last.to_s.lines.map(&:strip)
    expect(result).to include("503", "idle")
  end

  it "wires proxy middlewares and observability/root routes" do
    stdout, stderr, status = run_config_ru(<<~RUBY)
      require "logger"
      require "rack"
      LOGGER = Logger.new(File::NULL)
      source = File.read("config.ru")
      Rack::Builder.parse_file("config.ru")
      routes = Sinatra::Application.routes.fetch("GET").map { |route| route[0].to_s }
      puts "__SOURCE__"
      puts source
      puts "__ROUTES__"
      puts routes.join("\\n")
    RUBY

    expect(status.exitstatus).to eq(0), stderr
    middlewares = stdout.split("__SOURCE__\n", 2).last.to_s.split("__ROUTES__\n", 2).first.to_s
    routes = stdout.split("__ROUTES__\n", 2).last.to_s
    expect(middlewares).to include("use ProxyAuthMiddleware")
    expect(middlewares).to include("use ConnectProxyMiddleware")
    expect(routes).to include("/observability")
    expect(routes).to include("/")
  end

  it "derives flow-control windows from runtime max payload instead of environment toggles" do
    flow_env = {
      "FLOW_CONTROL_ENABLED" => "false",
      "FLOW_INITIAL_WINDOW_BYTES" => "1",
      "FLOW_CREDIT_BATCH_BYTES" => "1",
      "FLOW_MAX_WINDOW_BYTES" => "1",
      "FLOW_CREDIT_WAIT_TIMEOUT" => "0.001"
    }
    stdout, stderr, status = run_config_ru(<<~RUBY, extra_env: flow_env)
      require "logger"
      require "rack"
      LOGGER = Logger.new(File::NULL)
      Rack::Builder.parse_file("config.ru")
      FakeNats = Struct.new(:max_payload) do
        def close; end
      end
      SERVICE_RUNTIME.instance_variable_set(:@nats_service, FakeNats.new(1_048_576))
      puts "__FLOW__"
      puts SERVICE_RUNTIME.send(:flow_window_config).to_json
    RUBY

    expect(status.exitstatus).to eq(0), stderr
    flow_config = JSON.parse(stdout.split("__FLOW__\n", 2).last.to_s)
    expect(flow_config).to include(
      "flow_chunk_size" => 32_768,
      "flow_initial_window_bytes" => 1_048_576,
      "flow_credit_batch_bytes" => 262_144,
      "flow_max_window_bytes" => 4_194_304,
      "flow_credit_wait_timeout" => 30.0
    )
  end
end
