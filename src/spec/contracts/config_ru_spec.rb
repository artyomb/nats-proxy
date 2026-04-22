require "open3"
require_relative "../spec_helper"

RSpec.describe "config.ru contract" do
  def run_config_ru(script)
    env = {
      "APP_ENV" => "test",
      "PROXY_AUTH_ENABLED" => "false",
      "NO_RT_DEBUG" => "1",
      "QUIET" => "true",
      "CONSOLE_LEVEL" => "error"
    }
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
end
