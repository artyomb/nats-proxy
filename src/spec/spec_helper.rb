require "simplecov"
require "fileutils"

FULL_SUITE_PATTERNS = %w[spec spec/ ./spec ./spec/].freeze

def full_suite_run?
  return ENV["COVERAGE_GATE"] == "1" if ENV.key?("COVERAGE_GATE")

  args = ARGV.reject { |arg| arg.start_with?("-") }
  args.empty? || args.all? { |arg| FULL_SUITE_PATTERNS.include?(arg) }
end

def coverage_command_name
  return ENV["SIMPLECOV_COMMAND_NAME"] unless ENV["SIMPLECOV_COMMAND_NAME"].to_s.empty?

  if full_suite_run?
    "rspec-full"
  else
    "rspec-#{File.basename($PROGRAM_NAME, ".rb")}-#{$$}"
  end
end

if full_suite_run?
  FileUtils.rm_f("coverage/.resultset.json")
  FileUtils.rm_f("coverage/.resultset.json.lock")
  FileUtils.rm_f("coverage/.last_run.json")
end

SimpleCov.command_name(coverage_command_name)
SimpleCov.start do
  add_filter "/spec/"
  enable_coverage :branch
  merge_timeout 3600
  use_merging true

  add_group "Bridge", %r{/(bridge_core|bridge_protocol|request_context)\.rb\z}
  add_group "Adapters", %r{/(http_gateway|tcp_tunnel_bridge|socks5_server|proxy_auth)\.rb\z}
  add_group "Runtime", %r{/(config\.ru|(service_runtime|nats_async_runtime|protocol_rack_connect_patch)\.rb)\z}
end

require "stringio"
require "tmpdir"
require "socket"
require "logger"
require "rspec"
require "rack/test"
require "async/rspec"

LOGGER = Logger.new(File::NULL) unless defined?(LOGGER)

Dir[File.expand_path("support/**/*.rb", __dir__)].sort.each { |file| require file }

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.syntax = :expect
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
    mocks.verify_doubled_constant_names = true
  end

  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand config.seed
  config.define_derived_metadata(file_path: %r{/spec/contracts/}) { |metadata| metadata[:layer] = :contracts }
  config.define_derived_metadata(file_path: %r{/spec/system/}) do |metadata|
    metadata[:layer] = :system
    metadata[:system] = true
  end
  config.define_derived_metadata(file_path: %r{/spec/unit/}) { |metadata| metadata[:layer] = :unit }
  config.include Rack::Test::Methods, type: :request
  config.include Async::RSpec::Reactor

  config.before do
    stub_const("LOGGER", instance_double("Logger", info: nil, warn: nil, error: nil))
  end
end
