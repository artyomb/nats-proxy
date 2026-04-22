require 'stack-service-base/logging'
require 'rspec-benchmark'
require 'rack/test'
require 'async/rspec'
require 'rack/builder'
require "rspec/snapshot"

module Rack::Test::AppHelper
  def app = RSpec.configuration.app
end

RSpec.configure do |config|
  config.include Rack::Test::AppHelper, type: :request
  config.include Rack::Test::Methods, type: :request
  config.include RSpec::Benchmark::Matchers
  config.include RSpec::Snapshot
  config.include_context Async::RSpec::Reactor
  config.add_setting :app

  config.before(type: :request) do
    header 'Host', 'localhost'
  end

  config.before(:suite) do
    if RSpec.world.filtered_examples.values.flatten.any? { |e| e.metadata[:type] == :request }
      rack_app, = Rack::Builder.parse_file(File.expand_path("../../config.ru", __dir__))
      RSpec.configuration.app = rack_app
    end
  end
end
