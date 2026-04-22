return unless ENV["SIMPLECOV_CHILD"] == "1"

require "simplecov"

SimpleCov.command_name(ENV.fetch("SIMPLECOV_COMMAND_NAME", "rspec-child-#{$$}"))
SimpleCov.start do
  add_filter "/spec/"
  enable_coverage :branch
  merge_timeout 3600
  use_merging true

  add_group "Bridge", %r{/(bridge_core|bridge_protocol|request_context)\.rb\z}
  add_group "Adapters", %r{/(http_gateway|tcp_tunnel_bridge|socks5_server|proxy_auth)\.rb\z}
  add_group "Runtime", %r{/(config\.ru|(service_runtime|nats_async_runtime|protocol_rack_connect_patch)\.rb)\z}
end
