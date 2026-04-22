require_relative "../spec_helper"
require_relative "../../protocol_rack_connect_patch"

RSpec.describe Async::HTTP::Protocol::HTTP1::Request do
  it "preserves raw absolute-form request-target metadata" do
    headers = Protocol::HTTP::Headers.new
    connection = double("connection")
    allow(connection).to receive(:read_request).and_yield(
      "proxy.internal:7000",
      "GET",
      "http://api.example.test:8080/v1/models?limit=5",
      "HTTP/1.1",
      headers,
      nil
    )

    request = described_class.read(connection)

    expect(request.scheme).to eq("http")
    expect(request.authority).to eq("api.example.test:8080")
    expect(request.path).to eq("/v1/models?limit=5")
    expect(request.request_target).to eq("http://api.example.test:8080/v1/models?limit=5")
    expect(request.absolute_form_target).to be(true)
  end
end

RSpec.describe Protocol::Rack::Adapter::Rack31 do
  let(:adapter_class) do
    Class.new(described_class) do
      def logger = nil
      def unwrap_request(_request, env) = env
    end
  end
  let(:adapter) { adapter_class.allocate }

  it "uses CONNECT authority when request.path is nil" do
    request = Struct.new(:path, :authority, :method, :version, :scheme, :protocol, :body, keyword_init: true) do
      def connect? = true
    end.new(
      path: nil,
      authority: "example.com:443",
      method: "CONNECT",
      version: "HTTP/1.1",
      scheme: "http",
      protocol: nil,
      body: nil
    )

    env = adapter.make_environment(request)

    expect(env["PATH_INFO"]).to eq("example.com:443")
    expect(env["REQUEST_URI"]).to eq("example.com:443")
  end
end
