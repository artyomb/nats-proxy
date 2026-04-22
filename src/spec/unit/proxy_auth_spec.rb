require "base64"
require_relative "../spec_helper"
require_relative "../../proxy_auth"

RSpec.describe ProxyAuth do
  let(:password_hash) { BCrypt::Password.create("secret") }

  it "authorizes valid HTTP proxy credentials" do
    auth = described_class.new(enabled: true, users_json: { "alice" => password_hash }.to_json)
    env = { "HTTP_PROXY_AUTHORIZATION" => "Basic #{Base64.strict_encode64('alice:secret')}" }

    expect(auth.authorize_http_proxy_request(env)).to eq(:authorized)
  end

  it "enters safety lock on invalid startup config" do
    auth = described_class.new(enabled: true, users_json: "{")

    expect(auth).not_to be_available
    expect(auth.authorize_http_proxy_request("HTTP_PROXY_AUTHORIZATION" => "Basic xxx")).to eq(:blocked)
  end

  it "treats CONNECT as proxy-specific traffic" do
    auth = described_class.new(enabled: false, users_json: nil)
    gateway = instance_double("HttpGateway", local_passthrough_path?: false, proxy_forward_request?: false)

    expect(auth.proxy_specific_http_request?({ "REQUEST_METHOD" => "CONNECT", "PATH_INFO" => "/" }, gateway: gateway)).to be(true)
  end
end

RSpec.describe ProxyAuthMiddleware do
  let(:password_hash) { BCrypt::Password.create("secret") }
  let(:proxy_auth) { ProxyAuth.new(enabled: true, users_json: { "alice" => password_hash }.to_json) }
  let(:http_gateway) { instance_double("HttpGateway", local_passthrough_path?: false, proxy_forward_request?: true) }
  let(:app) { ->(_env) { [200, { "content-type" => "text/plain" }, ["ok"]] } }

  it "returns generic 404 for unauthorized proxy requests" do
    middleware = described_class.new(app, proxy_auth: proxy_auth, http_gateway: http_gateway)

    status, headers, body = middleware.call("REQUEST_METHOD" => "GET", "PATH_INFO" => "/v1/models")

    expect(status).to eq(404)
    expect(headers).not_to have_key("proxy-authenticate")
    expect(body).to eq(["Not Found"])
  end

  it "passes local observability routes through without proxy auth" do
    gateway = instance_double("HttpGateway", local_passthrough_path?: true, proxy_forward_request?: false)
    middleware = described_class.new(app, proxy_auth: proxy_auth, http_gateway: gateway)

    status, headers, body = middleware.call("REQUEST_METHOD" => "GET", "PATH_INFO" => "/observability/nats")

    expect(status).to eq(200)
    expect(headers).to eq("content-type" => "text/plain")
    expect(body).to eq(["ok"])
  end
end
