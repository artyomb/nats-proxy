require_relative "../spec_helper"
require_relative "../../nats_async_runtime"
require_relative "../../bridge_protocol"

RSpec.describe NatsAsyncRuntime do
  class FakeJetStream
    attr_reader :published

    def initialize
      @published = []
    end

    def publish(subject, payload, headers: nil)
      @published << { subject: subject, payload: payload, headers: headers }
    end

    def stream_info(stream)
      { config: { name: stream }, state: { messages: 3 } }
    end

    def consumer_info(_stream, consumer)
      { name: consumer, num_pending: 2 }
    end
  end

  class FakeAsyncClient
    attr_reader :jetstream

    def initialize
      @jetstream = FakeJetStream.new
      @closed = false
    end

    def start(task:) = task
    def close = (@closed = true)
    def resolve_backend(mode:, stream:) = (mode == :auto ? :core : mode)
    def publish(subject, payload, reply: nil, headers: nil) = { subject: subject, payload: payload, reply: reply, headers: headers }
    def subscribe(*) = 1
    def unsubscribe(*) = true
    def status = :connected
    def connected? = !@closed
    def closed? = @closed
    def last_error = nil
    def server_info = { max_payload: 4096 }
    def sent_pings = 1
    def received_pings = 2
    def received_pongs = 3
  end

  it "rejects unknown backend mode" do
    expect { described_class.new("nats://127.0.0.1:4222", backend_mode: :bad) }
      .to raise_error(ArgumentError, /Invalid NATS_MODE/)
  end

  it "starts, publishes and closes through the configured client" do
    runtime = described_class.new("nats://127.0.0.1:4222", backend_mode: :auto)
    fake_client = FakeAsyncClient.new
    allow(runtime).to receive(:build_client).and_return(fake_client)

    Sync do |task|
      runtime.start(task: task, stream: "proxy")
      expect(runtime.backend).to eq(:core)
      expect(runtime.ready?).to be(true)
      runtime.publish({ ping: true }, "subject.demo", "reply.demo", raw: true, headers: { "traceparent" => "tp" })
      runtime.publish({ ping: true }, "subject.js", raw: false)
    end

    expect(fake_client.jetstream.published.first).to include(subject: "subject.js", payload: "{\"ping\":true}")
    expect(runtime.close).to be(true)
    expect(runtime.ready?).to be(false)
  end

  it "publishes and receives a request envelope in core mode", :nats_server do
    Async do |task|
      publisher = described_class.new(nats_url, backend_mode: :core)
      subscriber = described_class.new(nats_url, backend_mode: :core)
      received = []
      condition = Async::Condition.new

      publisher.start(task: task, stream: "proxy")
      subscriber.start(task: task, stream: "proxy")
      sid = subscriber.subscribe("to.proxy.requests.>") do |message|
        received << JSON.parse(message.data)
        condition.signal
      end
      subscriber.client.flush

      publisher.publish(
        BridgeProtocol.request_envelope(
          request_id: "req-core-1",
          reply_to: "from.proxy.responses.srv-requester.req-core-1",
          operation: "http_request",
          payload: { "method" => "GET", "path" => "/api/echo", "headers" => {}, "body" => nil }
        ),
        "to.proxy.requests.srv-requester.req-core-1",
        raw: true
      )

      task.with_timeout(2) { condition.wait until received.any? }

      expect(received.first).to include(
        "type" => "request",
        "request_id" => "req-core-1",
        "operation" => "http_request"
      )
    ensure
      subscriber&.unsubscribe(sid) if sid
      publisher&.close
      subscriber&.close
    end
  end

  it "publishes and fetches a request envelope in jetstream mode", :nats_server do
    Async do |task|
      bootstrap = NatsAsync::Client.new(url: nats_url, verbose: false)
      bootstrap.start(task: task)
      bootstrap.jetstream.add_stream("proxy", subjects: ["to.proxy.>", "from.proxy.>"])

      publisher = described_class.new(nats_url, backend_mode: :jetstream)
      subscriber = described_class.new(nats_url, backend_mode: :jetstream)
      publisher.start(task: task, stream: "proxy")
      subscriber.start(task: task, stream: "proxy")

      consumer = subscriber.jetstream.pull_subscribe(
        "to.proxy.requests.>",
        stream: "proxy",
        consumer: "spec-http-runtime",
        config: {
          ack_policy: "explicit",
          filter_subject: "to.proxy.requests.>"
        },
        create: true
      )

      publisher.publish(
        BridgeProtocol.request_envelope(
          request_id: "req-js-1",
          reply_to: "from.proxy.responses.srv-requester.req-js-1",
          operation: "http_request",
          payload: { "method" => "GET", "path" => "/stream", "headers" => {}, "body" => nil }
        ),
        "to.proxy.requests.srv-requester.req-js-1",
        raw: false
      )

      messages = consumer.fetch(batch: 1, timeout: 2)
      payload = JSON.parse(messages.first.data)
      messages.first.ack

      expect(payload).to include(
        "type" => "request",
        "request_id" => "req-js-1",
        "operation" => "http_request"
      )
    ensure
      bootstrap&.close
      publisher&.close
      subscriber&.close
    end
  end
end
