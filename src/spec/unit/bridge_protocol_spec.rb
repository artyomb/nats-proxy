require_relative "../spec_helper"
require_relative "../../bridge_protocol"

RSpec.describe BridgeProtocol do
  describe ".chunk_event and .chunk_body" do
    it "preserves utf-8 chunk bodies" do
      event = described_class.chunk_event("hello")

      expect(event).to eq("type" => "response_chunk", "body" => "hello")
      expect(described_class.chunk_body(event)).to eq("hello".b)
    end

    it "encodes binary chunks with base64" do
      bytes = "\xFF\x00A".b
      event = described_class.chunk_event(bytes)

      expect(event["body_encoding"]).to eq("base64")
      expect(described_class.chunk_body(event)).to eq(bytes)
    end
  end

  describe ".wait_for_start_event" do
    it "returns the first start event" do
      queue = Async::Queue.new
      queue.enqueue({ "type" => "response_start", "status" => 200 }.to_json)

      expect(described_class.wait_for_start_event(queue, timeout: 0.1)).to include("status" => 200)
    end

    it "rejects invalid event ordering" do
      queue = Async::Queue.new
      queue.enqueue({ "type" => "response_chunk", "body" => "late" }.to_json)

      expect { described_class.wait_for_start_event(queue, timeout: 0.1) }
        .to raise_error(BridgeProtocol::ProtocolError, "Expected response_start, got response_chunk")
    end
  end

  describe ".collect_non_streaming_body" do
    it "collects chunks until response_end" do
      queue = Async::Queue.new
      queue.enqueue({ "type" => "response_chunk", "body" => "he" }.to_json)
      queue.enqueue({ "type" => "response_chunk", "body" => "llo" }.to_json)
      queue.enqueue({ "type" => "response_end" }.to_json)

      expect(described_class.collect_non_streaming_body(queue, timeout: 0.1)).to eq("hello")
    end
  end

  describe ".each_stream_chunk" do
    it "treats response_end as finished and yields chunks" do
      queue = Async::Queue.new
      queue.enqueue({ "type" => "response_chunk", "body" => "a" }.to_json)
      queue.enqueue({ "type" => "response_end" }.to_json)

      chunks = []
      outcome = described_class.each_stream_chunk(queue, timeout: 0.1) { |chunk| chunks << chunk }

      expect(outcome).to eq(:finished)
      expect(chunks).to eq(["a".b])
    end

    it "returns in-band response_error payload" do
      queue = Async::Queue.new
      queue.enqueue({ "type" => "response_error", "error" => "boom" }.to_json)

      expect(described_class.each_stream_chunk(queue, timeout: 0.1) { |_chunk| nil })
        .to eq("type" => "response_error", "error" => "boom")
    end
  end

  describe ".flow_credit_payload and .parse_flow_credit_payload" do
    it "builds and parses flow credit frames" do
      payload = described_class.flow_credit_payload(
        request_id: "req-1",
        service_id: "srv-1",
        direction: described_class::DIRECTION_RESPONSE,
        bytes: "128",
        timestamp: "2026-05-15T00:00:00Z"
      )

      expect(payload).to eq(
        "type" => "flow_credit",
        "request_id" => "req-1",
        "direction" => "response",
        "bytes" => 128,
        "service_id" => "srv-1",
        "timestamp" => "2026-05-15T00:00:00Z"
      )
      expect(described_class.parse_flow_credit_payload(payload.to_json)).to include(
        "request_id" => "req-1",
        "direction" => "response",
        "bytes" => 128
      )
    end

    it "rejects invalid flow credit payloads" do
      expect(described_class.parse_flow_credit_payload({ "type" => "flow_credit", "request_id" => "req-1", "direction" => "bad", "bytes" => 1 }.to_json)).to be_nil
      expect(described_class.parse_flow_credit_payload({ "type" => "flow_credit", "request_id" => "req-1", "direction" => "response", "bytes" => 0 }.to_json)).to be_nil
      expect(described_class.parse_flow_credit_payload("not-json")).to be_nil
    end
  end

  describe ".normalize_headers" do
    it "downcases keys and preserves array values" do
      expect(described_class.normalize_headers("Set-Cookie" => ["a=1", "b=2"], "X-Id" => 10))
        .to eq("set-cookie" => ["a=1", "b=2"], "x-id" => "10")
    end
  end
end
