require_relative "../spec_helper"

RSpec.describe "CONNECT tunnel load", :system, :load do
  before do
    skip "set CONNECT_LOAD_SPEC=1 to run CONNECT load checks" unless ENV["CONNECT_LOAD_SPEC"] == "1"
  end

  it "keeps concurrent CONNECT tunnels functional under sustained echo traffic" do
    mode = ENV.fetch("CONNECT_LOAD_MODE", "core").to_sym
    concurrency = ENV.fetch("CONNECT_LOAD_CONCURRENCY", "20").to_i
    rounds = ENV.fetch("CONNECT_LOAD_ROUNDS", "3").to_i
    payload_bytes = ENV.fetch("CONNECT_LOAD_PAYLOAD_BYTES", "65536").to_i
    response_timeout = ENV.fetch("CONNECT_LOAD_RESPONSE_TIMEOUT", "10").to_i
    receiver_max_inflight = ENV.fetch("CONNECT_LOAD_RECEIVER_MAX_INFLIGHT", "20").to_i
    stream_timeout = ENV.fetch("CONNECT_LOAD_STREAM_TIMEOUT", "5").to_i

    upstream = SystemHttpServer.new
    echo_server = SystemEchoServer.new
    payload = "x".b * payload_bytes

    with_service_cluster(
      mode:,
      upstream_url: upstream.base_url,
      response_timeout:,
      stream_timeout:,
      receiver_max_inflight:
    ) do |cluster|
      requester = cluster.fetch(:requester)
      receiver = cluster.fetch(:receiver)
      started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      results = Queue.new
      threads = concurrency.times.map do |index|
        Thread.new do
          run_connect_echo_client(
            requester_port: requester.port,
            target_port: echo_server.port,
            payload: payload,
            rounds: rounds,
            index: index
          )
          results << { ok: true }
        rescue StandardError => e
          results << { ok: false, error: "#{e.class}: #{e.message}" }
        end
      end

      threads.each(&:join)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
      failures = drain_queue(results).reject { |result| result.fetch(:ok) }

      aggregate_failures do
        expect(failures).to be_empty, load_summary(
          failures:,
          concurrency:,
          rounds:,
          payload_bytes:,
          elapsed:
        )
        expect(requester).to be_alive
        expect(receiver).to be_alive
      end
    end
  ensure
    upstream&.stop
    echo_server&.stop
  end

  def run_connect_echo_client(requester_port:, target_port:, payload:, rounds:, index:)
    socket = open_connect_tunnel(
      host: "127.0.0.1",
      port: requester_port,
      target: "127.0.0.1:#{target_port}"
    )

    rounds.times do |round|
      socket.write(payload)
      echoed = read_exact(socket, payload.bytesize)
      raise "client=#{index} round=#{round} echo mismatch" unless echoed == payload
    end
  ensure
    socket&.close
  end

  def read_exact(socket, bytes)
    data = +"".b
    data << socket.readpartial([bytes - data.bytesize, 16_384].min) while data.bytesize < bytes
    data
  end

  def drain_queue(queue)
    items = []
    items << queue.pop(true) while true
  rescue ThreadError
    items
  end

  def load_summary(failures:, concurrency:, rounds:, payload_bytes:, elapsed:)
    <<~MSG
      CONNECT load failed
      concurrency=#{concurrency}, rounds=#{rounds}, payload_bytes=#{payload_bytes}, elapsed=#{elapsed.round(3)}s
      failures=#{failures.first(10)}
    MSG
  end
end
