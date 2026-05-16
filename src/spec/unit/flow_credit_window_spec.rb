require_relative "../spec_helper"
require_relative "../../flow_credit_window"

RSpec.describe FlowCreditWindow do
  it "blocks reserve until credit is granted" do
    window = described_class.new(max_bytes: 1024)

    Sync do |task|
      result = nil
      waiting = Async::Queue.new
      waiter = task.async do
        result = window.reserve(512, timeout: 1, on_wait: -> { waiting.enqueue(true) })
      end
      waiting.dequeue

      expect(result).to be_nil
      window.grant(256)
      waiter.wait

      expect(result).to eq(256)
    end
  end

  it "reserves no more than available credit" do
    window = described_class.new(initial_bytes: 100, max_bytes: 1024)

    Sync do
      expect(window.reserve(256, timeout: 0.1)).to eq(100)
      expect(window.reserve(1, timeout: 0.01)).to eq(described_class::TIMEOUT)
    end
  end

  it "caps available credit at the configured maximum" do
    window = described_class.new(max_bytes: 300)
    window.grant(500)

    Sync do
      expect(window.reserve(1_000, timeout: 0.1)).to eq(300)
    end
  end

  it "wakes waiters when closed" do
    window = described_class.new(max_bytes: 1024)

    Sync do |task|
      result = :unset
      waiting = Async::Queue.new
      waiter = task.async do
        result = window.reserve(512, timeout: 1, on_wait: -> { waiting.enqueue(true) })
      end
      waiting.dequeue
      window.close(reason: "done")
      waiter.wait

      expect(result).to be_nil
      expect(window.closed_reason).to eq("done")
      expect(window.reserve(1, timeout: 0.1)).to be_nil
    end
  end

  it "derives default window sizes from chunk size" do
    expect(described_class.default_initial_bytes(chunk_size: 32_768)).to eq(1_048_576)
    expect(described_class.default_batch_bytes(chunk_size: 32_768)).to eq(262_144)
    expect(described_class.default_max_bytes(chunk_size: 32_768)).to eq(4_194_304)
  end

  it "reports wait only when reserve blocks" do
    window = described_class.new(initial_bytes: 10, max_bytes: 10)
    waits = 0

    Sync do
      expect(window.reserve(5, timeout: 0.1, on_wait: -> { waits += 1 })).to eq(5)
      expect(window.reserve(10, timeout: 0.01, on_wait: -> { waits += 1 })).to eq(5)
      expect(window.reserve(1, timeout: 0.01, on_wait: -> { waits += 1 })).to eq(described_class::TIMEOUT)
    end

    expect(waits).to eq(1)
  end
end
