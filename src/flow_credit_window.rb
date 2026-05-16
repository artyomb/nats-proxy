require 'async'
require 'async/queue'

class FlowCreditWindow
  DEFAULT_WINDOW_CHUNKS = 32
  DEFAULT_BATCH_CHUNKS = 8
  DEFAULT_MAX_WINDOW_MULTIPLIER = 4
  TIMEOUT = :timeout

  def self.default_initial_bytes(chunk_size:)
    positive_chunk_size(chunk_size) * DEFAULT_WINDOW_CHUNKS
  end

  def self.default_batch_bytes(chunk_size:)
    positive_chunk_size(chunk_size) * DEFAULT_BATCH_CHUNKS
  end

  def self.default_max_bytes(chunk_size:)
    default_initial_bytes(chunk_size:) * DEFAULT_MAX_WINDOW_MULTIPLIER
  end

  attr_reader :available_bytes, :closed_reason

  def initialize(initial_bytes: 0, max_bytes:)
    @available_bytes = [initial_bytes.to_i, 0].max
    @max_bytes = [max_bytes.to_i, 1].max
    @available_bytes = [@available_bytes, @max_bytes].min
    @closed = false
    @closed_reason = nil
    @waiters = 0
    @generation = 0
    @lock = Mutex.new
    @signals = Async::Queue.new
  end

  def grant(bytes)
    amount = bytes.to_i
    return false if amount <= 0

    waiters = 0
    @lock.synchronize do
      return false if @closed

      @available_bytes = [@available_bytes + amount, @max_bytes].min
      @generation += 1
      waiters = @waiters
    end
    signal_waiters(waiters)
    true
  end

  def reserve(max_bytes, timeout:, cancel_check: nil, on_wait: nil)
    requested = max_bytes.to_i
    return nil if requested <= 0

    deadline = timeout.to_f.positive? ? Time.now + timeout.to_f : nil
    wait_recorded = false

    loop do
      reserved, generation = try_reserve(requested)
      return reserved if reserved
      return nil unless generation
      return nil if cancel_check&.call

      remaining = deadline ? deadline - Time.now : nil
      return TIMEOUT if remaining && remaining <= 0

      unless wait_recorded
        on_wait&.call
        wait_recorded = true
      end
      wait_for_credit(timeout: remaining, generation:)
    end
  end

  def close(reason: 'closed')
    waiters = 0
    @lock.synchronize do
      return false if @closed

      @closed = true
      @closed_reason = reason.to_s
      @generation += 1
      waiters = @waiters
    end
    signal_waiters(waiters)
    true
  end

  def closed?
    @lock.synchronize { @closed }
  end

  private

  def self.positive_chunk_size(chunk_size)
    [chunk_size.to_i, 1].max
  end
  private_class_method :positive_chunk_size

  def try_reserve(requested)
    @lock.synchronize do
      return nil if @closed
      return [nil, @generation] if @available_bytes <= 0

      reserved = [requested, @available_bytes].min
      @available_bytes -= reserved
      [reserved, @generation]
    end
  end

  def wait_for_credit(timeout:, generation:)
    @lock.synchronize do
      return if @closed || @available_bytes.positive? || @generation != generation

      @waiters += 1
    end
    if timeout
      @signals.dequeue(timeout:)
    else
      @signals.dequeue
    end
  rescue Async::TimeoutError
    TIMEOUT
  ensure
    @lock.synchronize { @waiters -= 1 if @waiters.positive? }
  end

  def signal_waiters(waiters)
    waiters.to_i.times { @signals.enqueue(true) }
  end
end
