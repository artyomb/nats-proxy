require_relative 'bridge_protocol'

class RequestContext
  CANCEL_TAIL_BUDGET = 1

  attr_reader :request_id, :response_queue, :cancel_reason
  attr_accessor :request_subject, :receiver_service_id, :worker, :upstream_queue, :tunnel_data_queue

  def initialize(request_id:, response_queue: nil, initial_state: 'pending_start')
    @request_id = request_id
    @response_queue = response_queue || Async::Queue.new
    @state = initial_state
    @outcome = nil
    @cancel_reason = nil
    @cancel_sent = false
    @cancel_tail_budget = CANCEL_TAIL_BUDGET
    @lock = Mutex.new
  end

  def cancel_requested? = synchronized { @state == 'cancel_requested' }

  def terminal? = synchronized { @state == 'terminal' }

  def mark_streaming!
    synchronized { @state = 'streaming' }
  end

  def mark_terminal!(outcome)
    synchronized do
      @state = 'terminal'
      @outcome ||= outcome
    end
  end

  def outcome
    synchronized { @outcome }
  end

  def request_cancel!(reason:)
    synchronized do
      next :terminal if @state == 'terminal'
      next :already_sent if @cancel_sent

      @state = 'cancel_requested'
      @cancel_reason = reason
      @outcome ||= BridgeProtocol::OUTCOME_CANCELED
      :ready
    end
  end

  def mark_cancel_sent!
    synchronized { @cancel_sent = true }
  end

  def observe_cancel!(reason:)
    synchronized do
      return false if @state == 'terminal' || @state == 'cancel_requested'

      @state = 'cancel_requested'
      @outcome = BridgeProtocol::OUTCOME_CANCELED
      @cancel_reason = reason
      true
    end
  end

  def allow_event_after_cancel?(event_type, allow_end: false)
    synchronized do
      return false if @state == 'terminal'
      return true unless @state == 'cancel_requested'
      return true if allow_end && [BridgeProtocol::RESPONSE_END, BridgeProtocol::SESSION_CLOSE].include?(event_type)
      return false unless event_type == BridgeProtocol::RESPONSE_CHUNK
      return false if @cancel_tail_budget <= 0

      @cancel_tail_budget -= 1
      true
    end
  end

  def finalize!(fallback_outcome:)
    synchronized do
      @state = 'terminal'
      @outcome ||= fallback_outcome
      @outcome
    end
  end

  private

  def synchronized(&block)
    @lock.synchronize(&block)
  end
end
