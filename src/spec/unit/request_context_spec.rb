require_relative "../spec_helper"
require_relative "../../request_context"

RSpec.describe RequestContext do
  subject(:context) { described_class.new(request_id: "req-1") }

  it "marks cancel only once and stores the first reason" do
    expect(context.request_cancel!(reason: "downstream_disconnect")).to eq(:ready)
    context.mark_cancel_sent!

    expect(context.request_cancel!(reason: "other")).to eq(:already_sent)
    expect(context.cancel_reason).to eq("downstream_disconnect")
    expect(context.outcome).to eq(BridgeProtocol::OUTCOME_CANCELED)
  end

  it "allows one trailing chunk after cancel and blocks the second" do
    context.observe_cancel!(reason: "client_closed")

    expect(context.allow_event_after_cancel?(BridgeProtocol::RESPONSE_CHUNK)).to be(true)
    expect(context.allow_event_after_cancel?(BridgeProtocol::RESPONSE_CHUNK)).to be(false)
    expect(context.allow_event_after_cancel?(BridgeProtocol::RESPONSE_END, allow_end: true)).to be(true)
  end

  it "preserves the first terminal outcome" do
    context.mark_terminal!(BridgeProtocol::OUTCOME_COMPLETED)
    context.finalize!(fallback_outcome: BridgeProtocol::OUTCOME_TIMEOUT)

    expect(context.outcome).to eq(BridgeProtocol::OUTCOME_COMPLETED)
    expect(context).to be_terminal
  end
end
