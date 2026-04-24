---
title: Cancellation
description: Best-effort cancellation for disconnected streams and tunnels.
---

Cancellation is best effort. It is used when downstream streaming clients disconnect or a tunnel writer times out or fails. The requester publishes a cancel envelope to the original request subject.

```json
{
  "type": "cancel",
  "request_id": "req-id",
  "cancel": {
    "request_id": "req-id",
    "service_id": "requester-1",
    "reason": "downstream_disconnect",
    "timestamp": "2026-04-24T00:00:00Z"
  }
}
```

Receiver-side active streams observe this envelope through `BridgeCore`. Duplicate or late cancels are ignored. A stream can still emit a bounded trailing event after cancel because the upstream call and NATS delivery are asynchronous.

Observed outcomes are recorded by `ObservabilityCollector` as `canceled`, distinct from `success`, `timeout`, and `error`.

