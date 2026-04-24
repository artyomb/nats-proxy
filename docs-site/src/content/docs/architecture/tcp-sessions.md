---
title: TCP Sessions
description: CONNECT and SOCKS5 tunnel flow over NATS.
---

`CONNECT` and SOCKS5 both use the `tcp_stream` bridge operation. The requester opens a session, the receiver connects to the requested `host:port`, then binary frames move over session subjects.

## Session Open Payload

```json
{
  "operation": "tcp_stream",
  "payload": {
    "host": "example.internal",
    "port": 443,
    "ingress_kind": "http_connect",
    "method": "CONNECT",
    "requester_service_id": "requester-1"
  }
}
```

For SOCKS5, `ingress_kind` is `socks5` and `method` is `SOCKS5_CONNECT`.

## Flow

```mermaid
sequenceDiagram
  participant Client
  participant Req as requester
  participant NATS
  participant Rec as receiver
  participant Target as TCP target

  Client->>Req: CONNECT or SOCKS5 CONNECT
  Req->>NATS: tcp_stream request envelope
  NATS->>Rec: session open request
  Rec->>Target: TCP connect
  Rec-->>NATS: session_established
  NATS-->>Req: session_established
  Req-->>Client: HTTP 200 or SOCKS success
  loop client to target
    Client->>Req: bytes
    Req->>NATS: upstream session_data
    NATS->>Rec: upstream frame
    Rec->>Target: bytes
  end
  loop target to client
    Target-->>Rec: bytes
    Rec-->>NATS: downstream session_data
    NATS-->>Req: downstream frame
    Req-->>Client: bytes
  end
  Rec-->>NATS: session_close
  NATS-->>Req: session_close
```

If the receiver cannot connect to the target, it emits a controlled bridge response with HTTP status `502` for the session open. If session establishment does not arrive before `NATS_RESPONSE_TIMEOUT`, requester returns a timeout to the caller.

Requester and receiver chunk size is capped by the NATS max payload and the local default chunk size of 32 KiB.

