---
title: Bridge Protocol
description: HTTP request envelopes and response event framing.
---

The bridge protocol is JSON-framed for HTTP work. Requester publishes a request envelope to a request subject and listens for response events on `reply_to`.

## Request Envelope

`BridgeProtocol.request_envelope` builds the envelope:

```json
{
  "type": "request",
  "request_id": "req-id",
  "reply_to": "from.proxy.responses.requester-service.req-id",
  "operation": "http_request",
  "payload": {
    "method": "GET",
    "path": "/path",
    "headers": {},
    "body": null
  }
}
```

Required fields are `request_id`, `reply_to`, `operation`, and `payload`. `BridgeCore` validates these fields before dispatching to a registered handler.

## Subjects

| Purpose | Pattern |
|---|---|
| Per-request work | `<NATS_REQUEST_SUBJECT_ROOT>.requests.<SERVICE_ID>.<request_id>` |
| Response events | `<NATS_RESPONSE_SUBJECT_ROOT>.responses.<SERVICE_ID>.<request_id>` |
| Receiver listen subject | `LISTEN_SUBJECT`, default `<request_root>.requests.>` |

## Response Events

| Event | Payload |
|---|---|
| `response_start` | HTTP `status`, normalized response `headers`, `content_type`, and `streaming`. |
| `response_chunk` | `body` for UTF-8 chunks, or `body_encoding=base64` and `body_base64` for binary chunks. |
| `response_error` | `error` string. Used for stream failures and cancellation diagnostics. |
| `response_end` | Terminal marker for an HTTP response. |

`text/event-stream` and `application/x-ndjson` responses are treated as streaming. Other responses are buffered until `response_end`.

## Non-Streaming Request

```mermaid
sequenceDiagram
  participant Client
  participant Req as requester
  participant NATS
  participant Rec as receiver
  participant Up as upstream

  Client->>Req: HTTP request
  Req->>NATS: request envelope with reply_to
  NATS->>Rec: http_request
  Rec->>Up: forwarded HTTP request
  Up-->>Rec: complete HTTP response
  Rec-->>NATS: response_start(streaming=false)
  Rec-->>NATS: response_chunk
  Rec-->>NATS: response_end
  NATS-->>Req: response events
  Req-->>Client: HTTP response
```

## Streaming Response

```mermaid
sequenceDiagram
  participant Client
  participant Req as requester
  participant NATS
  participant Rec as receiver
  participant Up as upstream

  Client->>Req: HTTP request
  Req->>NATS: request envelope
  NATS->>Rec: http_request
  Rec->>Up: forwarded HTTP request
  Up-->>Rec: SSE or NDJSON stream
  Rec-->>NATS: response_start(streaming=true)
  loop each upstream chunk
    Rec-->>NATS: response_chunk
    NATS-->>Req: response_chunk
    Req-->>Client: stream chunk
  end
  Rec-->>NATS: response_end
  Req-->>Client: close response body
```

## Error During Stream

```mermaid
sequenceDiagram
  participant Client
  participant Req as requester
  participant NATS
  participant Rec as receiver
  participant Up as upstream

  Client->>Req: streaming HTTP request
  Req->>NATS: request envelope
  NATS->>Rec: http_request
  Rec->>Up: forwarded HTTP request
  Up-->>Rec: starts stream
  Rec-->>NATS: response_start(streaming=true)
  Rec-->>NATS: response_chunk
  Up--xRec: timeout or connection failure
  Rec-->>NATS: response_error
  Rec-->>NATS: response_end
  Req-->>Client: in-band stream error then close
```

