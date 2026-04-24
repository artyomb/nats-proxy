---
title: Topology
description: Common requester placements and end-to-end traffic flow.
---

The requester should be placed where callers can use it as a local or nearby proxy. The receiver should be placed where the upstream service or TCP target is reachable.

## Requester Placement

| Placement | When it fits | Caller address |
|---|---|---|
| Workstation | Browser, CLI, SDK, or manual debugging from one machine | `127.0.0.1:<requester-port>` |
| Gateway server | Several clients share one proxy endpoint | Gateway host and published requester port |
| Application stack | Services in the same compose/Kubernetes network need egress through the bridge | Requester service DNS name |

```mermaid
flowchart LR
  subgraph workstation["Workstation"]
    cli["Browser / CLI / SDK"] --> req_local["requester"]
  end

  subgraph gateway["Gateway server"]
    req_gateway["requester"]
  end

  subgraph app["Application stack"]
    svc_a["Service A"] --> req_stack["requester"]
    svc_b["Service B"] --> req_stack
  end

  subgraph transport["NATS transport"]
    nats[("Core NATS / JetStream / leafnode")]
  end

  subgraph remote["Remote network"]
    receiver["receiver"]
    upstream["Upstream HTTP service / TCP target"]
    receiver --> upstream
  end

  cli -. optional network proxy .-> req_gateway
  req_local --> nats
  req_gateway --> nats
  req_stack --> nats
  nats --> receiver
```

## Traffic Flow

After a request reaches a requester, HTTP and TCP proxy traffic follow the same high-level path:

```mermaid
sequenceDiagram
  participant Caller as Caller
  participant Req as requester
  participant NATS as NATS
  participant Rec as receiver
  participant Up as Upstream / TCP target

  Caller->>Req: HTTP / absolute-form / CONNECT / SOCKS5
  Req->>NATS: request envelope or session frame
  NATS->>Rec: bridge traffic
  Rec->>Up: HTTP request or TCP connection
  Up-->>Rec: response or bytes
  Rec-->>NATS: response event or downstream frame
  NATS-->>Req: bridged response
  Req-->>Caller: HTTP response or tunnel bytes
```

Only NATS has to be reachable between requester and receiver. The requester does not need direct network access to `UPSTREAM_URL`.

