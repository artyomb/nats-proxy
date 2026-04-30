---
title: Topology
description: How requester, receiver, NATS, and upstream targets are placed.
---

Topology is the deployment shape around the same bridge idea:

- callers must be able to reach the requester;
- the receiver must be able to reach the upstream HTTP service or requested TCP target;
- requester and receiver must both be able to reach NATS;
- direct network access between caller and upstream is not required.

## Placement Rule

| Component | Place it where... | It needs access to... |
|---|---|---|
| Caller | The application, UI, browser, SDK, CLI, or system proxy setting already runs. | The requester endpoint. |
| Requester | Callers can address it as an HTTP endpoint, HTTP proxy, or SOCKS5 proxy. | NATS. |
| Receiver | Outbound requests should be executed. | NATS and the required `UPSTREAM_URL` or TCP target. |
| NATS | Both sides can connect to it. | Requester and receiver clients. |

The requester can be local to a developer machine, inside an application stack, on a gateway server, or anywhere else that callers can reach. The receiver can run in a different network segment, on a host close to the upstream service, or near a TCP target. Those placement choices are deployment decisions; the bridge contract stays the same.

## High-Level Flow

```mermaid
flowchart LR
  caller["Caller<br/>app / UI / proxy settings"]
  target["UPSTREAM_URL<br/>or TCP target"]

  subgraph transport["NATS transport"]
    requester["requester<br/>client-facing endpoint"]
    receiver["receiver<br/>outbound side"]
    requester <-->|request / response<br/>or tunnel frames| receiver
  end

  caller <-->|HTTP / proxy / CONNECT / SOCKS5| requester
  receiver <-->|HTTP request<br/>or TCP connection| target
```

The caller and target stay outside the NATS transport. Only requester and receiver have to share the NATS path.

## Multi-Instance Shape

The same placement rule works when there are several requesters or receivers:

```mermaid
flowchart LR
  caller_a["Caller A"] --> req_a["requester A"]
  caller_b["Caller B"] --> req_b["requester B"]
  caller_c["Caller C"] --> req_c["requester C"]
  caller_d["Caller D"] --> req_d["requester D"]
  caller_e["Caller E"] --> req_e["requester E"]

  nats["NATS<br/>request/session-open distribution"]

  subgraph requesters["Requester instances"]
    req_a
    req_b
    req_c
    req_d
    req_e
  end

  subgraph receivers["Receiver pool"]
    rec_1["receiver 1"]
    rec_2["receiver 2"]
    rec_3["receiver 3"]
  end

  req_a --> nats
  req_b --> nats
  req_c --> nats
  req_d --> nats
  req_e --> nats

  nats -. "new flow can land on any receiver" .-> rec_1
  nats -. "new flow can land on any receiver" .-> rec_2
  nats -. "new flow can land on any receiver" .-> rec_3

  rec_1 --> target["UPSTREAM_URL<br/>or TCP target"]
  rec_2 --> target
  rec_3 --> target
```

Requesters are not usually a pool for each other. They are the endpoints your callers use. You can run one requester per application, per host, per tenant, or behind your own load balancer.

Receivers can form a worker pool. NATS distributes only the initial HTTP request or tunnel session open, so the pool size does not need to match the requester count. After a receiver is selected, streaming chunks, TCP tunnel bytes, and owner-addressed cancellation stay routed between the original requester and that selected receiver.

For that to stay unambiguous, live requester and receiver instances should use stable, unique `SERVICE_ID` values. In Core NATS, receiver replicas share `NATS_QUEUE_GROUP`. In JetStream, receiver replicas share the request `NATS_CONSUMER_NAME`.

## What Changes Between Topologies

Different deployments usually change only these things:

| Decision | Examples |
|---|---|
| Where callers find the requester | `127.0.0.1`, a compose service name, a gateway host, an internal load balancer. |
| How requester and receiver reach NATS | Shared NATS, external NATS, embedded NATS, JetStream, Core NATS, or leafnodes. |
| What the receiver can reach | A fixed `UPSTREAM_URL`, an absolute HTTP proxy target, or a TCP target requested by `CONNECT`/SOCKS5. |

Detailed run commands and deployment variants are documented in the deployment section. This page only describes the placement model.
