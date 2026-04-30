---
title: Roles
description: What requester and receiver do in a nats-proxy bridge.
---

`nats-proxy` runs the same container image on both sides of the bridge. The role only decides which side of the request flow the process serves.

| Role | User-facing responsibility | Where it belongs |
|---|---|---|
| `requester` | Entry point for clients. It accepts HTTP, HTTP proxy, `CONNECT`, and optional SOCKS5 traffic, then returns the final response to the caller. | Wherever applications, browsers, CLIs, SDKs, or proxy settings can reach it. |
| `receiver` | Outbound side. It receives work through NATS and performs the real HTTP request or TCP connection. | Wherever the required `UPSTREAM_URL` or requested TCP target is reachable. |

In the normal bridge topology, the client talks to the requester. It does not need to know that NATS and a receiver exist behind the endpoint.

Both roles can have multiple live instances. Multiple requesters give callers multiple entry points. Multiple receivers act as outbound workers for new bridge work.

## Runtime Selection

`SERVICE_ROLE` explicitly selects the role. If it is not set, `src/config.ru` chooses:

| Condition | Resolved role |
|---|---|
| `UPSTREAM_URL` is set | `receiver` |
| `UPSTREAM_URL` is not set | `requester` |

This default is convenient for simple setups, but production deployments should set `SERVICE_ROLE` explicitly so the container intent is clear.

## What Each Role Starts

| Role | Runtime pieces |
|---|---|
| `requester` | Response listener, downstream TCP session listener, HTTP routes, `CONNECT` middleware, and optional SOCKS5 listener when `SOCKS5_ENABLED=true`. |
| `receiver` | Request listener, upstream TCP session listener, cancel listener, `http_request` handler, and `tcp_stream` handler. |

The requester publishes work into NATS and waits for response events or downstream tunnel frames. The receiver consumes that work, performs outbound HTTP/TCP, and publishes the result back through NATS.

NATS balances the start of a flow, not every message in the flow. A new HTTP request or tunnel session open can be handled by any receiver in the pool. Once the receiver publishes `response_start` or `session_established`, that receiver becomes the owner for the flow. Later stream cancellation and TCP session bytes are routed to that owner receiver, while responses and downstream tunnel bytes are routed back to the original requester.

At the protocol level, plain HTTP and HTTP proxy traffic use the HTTP request path. HTTP `CONNECT` and SOCKS5 use the TCP tunnel path. The exact bridge operation names are described in [Bridge Protocol](../architecture/bridge-protocol/).
