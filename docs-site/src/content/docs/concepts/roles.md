---
title: Roles
description: Requester and receiver responsibilities.
---

`SERVICE_ROLE` controls which side of the bridge a process runs. If it is not set, `src/config.ru` resolves `receiver` when `UPSTREAM_URL` is set and `requester` otherwise.

| Role | Starts | Uses |
|---|---|---|
| `requester` | Response listener, downstream TCP session listener, optional SOCKS5 listener | Local client traffic, NATS response subjects, session downstream subjects |
| `receiver` | Request listener, upstream TCP session listener | NATS request subjects, `UPSTREAM_URL`, requested TCP targets |

The requester accepts plain HTTP routes, HTTP proxy absolute-form requests, `CONNECT`, and SOCKS5 when enabled. It needs a live outbound bridge before it can send work through NATS.

The receiver consumes bridge envelopes and dispatches by `operation`. Current operations are `http_request` and `tcp_stream`.

