---
title: Traffic Patterns
description: Supported requester-side ingress patterns.
---

`nats-proxy` supports several requester-side ingress patterns. This page describes user-facing behavior, not the internal bridge wire protocol.

| Pattern | How the caller sends it | What receiver does | Notes |
|---|---|---|---|
| Plain HTTP | `curl http://requester:7000/path` | Forwards to `UPSTREAM_URL/path` | Local `/health`, `/healthcheck`, and `/observability*` remain local unless the request is classified as proxy traffic. |
| HTTP proxy absolute-form | `curl -x http://requester:7000 http://example.test/path` | Forwards to the absolute target URL | Classified as proxy-specific traffic and guarded by proxy auth when enabled. |
| HTTP `CONNECT` | `CONNECT host:port HTTP/1.1` | Opens a TCP connection to `host:port` | Requires Rack hijack support; Falcon is used in the provided commands. |
| SOCKS5 | SOCKS5 `CONNECT` to requester SOCKS port | Opens the same TCP session flow as `CONNECT` | Enabled with `SOCKS5_ENABLED=true`. |
| SSE / NDJSON streaming | Upstream responds with `text/event-stream` or `application/x-ndjson` | Emits streaming response events | If a stream fails after start, requester writes an in-band error for SSE or NDJSON. |

Proxy auth applies to proxy-specific ingress: absolute-form HTTP proxy requests, legacy proxy requests detected from proxy headers, `CONNECT`, and SOCKS5. Plain local health and observability routes do not require proxy auth.

