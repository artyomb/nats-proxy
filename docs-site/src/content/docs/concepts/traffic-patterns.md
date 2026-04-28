---
title: Traffic Patterns
description: How callers can use the requester endpoint.
---

A caller can use the requester in several HTTP-compatible ways. The requester turns the incoming request into either an HTTP request over NATS or a TCP tunnel over NATS.

| Pattern | Caller behavior | How it crosses NATS | Receiver behavior |
|---|---|---|---|
| Direct HTTP endpoint | Calls `http://requester:7000/path` like the upstream API. | HTTP request envelope. | Forwards to `UPSTREAM_URL/path`. |
| HTTP proxy absolute-form | Sends an absolute URL through an HTTP proxy, for example `curl -x http://requester:7000 http://example.test/path`. | HTTP request envelope. | Forwards to the absolute target URL. |
| HTTP `CONNECT` | Opens `CONNECT host:port HTTP/1.1`. | TCP tunnel session. | Opens a TCP connection to `host:port`. |
| SOCKS5 | Connects to the requester SOCKS5 listener and sends a SOCKS5 `CONNECT`. | TCP tunnel session. | Opens the same TCP tunnel flow as HTTP `CONNECT`. |
| Streaming HTTP response | Upstream returns `text/event-stream` or `application/x-ndjson`. | HTTP request plus streaming response events. | Streams chunks back to the requester as they arrive. |

## Direct HTTP

Direct HTTP is used when the caller should treat `nats-proxy` as if it were the upstream HTTP API. The caller sends normal origin-form paths such as `/api/tags`, and the receiver forwards them relative to `UPSTREAM_URL`.

Local service endpoints stay local:

- `/health`
- `/healthcheck`
- `/observability`
- `/observability/*`

Those endpoints are not bridged unless the request is classified as proxy traffic.

## HTTP Proxy

HTTP proxy traffic is detected when the request target is an absolute HTTP URL or when proxy-specific headers let the requester reconstruct an absolute target.

Proxy-specific HTTP traffic is guarded by proxy auth when `PROXY_AUTH_ENABLED` is true. Plain local health and observability routes do not require proxy auth.

## TCP Tunnels

HTTP `CONNECT` and SOCKS5 both open a TCP tunnel session. The receiver connects to the requested host and port, then binary frames move through NATS in both directions.

HTTP `CONNECT` requires Rack hijack support. The provided runtime commands use Falcon for that reason.

SOCKS5 is only available when `SOCKS5_ENABLED=true`. If proxy auth is enabled, SOCKS5 uses username/password authentication against the same configured users.

## Streaming Responses

`text/event-stream` and `application/x-ndjson` are treated as streaming media types. For these responses, the requester writes chunks to the downstream client as response events arrive.

If a stream fails after it has started, the requester writes an in-band error formatted for the stream type instead of replacing the whole response.
