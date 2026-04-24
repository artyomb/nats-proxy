---
title: Overview
description: Runtime composition and bridge responsibilities.
---

Runtime setup starts in `src/config.ru` and is composed by `ServiceRuntime`.

```mermaid
flowchart TB
  config["config.ru"] --> runtime["ServiceRuntime"]
  runtime --> nats["NatsAsyncRuntime"]
  runtime --> core["BridgeCore"]
  runtime --> http["HttpGateway"]
  runtime --> tcp["TcpTunnelBridge"]
  runtime --> socks["Socks5Server optional"]
  runtime --> obs["ObservabilityCollector"]
  runtime --> auth["ProxyAuth"]
```

`BridgeCore` owns NATS subjects, pending request contexts, request dispatch, response listeners, session frame subjects, JetStream pull consumers, and cancellation envelopes.

`HttpGateway` converts Rack requests to `http_request` payloads and converts bridge response events back into Rack responses. It can also execute direct upstream calls when the process has `UPSTREAM_URL` and the outbound bridge is unavailable.

`TcpTunnelBridge` maps `CONNECT` and SOCKS5 sessions to a `tcp_stream` operation and binary session subjects.

