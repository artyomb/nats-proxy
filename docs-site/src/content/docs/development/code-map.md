---
title: Code Map
description: Runtime file responsibilities and where to find tests.
---

This page is a maintainer map. Use it when you already know the service behavior and need to find the file or test suite that owns a specific part of it.

| Area | Runtime files | Tests |
|---|---|---|
| Rack entrypoint, routes, env, middleware | `src/config.ru` | `src/spec/contracts/config_ru_spec.rb` |
| Boot lifecycle and role-specific listeners | `src/service_runtime.rb` | `src/spec/unit/service_runtime_spec.rb` |
| NATS subjects, request dispatch, response/session listeners, JetStream consumers | `src/bridge_core.rb` | `src/spec/unit/bridge_core_spec.rb`, `src/spec/system/http_bridge_system_spec.rb` |
| Protocol helpers and event parsing | `src/bridge_protocol.rb` | `src/spec/unit/bridge_protocol_spec.rb` |
| Per-request state, cancellation state, queues | `src/request_context.rb` | `src/spec/unit/request_context_spec.rb` |
| HTTP forwarding and streaming response rendering | `src/http_gateway.rb` | `src/spec/unit/http_gateway_spec.rb`, `src/spec/system/http_bridge_system_spec.rb` |
| CONNECT and bridged TCP sessions | `src/tcp_tunnel_bridge.rb` | `src/spec/unit/tcp_tunnel_bridge_spec.rb`, `src/spec/system/connect_tunnel_system_spec.rb` |
| Rack CONNECT protocol support patch | `src/protocol_rack_connect_patch.rb` | `src/spec/unit/protocol_rack_connect_patch_spec.rb` |
| SOCKS5 listener and handshake | `src/socks5_server.rb` | `src/spec/unit/socks5_server_spec.rb`, `src/spec/system/socks5_system_spec.rb` |
| Proxy authentication and safety lock | `src/proxy_auth.rb` | `src/spec/unit/proxy_auth_spec.rb` |
| NATS client wrapper and backend resolution | `src/nats_async_runtime.rb` | `src/spec/unit/nats_async_runtime_spec.rb` |
| Flow events, cases, metrics, NATS payloads | `src/observability_collector.rb` | `src/spec/unit/observability_collector_spec.rb` |
| Embedded NATS runtime behavior | `docker/ruby/entrypoint.sh`, `docker/ruby/Dockerfile` | Validated by code review and deployment examples; no dedicated shell test currently exists. |
