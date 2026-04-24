---
title: Environment
description: Environment variable reference for app runtime and embedded NATS entrypoint.
---

This table includes variables read by `src/config.ru`, `docker/ruby/entrypoint.sh`, and the runtime Dockerfile.

| Variable | Default | Role | Required | Source | Description | Example |
|---|---|---|---|---|---|---|
| `SERVICE_ROLE` | `receiver` if `UPSTREAM_URL` is set, else `requester` | both | No | `src/config.ru` | Runtime role. Must be `requester` or `receiver` when set. | `requester` |
| `UPSTREAM_URL` | unset | receiver | For receiver HTTP forwarding | `src/config.ru` | Base URL for receiver HTTP upstream. Also enables direct receiver HTTP proxying. | `http://app:8080` |
| `PORT` | `7000` in Docker image | both | No | `docker/ruby/Dockerfile` | Rack/Falcon bind port used by the image `CMD`. | `7000` |
| `NATS_URL` | `nats://localhost:4222` | both | No | `src/config.ru` | NATS server URL used by the app process. | `nats://127.0.0.1:4222` |
| `NATS_MODE` | `auto` | both | No | `src/config.ru` | Backend mode: `core`, `jetstream`, or `auto`. | `jetstream` |
| `NATS_STREAM` | `proxy` | both | JetStream only | `src/config.ru` | JetStream stream used by pull consumers and publishes. | `proxy` |
| `NATS_CONSUMER_NAME` | `nats-proxy` | both | No | `src/config.ru` | Base durable consumer name. Requester response/session consumers add suffixes. | `nats-proxy` |
| `NATS_QUEUE_GROUP` | value of `NATS_CONSUMER_NAME` | receiver | Core only | `src/config.ru` | Core NATS queue group for receiver request subscription. | `receivers` |
| `NATS_REQUEST_SUBJECT_ROOT` | `to.proxy` | both | No | `src/config.ru` | Root for request and upstream session subjects. | `to.proxy` |
| `NATS_RESPONSE_SUBJECT_ROOT` | `from.proxy` | both | No | `src/config.ru` | Root for response and downstream session subjects. | `from.proxy` |
| `LISTEN_SUBJECT` | `<request_root>.requests.>` | receiver | No | `src/config.ru` | Receiver subscription filter for bridge requests. | `to.proxy.requests.>` |
| `NATS_JS_API_PREFIX` | app default `$JS.API`; embedded entrypoint may export resolved value | both | No | `src/config.ru`, `entrypoint.sh` | JetStream API prefix. In embedded mode resolves from explicit value, `EMBEDDED_NATS_JS_DOMAIN`, or `$JS.API`. | `$JS.DOMAIN.API` |
| `SERVICE_ID` | `srv-<random>` | both | No | `src/config.ru` | Instance identifier used in subjects and observability. | `requester-a` |
| `NATS_RESPONSE_TIMEOUT` | `30` | requester | No | `src/config.ru` | Timeout waiting for `response_start` or `session_established`. | `30` |
| `STREAM_RESPONSE_TIMEOUT` | `30` | requester | No | `src/config.ru` | Idle timeout between response events or tunnel frames after start. | `30` |
| `RECEIVER_MAX_INFLIGHT` | `20` | receiver | No | `src/config.ru` | Receiver dispatch concurrency; queue size is twice this value. | `20` |
| `SOCKS5_ENABLED` | `false` | requester | No | `src/config.ru` | Enables requester SOCKS5 listener. | `true` |
| `SOCKS5_LISTEN_HOST` | `0.0.0.0` | requester | No | `src/config.ru` | SOCKS5 bind host. | `0.0.0.0` |
| `SOCKS5_LISTEN_PORT` | `1080` | requester | No | `src/config.ru` | SOCKS5 bind port. | `1080` |
| `PROXY_AUTH_ENABLED` | `true` | requester | No | `src/config.ru` | Enables proxy auth for proxy-specific ingress. Set exactly `false` to disable. | `true` |
| `PROXY_AUTH_USERS_JSON` | unset | requester | When auth enabled | `src/config.ru` | JSON object of username to bcrypt hash. Missing or invalid value triggers safety lock. | `{"alice":"$2a$12$..."}` |
| `APP_ENV` | unset | both | No | `src/config.ru` | When set to `test`, disables async warmup patch. | `test` |
| `RACK_ENV` | `production` in Docker image | both | No | `Dockerfile` | Rack environment. | `production` |
| `SERVER_ENV` | `production` in Docker image | both | No | `Dockerfile` | Runtime environment marker from image. | `production` |
| `RUBY_YJIT_ENABLE` | `1` in Docker image | both | No | `Dockerfile` | Enables Ruby YJIT in the deploy image. | `1` |
| `EMBEDDED_NATS_ENABLED` | `false` | both | No | `entrypoint.sh`, `Dockerfile` | Starts embedded `nats-server` before the Rack app. | `true` |
| `EMBEDDED_NATS_CONFIG` | empty | both | Required if embedded enabled without generation | `entrypoint.sh`, `Dockerfile` | Static NATS config path. | `/data/nats.conf` |
| `EMBEDDED_NATS_GENERATE_CONFIG` | `false` | both | Required if embedded enabled without static config | `entrypoint.sh`, `Dockerfile` | Generates a NATS config to `EMBEDDED_NATS_GENERATED_CONFIG_PATH`. | `true` |
| `EMBEDDED_NATS_GENERATED_CONFIG_PATH` | `/data/nats.conf` | both | No | `entrypoint.sh`, `Dockerfile` | Generated config path. | `/data/nats.conf` |
| `EMBEDDED_NATS_JETSTREAM_ENABLED` | receiver `true`, requester `false` when empty | both | No | `entrypoint.sh`, `Dockerfile` | Controls `jetstream` block in generated config. | `true` |
| `EMBEDDED_NATS_JETSTREAM_STORE_DIR` | `/data` | both | No | `entrypoint.sh`, `Dockerfile` | File store directory for embedded JetStream. | `/data` |
| `EMBEDDED_NATS_JS_DOMAIN` | empty | both | No | `entrypoint.sh`, `Dockerfile` | Optional JetStream domain in generated config and JS API prefix resolution. | `EDGE` |
| `EMBEDDED_NATS_LEAF_LISTEN_HOST` | `0.0.0.0` | both | No | `entrypoint.sh`, `Dockerfile` | Leafnode listener host in generated config. | `0.0.0.0` |
| `EMBEDDED_NATS_LEAF_LISTEN_PORT` | `7422` | both | No | `entrypoint.sh`, `Dockerfile` | Leafnode listener port in generated config. | `7422` |
| `EMBEDDED_NATS_LEAF_USER` | generated or reused from existing config | both | No | `entrypoint.sh`, `Dockerfile` | Leaf listener username. If absent, generated config creates or reuses one. | `leaf_user` |
| `EMBEDDED_NATS_LEAF_PASSWORD` | generated or reused from existing config | both | No | `entrypoint.sh`, `Dockerfile` | Leaf listener password. Must be paired with user if set. | `secret` |
| `LEAF_REMOTE_HOST` | empty | requester | Embedded requester generation | `entrypoint.sh`, `Dockerfile` | Receiver leafnode host for requester remotes. | `receiver.example.com` |
| `LEAF_REMOTE_PORT` | `7422` | requester | No | `entrypoint.sh`, `Dockerfile` | Receiver leafnode port. | `7422` |
| `LEAF_REMOTE_USER` | empty | requester | Unless using `LEAF_REMOTE_NKEY` | `entrypoint.sh`, `Dockerfile` | Username for requester to connect to receiver leafnode. | `leaf_user` |
| `LEAF_REMOTE_PASSWORD` | empty | requester | Unless using `LEAF_REMOTE_NKEY` | `entrypoint.sh`, `Dockerfile` | Password for requester leaf remote. | `secret` |
| `LEAF_REMOTE_NKEY` | empty | requester | Alternative to user/password | `entrypoint.sh`, `Dockerfile` | NKey auth for requester leaf remote. Cannot be combined with user/password. | `UD...` |
| `EMBEDDED_NATS_READY_RETRIES` | `40` | embedded | No | `entrypoint.sh` | Attempts for `nats rtt` readiness before stream bootstrap fails. | `40` |
| `EMBEDDED_NATS_READY_SLEEP_SEC` | `1` | embedded | No | `entrypoint.sh` | Sleep between embedded NATS readiness attempts. | `1` |

