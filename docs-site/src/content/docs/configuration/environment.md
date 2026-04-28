---
title: Environment
description: Environment variable reference for the app runtime and embedded NATS entrypoint.
---

This page lists environment variables read by the Ruby app, Docker runtime image, and embedded NATS entrypoint. Use it as a reference after choosing the role, topology, and traffic pattern.

## Runtime Role And HTTP

| Variable | Default | Applies to | Required when | Description | Example |
|---|---|---|---|---|---|
| `SERVICE_ROLE` | `receiver` if `UPSTREAM_URL` is set, else `requester` | both | No | Runtime role. Must be `requester` or `receiver` when set. | `requester` |
| `SERVICE_ID` | `srv-<random>` | both | No | Instance identifier used in NATS subjects and observability output. Set a stable value when you need predictable subject scopes. | `requester-a` |
| `UPSTREAM_URL` | unset | receiver | Receiver-side HTTP forwarding | Base URL used by `http_request` handling when the requester sends origin-form paths. TCP tunnels do not require it. | `http://app:8080` |
| `PORT` | `7000` in Docker image | both | No | Rack/Falcon bind port used by the runtime image `CMD`. | `7000` |
| `APP_ENV` | unset | both | No | When set to `test`, disables the async warmup hook in `config.ru`. | `test` |
| `RACK_ENV` | `production` in Docker image | both | No | Rack environment. | `production` |
| `SERVER_ENV` | `production` in Docker image | both | No | Runtime environment marker from the deploy image. | `production` |
| `RUBY_YJIT_ENABLE` | `1` in Docker image | both | No | Enables Ruby YJIT in the deploy image. | `1` |

## NATS Bridge

| Variable | Default | Applies to | Required when | Description | Example |
|---|---|---|---|---|---|
| `NATS_URL` | `nats://localhost:4222` | both | No | NATS server URL used by the Ruby app process. | `nats://127.0.0.1:4222` |
| `NATS_MODE` | `auto` | both | No | Backend mode: `core`, `jetstream`, or `auto`. | `jetstream` |
| `NATS_STREAM` | `proxy` | both | JetStream mode | JetStream stream used by pull consumers and JetStream publishes. | `proxy` |
| `NATS_CONSUMER_NAME` | `nats-proxy` | both | No | Base durable consumer name. Requester response/session consumers add service-specific suffixes. | `nats-proxy` |
| `NATS_QUEUE_GROUP` | value of `NATS_CONSUMER_NAME` | receiver | Core NATS mode | Queue group for the receiver request subscription. | `receivers` |
| `NATS_REQUEST_SUBJECT_ROOT` | `to.proxy` | both | No | Root for request and upstream session subjects. | `to.proxy` |
| `NATS_RESPONSE_SUBJECT_ROOT` | `from.proxy` | both | No | Root for response and downstream session subjects. | `from.proxy` |
| `LISTEN_SUBJECT` | `<request_root>.requests.>` | receiver | No | Receiver subscription filter for bridge request envelopes. | `to.proxy.requests.>` |
| `NATS_JS_API_PREFIX` | `$JS.API`, or resolved from embedded NATS domain | both | No | JetStream API prefix. Embedded mode exports the resolved value after starting `nats-server`. | `$JS.DOMAIN.API` |

## Timeouts And Receiver Load

| Variable | Default | Applies to | Required when | Description | Example |
|---|---|---|---|---|---|
| `NATS_RESPONSE_TIMEOUT` | `30` | requester | No | Timeout waiting for `response_start` or `session_established`. | `30` |
| `STREAM_RESPONSE_TIMEOUT` | `30` | requester | No | Idle timeout between response events or tunnel frames after a response/session starts. | `30` |
| `RECEIVER_MAX_INFLIGHT` | `20` | receiver | No | Receiver dispatch concurrency. Internal queue size is twice this value. | `20` |

## Proxy And SOCKS5 Ingress

| Variable | Default | Applies to | Required when | Description | Example |
|---|---|---|---|---|---|
| `PROXY_AUTH_ENABLED` | `true` | requester | No | Enables proxy auth for proxy-specific ingress. Set exactly `false` to disable. | `true` |
| `PROXY_AUTH_USERS_JSON` | unset | requester | Proxy auth is enabled | JSON object of username to bcrypt hash. Missing or invalid value triggers the safety lock. | `{"alice":"$2a$12$..."}` |
| `SOCKS5_ENABLED` | `false` | requester | No | Starts the requester SOCKS5 listener when `true`. | `true` |
| `SOCKS5_LISTEN_HOST` | `0.0.0.0` | requester | No | SOCKS5 bind host. | `0.0.0.0` |
| `SOCKS5_LISTEN_PORT` | `1080` | requester | No | SOCKS5 bind port. | `1080` |

## Embedded NATS

These variables are read by `docker/ruby/entrypoint.sh` before the Ruby app starts.

| Variable | Default | Applies to | Required when | Description | Example |
|---|---|---|---|---|---|
| `EMBEDDED_NATS_ENABLED` | `false` | both | No | Starts embedded `nats-server` before the Rack app. | `true` |
| `EMBEDDED_NATS_CONFIG` | empty | both | Embedded NATS enabled without generated config | Static NATS config path. | `/data/nats.conf` |
| `EMBEDDED_NATS_GENERATE_CONFIG` | `false` | both | Embedded NATS enabled without static config | Generates a NATS config to `EMBEDDED_NATS_GENERATED_CONFIG_PATH`. | `true` |
| `EMBEDDED_NATS_GENERATED_CONFIG_PATH` | `/data/nats.conf` | both | No | Generated config path. | `/data/nats.conf` |
| `EMBEDDED_NATS_JETSTREAM_ENABLED` | receiver `true`, requester `false` when empty | both | No | Controls whether generated config includes a `jetstream` block. | `true` |
| `EMBEDDED_NATS_JETSTREAM_STORE_DIR` | `/data` | both | No | File store directory for embedded JetStream. | `/data` |
| `EMBEDDED_NATS_JS_DOMAIN` | empty | both | No | Optional JetStream domain in generated config and JS API prefix resolution. | `EDGE` |
| `EMBEDDED_NATS_READY_RETRIES` | `40` | embedded | No | Attempts for `nats rtt` readiness before stream bootstrap fails. | `40` |
| `EMBEDDED_NATS_READY_SLEEP_SEC` | `1` | embedded | No | Sleep between embedded NATS readiness attempts. | `1` |

## Embedded Leafnode

Generated embedded config always includes a leafnode listener. In requester role, generated config also requires a remote leafnode target.

| Variable | Default | Applies to | Required when | Description | Example |
|---|---|---|---|---|---|
| `EMBEDDED_NATS_LEAF_LISTEN_HOST` | `0.0.0.0` | both | No | Leafnode listener host in generated config. | `0.0.0.0` |
| `EMBEDDED_NATS_LEAF_LISTEN_PORT` | `7422` | both | No | Leafnode listener port in generated config. | `7422` |
| `EMBEDDED_NATS_LEAF_USER` | generated or reused from existing config | both | If paired password is set | Leaf listener username. If absent, generated config creates or reuses one. | `leaf_user` |
| `EMBEDDED_NATS_LEAF_PASSWORD` | generated or reused from existing config | both | If paired user is set | Leaf listener password. Must be paired with user if set manually. | `secret` |
| `LEAF_REMOTE_HOST` | empty | requester | Requester embedded generated config | Receiver leafnode host for requester remotes. | `receiver.example.com` |
| `LEAF_REMOTE_PORT` | `7422` | requester | No | Receiver leafnode port. | `7422` |
| `LEAF_REMOTE_USER` | empty | requester | Unless using `LEAF_REMOTE_NKEY` | Username for requester to connect to receiver leafnode. | `leaf_user` |
| `LEAF_REMOTE_PASSWORD` | empty | requester | Unless using `LEAF_REMOTE_NKEY` | Password for requester leaf remote. | `secret` |
| `LEAF_REMOTE_NKEY` | empty | requester | Alternative to user/password | NKey auth for requester leaf remote. Cannot be combined with user/password. | `UD...` |
