# Self NATS Deployment (EN)

This guide describes HTTP-to-NATS proxy gateway deployment in self NATS mode (embedded `nats-server` inside the service container) on two servers:
- `receiver` node: accepts leaf connection, owns JetStream stream, proxies requests to `UPSTREAM_URL`.
- `requester` node: accepts HTTP/CONNECT/SOCKS5 traffic, publishes requests to local NATS, and uses leaf link to the receiver side.

## 1. Self NATS functional model

Current service behavior:
1. Runtime image now includes `nats-server` and `nats` CLI.
2. Container starts via `docker/ruby/entrypoint.sh`, which:
- starts embedded NATS when `EMBEDDED_NATS_ENABLED=true`;
- supports generated NATS config (`EMBEDDED_NATS_GENERATE_CONFIG=true`);
- validates requester leaf-remote requirements (fail-fast);
- bootstraps stream for `receiver` when `NATS_MODE=jetstream` and stream is missing;
- resolves `NATS_JS_API_PREFIX` automatically (`NATS_JS_API_PREFIX` -> `EMBEDDED_NATS_JS_DOMAIN` -> `$JS.API`).
3. NATS readiness check for bootstrap uses `nats rtt`.
4. `EMBEDDED_NATS_JS_DOMAIN` is supported for JetStream domain configuration.

## 2. Deployment order (critical)

Deploy `receiver` first, then `requester`.

Why:
1. `requester` requires reachable `LEAF_REMOTE_HOST:LEAF_REMOTE_PORT`.
2. In JetStream mode, receiver initializes stream state first.
3. This reduces startup retries and avoids false leaf connection failures.

## 3. Requirements for both servers

For both nodes:
1. Linux + Docker Engine + Docker Compose plugin.
2. Access to image (`registry`) or image archive (`.tar/.tar.gz`).
3. Open ports:
- `7000/tcp` for service HTTP API;
- `7422/tcp` on receiver for leaf listener;
- `4222/tcp` locally for embedded NATS.

Network reachability:
1. `requester -> receiver:7422/tcp` is required.
2. `receiver -> upstream` is required.

## 4. Host preparation and image installation

On each server:

```bash
mkdir -p /opt/self-nats-proxy
cd /opt/self-nats-proxy
```

Option A (from registry):

```bash
docker login <registry_host>
docker pull <registry>/<image>:<tag>
```

Option B (offline archive):

```bash
# copy image archive into current directory first
cat <image_archive>.tar | docker load
# or
cat <image_archive>.tar.gz | gunzip | docker load
```

Verify image is available:

```bash
docker image ls | rg '<image>|<tag>'
```

Copy compose templates from this repository:
- `src/docs/self-nats/examples/docker-compose.receiver.yaml`
- `src/docs/self-nats/examples/docker-compose.requester.yaml`

## 5. Receiver initialization (step 1)

1. On receiver server:

```bash
cp <path>/docker-compose.receiver.yaml ./docker-compose.yaml
```

2. Fill runtime values in `docker-compose.yaml`:
- `<registry>/<image>:<tag>`
- `<receiver_service_id>`
- `<upstream_base_url>`
- `<stream_name>`
- `<receiver_consumer_name>`
- `<request_subject_root>`
- `<response_subject_root>`
- `<leaf_listener_user>`
- `<leaf_listener_password>`

3. Start service:

```bash
docker compose up -d
```

4. Validate:

```bash
docker compose ps
docker compose logs --tail=200
curl -fsS http://127.0.0.1:7000/healthcheck
```

5. Verify leaf listener is exposed:

```bash
ss -ltnp | rg ':7422'
```

## 6. Requester initialization (step 2)

1. On requester server:

```bash
cp <path>/docker-compose.requester.yaml ./docker-compose.yaml
```

2. Fill runtime values:
- `<registry>/<image>:<tag>`
- `<requester_service_id>`
- `<stream_name>`
- `<requester_consumer_name>`
- `<request_subject_root>`
- `<response_subject_root>`
- `<receiver_public_host_or_ip>`
- proper leaf remote auth (`LEAF_REMOTE_*`)

3. Validate leaf auth mode:
- Either `LEAF_REMOTE_USER` + `LEAF_REMOTE_PASSWORD`.
- Or `LEAF_REMOTE_NKEY`.
- Do not use both modes at the same time.

4. Start service:

```bash
docker compose up -d
```

5. Validate:

```bash
docker compose ps
docker compose logs --tail=200
curl -fsS http://127.0.0.1:7000/healthcheck
```

## 7. Basic smoke pipeline

1. Send test HTTP request to requester (`:7000`).
2. Confirm request processing appears in receiver logs.
3. Check `/observability/cases` on both nodes.

Example checks:

```bash
# requester
curl -sS http://127.0.0.1:7000/observability/nats
curl -sS 'http://127.0.0.1:7000/observability/cases?limit=5'

# receiver
curl -sS http://127.0.0.1:7000/observability/nats
curl -sS 'http://127.0.0.1:7000/observability/cases?limit=5'
```

## 8. Full configuration variants

### 8.1 Embedded NATS mode

1. `EMBEDDED_NATS_ENABLED=false`
- No embedded NATS startup.
- Service must connect to external `NATS_URL`.

2. `EMBEDDED_NATS_ENABLED=true` + `EMBEDDED_NATS_CONFIG=<path>`
- Static NATS config file is used.

3. `EMBEDDED_NATS_ENABLED=true` + `EMBEDDED_NATS_GENERATE_CONFIG=true`
- NATS config is generated to `EMBEDDED_NATS_GENERATED_CONFIG_PATH`.

### 8.2 Embedded JetStream enablement

1. `EMBEDDED_NATS_JETSTREAM_ENABLED=true` forces JetStream on.
2. `EMBEDDED_NATS_JETSTREAM_ENABLED=false` forces JetStream off.
3. Empty value (`""`) uses role default:
- `receiver` -> `true`
- `requester` -> `false`

### 8.3 Service backend modes

1. `NATS_MODE=core`
- Core NATS only.
- No stream bootstrap.

2. `NATS_MODE=jetstream`
- JetStream required.
- Receiver in embedded mode bootstraps stream if needed.

3. `NATS_MODE=auto`
- Resolve to `jetstream` when stream exists, otherwise `core`.

### 8.4 Requester -> receiver leaf auth variants

1. User/password mode:
- `LEAF_REMOTE_USER`
- `LEAF_REMOTE_PASSWORD`

2. NKey mode:
- `LEAF_REMOTE_NKEY`
- `LEAF_REMOTE_USER/PASSWORD` must stay empty.

3. Invalid setup:
- setting `LEAF_REMOTE_NKEY` and `LEAF_REMOTE_USER/PASSWORD` together causes fail-fast.

### 8.5 Local leaf listener credentials

1. Explicit values:
- `EMBEDDED_NATS_LEAF_USER`
- `EMBEDDED_NATS_LEAF_PASSWORD`

2. Auto-generated values:
- entrypoint generates credentials and prints them in logs;
- on next start it reuses existing credentials from generated config.

### 8.6 JetStream API prefix resolution

`NATS_JS_API_PREFIX` resolution order:
1. If `NATS_JS_API_PREFIX` is set -> use it.
2. Else if `EMBEDDED_NATS_JS_DOMAIN` is set -> `$JS.<domain>.API`.
3. Else -> `$JS.API`.

### 8.7 Timeouts and throughput

1. `NATS_RESPONSE_TIMEOUT`
- timeout waiting for first `response_start`.

2. `STREAM_RESPONSE_TIMEOUT`
- idle timeout between events after `response_start`.

3. `RECEIVER_MAX_INFLIGHT`
- receiver request processing concurrency.

### 8.8 Extra requester ingress modes

1. HTTP only (`SOCKS5_ENABLED=false`).
2. HTTP + SOCKS5 (`SOCKS5_ENABLED=true`).
3. Proxy auth on/off via `PROXY_AUTH_ENABLED`.

## 9. Upgrade and rollback

Perform upgrades in this order:
1. `receiver`
2. `requester`

Command template:

```bash
docker compose pull
docker compose up -d
```

Rollback:
1. Switch `<tag>` to previous version.
2. Restart `receiver`, then `requester`.

## 10. Example compose files

- Receiver: `src/docs/self-nats/examples/docker-compose.receiver.yaml`
- Requester: `src/docs/self-nats/examples/docker-compose.requester.yaml`
