---
title: Self-NATS Leafnodes
description: Two-node deployment with embedded NATS and leafnode connectivity.
---

Self-NATS mode runs embedded `nats-server` inside each service container and connects requester to receiver through NATS leafnodes.

Deployment order matters: start the receiver first, then the requester. The requester generated config requires a reachable `LEAF_REMOTE_HOST`, and the receiver is the side that bootstraps the JetStream stream when `NATS_MODE=jetstream`.

## Receiver

Receiver owns the upstream side and exposes the leaf listener:

```bash
docker run -d \
  --name nats-proxy-receiver \
  --restart unless-stopped \
  -p 7000:7000 \
  -p 7422:7422 \
  -v proxy-receiver-data:/data \
  -e SERVICE_ROLE=receiver \
  -e SERVICE_ID=receiver-1 \
  -e UPSTREAM_URL=http://upstream.internal:8080 \
  -e NATS_URL=nats://127.0.0.1:4222 \
  -e NATS_MODE=jetstream \
  -e NATS_STREAM=proxy \
  -e NATS_CONSUMER_NAME=nats-proxy \
  -e EMBEDDED_NATS_ENABLED=true \
  -e EMBEDDED_NATS_GENERATE_CONFIG=true \
  -e EMBEDDED_NATS_JETSTREAM_ENABLED=true \
  -e EMBEDDED_NATS_LEAF_LISTEN_HOST=0.0.0.0 \
  -e EMBEDDED_NATS_LEAF_LISTEN_PORT=7422 \
  -e EMBEDDED_NATS_LEAF_USER=leaf_user \
  -e EMBEDDED_NATS_LEAF_PASSWORD=leaf_password \
  -e PORT=7000 \
  <registry>/<image>:<tag>
```

Verify:

```bash
curl -fsS http://127.0.0.1:7000/healthcheck
curl -sS http://127.0.0.1:7000/observability/nats
```

## Requester

Requester connects its local embedded NATS leafnode to the receiver leaf listener:

```bash
docker run -d \
  --name nats-proxy-requester \
  --restart unless-stopped \
  -p 17000:7000 \
  -p 37138:1080 \
  -v proxy-requester-data:/data \
  -e SERVICE_ROLE=requester \
  -e SERVICE_ID=requester-1 \
  -e NATS_URL=nats://127.0.0.1:4222 \
  -e NATS_MODE=jetstream \
  -e NATS_STREAM=proxy \
  -e NATS_CONSUMER_NAME=nats-proxy \
  -e PROXY_AUTH_ENABLED=false \
  -e SOCKS5_ENABLED=true \
  -e SOCKS5_LISTEN_HOST=0.0.0.0 \
  -e SOCKS5_LISTEN_PORT=1080 \
  -e EMBEDDED_NATS_ENABLED=true \
  -e EMBEDDED_NATS_GENERATE_CONFIG=true \
  -e EMBEDDED_NATS_JETSTREAM_ENABLED=false \
  -e LEAF_REMOTE_HOST=<receiver_public_host_or_ip> \
  -e LEAF_REMOTE_PORT=7422 \
  -e LEAF_REMOTE_USER=leaf_user \
  -e LEAF_REMOTE_PASSWORD=leaf_password \
  -e PORT=7000 \
  <registry>/<image>:<tag>
```

Verify:

```bash
curl -fsS http://127.0.0.1:17000/healthcheck
curl -sS http://127.0.0.1:17000/observability/nats
curl -i http://127.0.0.1:17000/
curl -sS 'http://127.0.0.1:17000/observability/cases?limit=5'
```

## Compose Files

The same topology can be expressed as Compose files by mapping the same environment variables from the `docker run` examples into `services.<name>.environment`, publishing `7000` for the HTTP service and `7422` on the receiver for leafnode access, and mounting `/data` as a persistent volume.

The requester supports either `LEAF_REMOTE_USER` plus `LEAF_REMOTE_PASSWORD`, or `LEAF_REMOTE_NKEY`; the entrypoint fails fast if both modes are configured together.

`NATS_JS_API_PREFIX` resolution in embedded mode is:

1. explicit `NATS_JS_API_PREFIX`;
2. `$JS.<EMBEDDED_NATS_JS_DOMAIN>.API`;
3. `$JS.API`.
