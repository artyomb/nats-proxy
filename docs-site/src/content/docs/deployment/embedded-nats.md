---
title: Embedded NATS
description: Embedded nats-server behavior in the runtime image.
---

The deploy image contains `nats-server` and the `nats` CLI. The entrypoint starts embedded NATS only when `EMBEDDED_NATS_ENABLED=true`.

Use embedded NATS when each `nats-proxy` container should own its local NATS process instead of connecting to a separately managed NATS service. This is the base runtime behavior used by [Self-NATS Leafnodes](self-nats-leafnodes/).

Embedded startup requires one of:

- `EMBEDDED_NATS_CONFIG=/path/to/nats.conf`
- `EMBEDDED_NATS_GENERATE_CONFIG=true`

Generated config requires an explicit `SERVICE_ROLE`. In requester role it also requires `LEAF_REMOTE_HOST` and either `LEAF_REMOTE_USER` plus `LEAF_REMOTE_PASSWORD`, or `LEAF_REMOTE_NKEY`.

Generated config includes:

- optional JetStream store configuration;
- a leafnode listener on `EMBEDDED_NATS_LEAF_LISTEN_HOST:EMBEDDED_NATS_LEAF_LISTEN_PORT`;
- leafnode authorization credentials;
- requester-side remote leafnode connection when `SERVICE_ROLE=requester`.

The generated config does not enable a NATS monitoring endpoint. Do not publish `8222` unless you provide a custom config that enables monitoring.

In embedded JetStream mode, the entrypoint bootstraps the stream only for `SERVICE_ROLE=receiver` and `NATS_MODE=jetstream`.

## Generated Config

For generated config, mount `/data` as persistent storage if JetStream is enabled or if generated leafnode credentials must survive container recreation:

```bash
docker run -d \
  --name nats-proxy-receiver \
  --restart unless-stopped \
  -p 7000:7000 \
  -p 7422:7422 \
  -v nats-proxy-receiver-data:/data \
  -e SERVICE_ROLE=receiver \
  -e SERVICE_ID=receiver-1 \
  -e UPSTREAM_URL=http://upstream.internal:8080 \
  -e NATS_URL=nats://127.0.0.1:4222 \
  -e NATS_MODE=jetstream \
  -e NATS_STREAM=proxy \
  -e EMBEDDED_NATS_ENABLED=true \
  -e EMBEDDED_NATS_GENERATE_CONFIG=true \
  -e EMBEDDED_NATS_JETSTREAM_ENABLED=true \
  -e EMBEDDED_NATS_LEAF_LISTEN_HOST=0.0.0.0 \
  -e EMBEDDED_NATS_LEAF_LISTEN_PORT=7422 \
  -e PORT=7000 \
  <registry>/<image>:<tag>
```

## Static Config

For a hand-written NATS config, mount the file and point `EMBEDDED_NATS_CONFIG` at the container path:

```bash
docker run -d \
  --name nats-proxy-receiver \
  --restart unless-stopped \
  -p 7000:7000 \
  -p 7422:7422 \
  -v ./nats.conf:/etc/nats/nats.conf:ro \
  -v nats-proxy-receiver-data:/data \
  -e SERVICE_ROLE=receiver \
  -e SERVICE_ID=receiver-1 \
  -e UPSTREAM_URL=http://upstream.internal:8080 \
  -e NATS_URL=nats://127.0.0.1:4222 \
  -e NATS_MODE=jetstream \
  -e NATS_STREAM=proxy \
  -e EMBEDDED_NATS_ENABLED=true \
  -e EMBEDDED_NATS_CONFIG=/etc/nats/nats.conf \
  -e PORT=7000 \
  <registry>/<image>:<tag>
```

The app process still uses `NATS_URL` to connect to NATS. In embedded mode this is usually `nats://127.0.0.1:4222` because `nats-server` runs in the same container as the Ruby service.

When `NATS_MODE=jetstream` is used with a static config, the mounted `nats.conf` must enable JetStream and provide a client listener reachable by `NATS_URL`. The entrypoint can bootstrap the stream on the receiver, but it does not rewrite a static NATS config.
