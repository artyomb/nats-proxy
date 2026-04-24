---
title: External NATS
description: Running requester and receiver against an existing NATS topology.
---

External NATS mode means the container does not start `nats-server`. Keep `EMBEDDED_NATS_ENABLED=false` and point both roles at an existing `NATS_URL`.

Receiver:

```bash
docker run -d \
  --name nats-proxy-receiver \
  --restart unless-stopped \
  -p 7001:7000 \
  -e SERVICE_ROLE=receiver \
  -e SERVICE_ID=receiver-1 \
  -e UPSTREAM_URL=http://upstream.internal:8080 \
  -e NATS_URL=nats://nats.internal:4222 \
  -e NATS_MODE=core \
  -e PORT=7000 \
  <registry>/<image>:<tag>
```

Requester:

```bash
docker run -d \
  --name nats-proxy-requester \
  --restart unless-stopped \
  -p 7000:7000 \
  -e SERVICE_ROLE=requester \
  -e SERVICE_ID=requester-1 \
  -e NATS_URL=nats://nats.internal:4222 \
  -e NATS_MODE=core \
  -e PROXY_AUTH_ENABLED=false \
  -e PORT=7000 \
  <registry>/<image>:<tag>
```

Verification:

```bash
curl -fsS http://127.0.0.1:7000/healthcheck
curl -sS http://127.0.0.1:7000/observability/nats
curl -i http://127.0.0.1:7000/
```

For JetStream, set `NATS_MODE=jetstream`, ensure `NATS_STREAM` exists with subjects covering request and response roots, and use `/observability/nats` to inspect stream and consumer state.

