---
title: External NATS
description: Running requester and receiver against an existing NATS topology.
---

External NATS mode means the container does not start `nats-server`. Keep `EMBEDDED_NATS_ENABLED=false` and point both roles at an existing `NATS_URL`.

Use this topology when NATS is managed outside `nats-proxy`: a shared NATS cluster, a platform-provided NATS service, or a standalone NATS container maintained separately from requester and receiver.

Before starting the service containers, decide:

- which NATS URL both roles can reach;
- which upstream URL the receiver can reach;
- which host ports should expose requester and receiver health endpoints;
- whether requester proxy ingress should require [Proxy Auth](../configuration/proxy-auth/).

## Docker Compose

For Compose, keep the deployment file next to the node where it runs. The examples below configure the requester and receiver against an external Core NATS endpoint; embedded leafnode Compose variants are covered by [Self-NATS Leafnodes](self-nats-leafnodes/).

Receiver:

```yaml
services:
  nats-proxy-receiver:
    image: <registry>/<image>:<tag>
    container_name: nats-proxy-receiver
    restart: unless-stopped
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      SERVICE_ROLE: receiver
      SERVICE_ID: receiver-compose
      UPSTREAM_URL: http://host.docker.internal:8080
      NATS_URL: nats://host.docker.internal:4222
      NATS_MODE: core
      EMBEDDED_NATS_ENABLED: "false"
      PORT: 7000
    ports:
      - "17001:7000"
```

Requester:

```yaml
services:
  nats-proxy-requester:
    image: <registry>/<image>:<tag>
    container_name: nats-proxy-requester
    restart: unless-stopped
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      SERVICE_ROLE: requester
      SERVICE_ID: requester-compose
      NATS_URL: nats://host.docker.internal:4222
      NATS_MODE: core
      EMBEDDED_NATS_ENABLED: "false"
      PROXY_AUTH_ENABLED: "false"
      PORT: 7000
    ports:
      - "17000:7000"
```

Start each file with `docker compose up -d`, then verify:

```bash
curl -fsS http://127.0.0.1:17000/healthcheck
curl -fsS http://127.0.0.1:17001/healthcheck
curl -sS http://127.0.0.1:17000/observability/nats
curl -i http://127.0.0.1:17000/
```

Expected result: both containers connect to the same external NATS server, requester publishes bridge requests, receiver forwards them to `UPSTREAM_URL`, and `/observability/cases` starts showing bridged requests after traffic reaches the requester.

## Multi-Instance Notes

You can run multiple requesters and receivers against the same external NATS server. Each live process needs a unique `SERVICE_ID`.

Additional requester instances expose additional ingress points; clients or the platform decide which requester receives a client connection. Additional receiver instances can form a worker pool for new bridge requests and tunnel session opens.

For Core NATS, receiver replicas that should share new work must use the same `LISTEN_SUBJECT` and `NATS_QUEUE_GROUP`. For JetStream, they must share the same request stream and base `NATS_CONSUMER_NAME`. NATS distributes only the beginning of each flow; after a receiver is selected, continuation messages go back to the original requester and the owner receiver.

## Docker Run

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
  -e EMBEDDED_NATS_ENABLED=false \
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
  -e EMBEDDED_NATS_ENABLED=false \
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
