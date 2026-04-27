---
title: Getting Started
description: Start nats-proxy with Docker and verify a local requester-to-receiver bridge.
---

This page starts a local working topology with one NATS server, one receiver, and one requester. The requester accepts HTTP on `127.0.0.1:7000`, sends the request through NATS, and the receiver forwards it to `http://example.com`.

The quick-start commands disable proxy auth on the requester so the local bridge can be exercised with a plain `curl`; production proxy ingress should use [Proxy Auth](configuration/proxy-auth/).

## Build Image

Build the runtime image from the repository root:

```bash
REGISTRY_HOST=nats-proxy-local docker-compose -f docker/docker-compose.yml build nats_proxy
```

The `REGISTRY_HOST` value is only a local image namespace for this build. It makes the image name explicit without changing the compose file used by CI/CD.

## Start Topology

Create a Docker network so the service containers can resolve the NATS container by name:

```bash
docker network create nats-proxy
```

Start NATS:

```bash
docker run -d --name nats-proxy-nats --network nats-proxy nats:2.11-alpine
```

Start the receiver:

```bash
docker run -d \
  --name nats-proxy-receiver \
  --network nats-proxy \
  -p 7001:7000 \
  -e SERVICE_ROLE=receiver \
  -e SERVICE_ID=receiver-local \
  -e UPSTREAM_URL=http://example.com \
  -e NATS_URL=nats://nats-proxy-nats:4222 \
  -e NATS_MODE=core \
  -e NATS_RESPONSE_SUBJECT_ROOT=proxy \
  -e PROXY_AUTH_ENABLED=false \
  -e PORT=7000 \
  nats-proxy-local/nats-proxy
```

Start the requester:

```bash
docker run -d \
  --name nats-proxy-requester \
  --network nats-proxy \
  -p 7000:7000 \
  -e SERVICE_ROLE=requester \
  -e SERVICE_ID=requester-local \
  -e NATS_URL=nats://nats-proxy-nats:4222 \
  -e NATS_MODE=core \
  -e NATS_RESPONSE_SUBJECT_ROOT=proxy \
  -e PROXY_AUTH_ENABLED=false \
  -e PORT=7000 \
  nats-proxy-local/nats-proxy
```

## Verify

Check both replicas:

```bash
curl -fsS http://127.0.0.1:7000/healthcheck
```

```bash
curl -fsS http://127.0.0.1:7001/healthcheck
```

Inspect NATS runtime state:

```bash
curl -sS http://127.0.0.1:7000/observability/nats
```

Send a request through the requester:

```bash
curl -i http://127.0.0.1:7000/
```

Expected result: both healthchecks return success, `/observability/nats` shows the requester connected to NATS, and the request to `:7000` returns the upstream HTTP response through the receiver.

## Cleanup

Remove the quick-start containers:

```bash
docker rm -f nats-proxy-requester nats-proxy-receiver nats-proxy-nats
```

Remove the Docker network:

```bash
docker network rm nats-proxy
```

For complete deployment scenarios, see [External NATS](deployment/external-nats/), [Embedded NATS](deployment/embedded-nats/), and [Self-NATS Leafnodes](deployment/self-nats-leafnodes/). For source-based development, see [Local Dev](development/local-dev/).
