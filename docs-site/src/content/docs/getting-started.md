---
title: Getting Started
description: Start nats-proxy locally, with Compose, or with docker run.
---

This page shows three runnable startup paths. The examples disable proxy auth on the requester for smoke testing; production proxy ingress should use [Proxy Auth](configuration/proxy-auth/).

## Direct Rackup

Prerequisites:

- Ruby dependencies installed from `src/` with `bundle install`.
- A NATS server reachable at `nats://127.0.0.1:4222`.
- An upstream HTTP service reachable by the receiver.

Start the receiver and requester in separate terminals:

```bash
cd src
SERVICE_ROLE=receiver \
SERVICE_ID=receiver-local \
UPSTREAM_URL=http://127.0.0.1:8080 \
NATS_URL=nats://127.0.0.1:4222 \
NATS_MODE=core \
PORT=7001 \
bundle exec rackup -o 0.0.0.0 -p 7001 -s falcon
```

```bash
cd src
SERVICE_ROLE=requester \
SERVICE_ID=requester-local \
NATS_URL=nats://127.0.0.1:4222 \
NATS_MODE=core \
PROXY_AUTH_ENABLED=false \
PORT=7000 \
bundle exec rackup -o 0.0.0.0 -p 7000 -s falcon
```

Verify:

```bash
curl -fsS http://127.0.0.1:7000/healthcheck
curl -fsS http://127.0.0.1:7001/healthcheck
curl -sS http://127.0.0.1:7000/observability/nats
curl -i http://127.0.0.1:7000/
```

Expected result: both healthchecks return success, `/observability/nats` shows requester runtime state, and the request to `:7000` is forwarded through NATS to the receiver and then to `UPSTREAM_URL`.

## Docker Compose

For Compose, keep the deployment file next to the node where it runs. The example below uses external Core NATS because it is the smallest Compose setup; embedded leafnode Compose variants are covered by [Self-NATS Leafnodes](deployment/self-nats-leafnodes/).

Receiver:

```yaml
services:
  proxy-gateway-receiver:
    image: <registry>/<image>:<tag>
    container_name: proxy-gateway-receiver
    restart: unless-stopped
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      SERVICE_ROLE: receiver
      SERVICE_ID: receiver-compose
      UPSTREAM_URL: http://host.docker.internal:8080
      NATS_URL: nats://host.docker.internal:4222
      NATS_MODE: core
      PORT: 7000
    ports:
      - "17001:7000"
```

Requester:

```yaml
services:
  proxy-gateway-requester:
    image: <registry>/<image>:<tag>
    container_name: proxy-gateway-requester
    restart: unless-stopped
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      SERVICE_ROLE: requester
      SERVICE_ID: requester-compose
      NATS_URL: nats://host.docker.internal:4222
      NATS_MODE: core
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

## Inline docker run

External NATS mode is the smallest `docker run` variant. It assumes both containers can reach the same external NATS URL and the receiver can reach the upstream.

```bash
docker run -d \
  --name proxy-gateway-receiver \
  --restart unless-stopped \
  --add-host host.docker.internal:host-gateway \
  -p 17001:7000 \
  -e SERVICE_ROLE=receiver \
  -e SERVICE_ID=receiver-docker \
  -e UPSTREAM_URL=http://host.docker.internal:8080 \
  -e NATS_URL=nats://host.docker.internal:4222 \
  -e NATS_MODE=core \
  -e PORT=7000 \
  <registry>/<image>:<tag>
```

```bash
docker run -d \
  --name proxy-gateway-requester \
  --restart unless-stopped \
  --add-host host.docker.internal:host-gateway \
  -p 17000:7000 \
  -p 37138:1080 \
  -e SERVICE_ROLE=requester \
  -e SERVICE_ID=requester-docker \
  -e NATS_URL=nats://host.docker.internal:4222 \
  -e NATS_MODE=core \
  -e PROXY_AUTH_ENABLED=false \
  -e SOCKS5_ENABLED=true \
  -e SOCKS5_LISTEN_HOST=0.0.0.0 \
  -e SOCKS5_LISTEN_PORT=1080 \
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

Expected result: the requester listens on host port `17000` for HTTP and `37138` for SOCKS5. No NATS monitoring port is published here because the generated embedded config does not enable a monitoring listener.
