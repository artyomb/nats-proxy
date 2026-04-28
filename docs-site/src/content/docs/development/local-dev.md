---
title: Local Dev
description: Local development workflow for the Ruby service.
---

Install dependencies from `src/`:

```bash
cd src
bundle install
```

Run a local NATS server:

```bash
nats-server
```

The receiver command below uses `UPSTREAM_URL=http://example.com` only as a local smoke-test target. Replace it with the HTTP service you want the receiver side to call.

Start the receiver from another terminal:

```bash
cd src
SERVICE_ROLE=receiver \
SERVICE_ID=receiver-local \
UPSTREAM_URL=http://example.com \
NATS_URL=nats://127.0.0.1:4222 \
NATS_MODE=core \
NATS_RESPONSE_SUBJECT_ROOT=proxy \
PORT=7001 \
bundle exec rackup -o 0.0.0.0 -p 7001 -s falcon
```

Start the requester from another terminal:

```bash
cd src
SERVICE_ROLE=requester \
SERVICE_ID=requester-local \
NATS_URL=nats://127.0.0.1:4222 \
NATS_MODE=core \
NATS_RESPONSE_SUBJECT_ROOT=proxy \
PROXY_AUTH_ENABLED=false \
PORT=7000 \
bundle exec rackup -o 0.0.0.0 -p 7000 -s falcon
```

Verify the local bridge:

```bash
curl -fsS http://127.0.0.1:7000/healthcheck
```

```bash
curl -fsS http://127.0.0.1:7001/healthcheck
```

```bash
curl -i http://127.0.0.1:7000/
```

Useful local inspection commands:

```bash
curl -sS http://127.0.0.1:7000/observability/nats
curl -sS 'http://127.0.0.1:7000/observability/cases?limit=5'
curl -sS 'http://127.0.0.1:7001/observability/flows?limit=5'
```

The code is Rack/Sinatra based and uses Falcon in the provided runtime command because `CONNECT` tunneling needs Rack hijack support.
