---
title: Local Dev
description: Local development workflow for the Ruby service.
---

Install dependencies from `src/`:

```bash
cd src
bundle install
```

Run a local NATS server, then start receiver and requester with direct `rackup` commands from [Getting Started](../getting-started/). For local smoke tests, set `PROXY_AUTH_ENABLED=false` on the requester.

Useful local inspection commands:

```bash
curl -sS http://127.0.0.1:7000/observability/nats
curl -sS 'http://127.0.0.1:7000/observability/cases?limit=5'
curl -sS 'http://127.0.0.1:7001/observability/flows?limit=5'
```

The code is Rack/Sinatra based and uses Falcon in the provided runtime command because `CONNECT` tunneling needs Rack hijack support.

