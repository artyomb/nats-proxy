---
title: Healthcheck
description: Health endpoint and Docker healthcheck behavior.
---

The Rack service exposes `/healthcheck` through `StackServiceBase.rack_setup`. The Dockerfile uses it as the container healthcheck:

```text
curl --fail http://127.0.0.1:$PORT/healthcheck
```

Check both roles:

```bash
curl -fsS http://127.0.0.1:7000/healthcheck
curl -fsS http://127.0.0.1:7001/healthcheck
```

Healthcheck success means the Rack process is serving. To verify bridge readiness, also inspect `/observability/nats` and check `state`, `bridge_inbound`, and `bridge_outbound`.

