---
title: Healthcheck
description: Health endpoint and Docker healthcheck behavior.
---

Use healthcheck to confirm that the local HTTP process is answering. It is a process-level check, not a full bridge readiness check.

The Dockerfile uses `/healthcheck` as the container healthcheck:

```text
curl --fail http://127.0.0.1:$PORT/healthcheck
```

Check both roles:

```bash
curl -fsS http://127.0.0.1:7000/healthcheck
curl -fsS http://127.0.0.1:7001/healthcheck
```

Healthcheck success means the Rack process is serving. It does not prove that NATS listeners are ready or that the other bridge side is reachable. To verify bridge readiness, also inspect `/observability/nats` and check `state`, `bridge_inbound`, and `bridge_outbound`.
