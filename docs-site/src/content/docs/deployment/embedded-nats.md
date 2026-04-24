---
title: Embedded NATS
description: Embedded nats-server behavior in the runtime image.
---

The deploy image contains `nats-server` and the `nats` CLI. The entrypoint starts embedded NATS only when `EMBEDDED_NATS_ENABLED=true`.

Embedded startup requires one of:

- `EMBEDDED_NATS_CONFIG=/path/to/nats.conf`
- `EMBEDDED_NATS_GENERATE_CONFIG=true`

Generated config includes:

- optional JetStream store configuration;
- a leafnode listener on `EMBEDDED_NATS_LEAF_LISTEN_HOST:EMBEDDED_NATS_LEAF_LISTEN_PORT`;
- leafnode authorization credentials;
- requester-side remote leafnode connection when `SERVICE_ROLE=requester`.

The generated config does not enable a NATS monitoring endpoint. Do not publish `8222` unless you provide a custom config that enables monitoring.

In embedded JetStream mode, the entrypoint bootstraps the stream only for `SERVICE_ROLE=receiver` and `NATS_MODE=jetstream`.

