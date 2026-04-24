---
title: Docker
description: Runtime image details and compose build entry point.
---

The deploy image is built from `docker/ruby/Dockerfile` with build context `src` and additional Docker context `docker`.

```bash
docker compose -f docker/docker-compose.yml build nats_proxy
```

The image includes:

- Ruby application dependencies;
- `nats-server`;
- `nats` CLI;
- `/usr/local/bin/entrypoint.sh`;
- default command `bundle exec rackup -o 0.0.0.0 -p $PORT -s falcon`;
- healthcheck `curl --fail http://127.0.0.1:$PORT/healthcheck`.

Default exposed behavior is controlled by `PORT=7000`. NATS ports are not exposed by the Dockerfile; publish `4222` or `7422` only when your deployment needs host access to embedded NATS client or leafnode ports.

