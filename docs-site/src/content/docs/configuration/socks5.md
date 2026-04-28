---
title: SOCKS5
description: Enabling and operating the requester SOCKS5 listener.
---

SOCKS5 is a separate requester-side TCP listener. It is disabled by default and is only started in requester role when `SOCKS5_ENABLED=true`.

SOCKS5 uses the same NATS-backed TCP tunnel path as HTTP `CONNECT`: the requester accepts a SOCKS5 `CONNECT`, opens a `tcp_stream` bridge session, and the receiver connects to the requested host and port.

| Variable | Default | Description |
|---|---|---|
| `SOCKS5_ENABLED` | `false` | Starts the SOCKS5 listener when `true`. |
| `SOCKS5_LISTEN_HOST` | `0.0.0.0` | Bind host. |
| `SOCKS5_LISTEN_PORT` | `1080` | Bind port. |

Example:

```bash
SERVICE_ROLE=requester \
SOCKS5_ENABLED=true \
SOCKS5_LISTEN_HOST=0.0.0.0 \
SOCKS5_LISTEN_PORT=1080 \
PROXY_AUTH_ENABLED=false \
bundle exec rackup -o 0.0.0.0 -p 7000 -s falcon
```

SOCKS5 currently supports only the `CONNECT` command. Unsupported commands and unsupported address types are rejected at the SOCKS5 wire level.

When proxy auth is enabled, the server selects username/password auth and validates credentials against the configured proxy users. When proxy auth is disabled, it selects no-auth.
