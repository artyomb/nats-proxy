---
title: SOCKS5
description: Enabling and operating the requester SOCKS5 listener.
---

SOCKS5 is requester-only and disabled by default.

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

SOCKS5 supports `CONNECT`. Unsupported commands and unsupported address types are rejected at the SOCKS5 wire level. When auth is enabled, the server selects username/password auth and validates credentials through `ProxyAuth`.

