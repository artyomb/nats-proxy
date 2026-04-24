---
title: Proxy Auth
description: Proxy authentication, bcrypt users, and safety lock behavior.
---

Proxy authentication is enabled by default with `PROXY_AUTH_ENABLED=true`. Set `PROXY_AUTH_ENABLED=false` to disable it.

Auth applies only to proxy-specific ingress:

- HTTP proxy absolute-form requests.
- Requests detected as proxy requests from proxy headers.
- HTTP `CONNECT`.
- SOCKS5 when `SOCKS5_ENABLED=true`.

Local `/health`, `/healthcheck`, and `/observability*` routes pass through without proxy auth.

## Users JSON

`PROXY_AUTH_USERS_JSON` must be a non-empty JSON object where keys are usernames and values are bcrypt hashes:

```json
{"alice":"$2a$12$..."}
```

HTTP proxy and `CONNECT` requests use `Proxy-Authorization: Basic ...`. SOCKS5 uses username/password auth method `0x02` when proxy auth is enabled.

## Safety Lock

If proxy auth is enabled and `PROXY_AUTH_USERS_JSON` is missing, invalid JSON, empty, or contains invalid bcrypt hashes, `ProxyAuth` enters a safety lock. Proxy-specific traffic is denied with a generic:

```text
HTTP/1.1 404 Not Found
Not Found
```

The generic response is intentional. Runtime errors during credential verification also switch auth into the blocked state.

