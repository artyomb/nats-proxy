---
title: Proxy Auth
description: Proxy authentication, bcrypt users, and safety lock behavior.
---

Proxy auth protects requester-side proxy ingress. It is enabled by default. Set `PROXY_AUTH_ENABLED=false` only for trusted local development or a topology where another layer already controls access.

Auth applies to:

- HTTP proxy absolute-form requests;
- HTTP requests classified as proxy traffic from proxy headers;
- HTTP `CONNECT`;
- SOCKS5 when `SOCKS5_ENABLED=true`.

Auth does not apply to local service endpoints such as `/health`, `/healthcheck`, and `/observability*`.

## Users JSON

When proxy auth is enabled, `PROXY_AUTH_USERS_JSON` must be a non-empty JSON object where keys are usernames and values are bcrypt hashes:

```json
{"alice":"$2a$12$..."}
```

HTTP proxy and `CONNECT` requests use `Proxy-Authorization: Basic ...`.

SOCKS5 uses username/password authentication method `0x02` when proxy auth is enabled. When proxy auth is disabled, SOCKS5 selects no-auth method `0x00`.

## Safety Lock

If proxy auth is enabled and `PROXY_AUTH_USERS_JSON` is missing, invalid JSON, empty, or contains invalid bcrypt hashes, proxy-specific traffic is denied with a generic response:

```text
HTTP/1.1 404 Not Found
Not Found
```

The generic response is intentional. Runtime errors during credential verification also switch auth into the blocked state.
