---
title: Troubleshooting
description: Symptom-driven checks for common runtime failures.
---

| Symptom | Probable cause | Check | Fix |
|---|---|---|---|
| Requester returns `503 Service Unavailable` | Runtime boot not ready or no outbound bridge and no direct upstream fallback | `curl -sS http://requester:7000/observability/nats` | Check NATS connectivity, `NATS_URL`, `NATS_MODE`, and requester boot logs. |
| Requester does not receive a response | Receiver not subscribed, subject roots mismatch, or stream/consumer issue | Compare `NATS_REQUEST_SUBJECT_ROOT`, `NATS_RESPONSE_SUBJECT_ROOT`, `LISTEN_SUBJECT`; inspect `/observability/cases` on both roles | Align subject roots and ensure receiver is running before sending traffic. |
| Receiver does not read requests | Wrong `SERVICE_ROLE`, Core queue group issue, or JetStream consumer unavailable | Receiver `/observability/nats` should show `role=receiver` and `bridge_inbound=true` | Set `SERVICE_ROLE=receiver`; verify `NATS_QUEUE_GROUP` for Core or stream/consumer state for JetStream. |
| JetStream requests do not move | Stream missing or subjects do not cover roots | `curl -sS http://receiver:7000/observability/nats` and inspect `mode_details` | Create stream with `<request_root>.>` and `<response_root>.>` subjects, or use embedded receiver bootstrap with `NATS_MODE=jetstream`. |
| Proxy requests get generic `404 Not Found` | Proxy auth safety lock or invalid credentials | Check logs for `Proxy auth safety lock enabled`; inspect `PROXY_AUTH_USERS_JSON` | Provide valid bcrypt hashes or set `PROXY_AUTH_ENABLED=false` for a controlled test. |
| `CONNECT` returns `Session establishment timeout` | Receiver cannot establish the TCP target or response event did not arrive before `NATS_RESPONSE_TIMEOUT` | Receiver logs and requester `/observability/cases` | Verify target host/port from receiver network and increase timeout only after connectivity is confirmed. |
| Tunnel closes during idle period | No downstream frame or control event before `STREAM_RESPONSE_TIMEOUT` | Requester logs for `stream_timeout` | Check target-side idle behavior or raise `STREAM_RESPONSE_TIMEOUT`. |
| Upstream unavailable | `UPSTREAM_URL` missing or target refuses/times out | Receiver logs; HTTP response body includes `Upstream unavailable` or missing upstream message | Set `UPSTREAM_URL` on receiver and verify receiver-to-upstream network path. |
| Embedded requester exits on startup | Missing or conflicting leaf remote config | Container logs from entrypoint | Set `LEAF_REMOTE_HOST` plus either `LEAF_REMOTE_USER`/`LEAF_REMOTE_PASSWORD` or `LEAF_REMOTE_NKEY`, not both. |
| Embedded leaf connection fails | Receiver leaf listener not reachable or credentials mismatch | `ss -ltnp` on receiver for `:7422`; requester logs | Start receiver first, expose `7422`, and use receiver leaf credentials on requester. |

