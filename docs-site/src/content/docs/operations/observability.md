---
title: Observability
description: Runtime UI and JSON endpoints for flows, cases, metrics, and NATS state.
---

Observability is local to each process and backed by an in-memory recent event buffer. It is useful for diagnosis, not long-term storage.

The collector keeps events for the most recent request ids only. A quiet process can report `feed_health.state=stale`; that means no recent events were recorded, not necessarily that the service is unhealthy.

| Endpoint | Purpose |
|---|---|
| `GET /observability` | HTML UI. |
| `GET /observability/flows` | Raw event feed. |
| `GET /observability/cases` | Request/session summaries reconstructed from events. |
| `GET /observability/metrics` | Request, response, error, cancel, and flow-control rates for a bounded window. |
| `GET /observability/nats` | NATS connection snapshot and JetStream stream/consumer details when available. |

## Flows

```bash
curl -sS 'http://127.0.0.1:7000/observability/flows?limit=20'
curl -sS 'http://127.0.0.1:7000/observability/flows?request_id=<request_id>&include_nats_payload=true'
```

Supported filters: `request_id`, `subject`, `service_id`, `event_type`, `outcome`, `from`, `to`, `limit`, and `include_nats_payload`.

Read it as a chronological feed of `request_published`, `response_start`, `response_chunk`, `response_error`, `response_end`, `session_chunk`, `session_close`, `flow_credit_sent`, `flow_credit_received`, `flow_credit_wait`, `flow_credit_timeout`, `cancel_published`, and `cancel_observed`.

## Cases

```bash
curl -sS 'http://127.0.0.1:7000/observability/cases?limit=10'
curl -sS 'http://127.0.0.1:7000/observability/cases?outcome=timeout'
```

Supported filters include the flow filters plus `status`. Case statuses include `queued`, `in_progress`, `completed`, `failed`, `timed_out`, and `canceled`.

Use cases to answer whether a request reached the requester, got a response start, completed, timed out, or was canceled. Case summaries also include flow-control counters: `credits_total`, `credit_bytes_total`, `flow_waits_total`, and `flow_timeouts_total`.

## Metrics

```bash
curl -sS 'http://127.0.0.1:7000/observability/metrics?window_sec=60'
```

`window_sec` is clamped to `1..300`. The payload includes `requests_rps`, `responses_rps`, `errors_rps`, `cancels_rps`, `credits_rps`, `flow_waits_rps`, `flow_timeouts_rps`, a `flow_control` aggregate, and reconstruction quality ratios.

## NATS

```bash
curl -sS http://127.0.0.1:7000/observability/nats
```

The payload includes boot state, role, backend, bridge inbound/outbound readiness, NATS connection status, server info, ping counters, and JetStream stream/consumer details when the resolved backend is JetStream.
