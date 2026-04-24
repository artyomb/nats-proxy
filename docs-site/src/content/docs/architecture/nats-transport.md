---
title: NATS Transport
description: Core NATS, JetStream, subjects, and consumer behavior.
---

`NatsAsyncRuntime` resolves the backend at startup from `NATS_MODE`. The resolved backend stays fixed for the process lifetime and is exposed through `/observability/nats`.

| `NATS_MODE` | Behavior |
|---|---|
| `core` | Uses Core NATS publish/subscribe. Receiver subscribes to `LISTEN_SUBJECT` with queue group `NATS_QUEUE_GROUP`. |
| `jetstream` | Publishes through JetStream and consumes through pull consumers on `NATS_STREAM`. |
| `auto` | Asks the NATS client to resolve the backend for `NATS_STREAM`; use `/observability/nats` to see the actual result. |

## Subjects

| Purpose | Pattern |
|---|---|
| Request envelope | `<request_root>.requests.<service_id>.<request_id>` |
| Response event | `<response_root>.responses.<service_id>.<request_id>` |
| Upstream TCP frames | `<request_root>.sessions.upstream.<service_id>.<session_id>` |
| Downstream TCP frames | `<response_root>.sessions.downstream.<target_service_id>.<session_id>` |

The application defaults are `NATS_REQUEST_SUBJECT_ROOT=to.proxy`, `NATS_RESPONSE_SUBJECT_ROOT=from.proxy`, and `LISTEN_SUBJECT=to.proxy.requests.>`.

## JetStream Consumers

Receiver request processing uses durable consumer `NATS_CONSUMER_NAME` with explicit acknowledgements. The configured ack wait is derived from the larger of `NATS_RESPONSE_TIMEOUT` and `STREAM_RESPONSE_TIMEOUT`, plus 30 seconds. Processing sends `in_progress` heartbeats while long jobs run.

Requester response and session consumers are service-specific:

| Listener | Consumer name |
|---|---|
| Response events | `<NATS_CONSUMER_NAME>-responses-<SERVICE_ID>` |
| Receiver upstream session frames | `<NATS_CONSUMER_NAME>-sessions-upstream-<SERVICE_ID>` |
| Requester downstream session frames | `<NATS_CONSUMER_NAME>-sessions-downstream-<SERVICE_ID>` |

The entrypoint bootstraps a stream only when `SERVICE_ROLE=receiver` and `NATS_MODE=jetstream` in embedded NATS mode. Subjects are `<request_root>.>` and `<response_root>.>` unless both roots are the same, in which case one `<root>.>` subject is used.

