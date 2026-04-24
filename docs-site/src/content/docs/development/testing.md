---
title: Testing
description: Test suites and behavior coverage.
---

Run tests from `src/`:

```bash
cd src
bundle exec rspec
bundle exec rake spec:unit
bundle exec rake spec:system
bundle exec rake spec:contracts
```

System specs cover HTTP bridge behavior, streaming/cancel behavior, `CONNECT`, SOCKS5, Core NATS, and JetStream. Unit specs cover bridge protocol parsing, response rendering, proxy auth, NATS runtime, service runtime composition, observability reconstruction, and TCP tunnel behavior.

Some system tests start local NATS helpers from `src/spec/support`.

