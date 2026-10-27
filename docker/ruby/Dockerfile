FROM ruby:4.0.9-slim-bookworm AS base

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends build-essential pkg-config libpq-dev curl cmake libssl-dev libyaml-dev\
     && rm -rf /var/lib/apt/lists/*

RUN echo 'gem: --no-document' >> ~/.gemrc

WORKDIR /app

COPY .ruby-version Gemfile Gemfile.lock /app/

RUN bundle install --jobs $(nproc) --retry=3 && \
    bundle clean --force && rm -rf /usr/local/bundle/cache/*

# The bundle replaces the vulnerable default JSON gem (CVE-2026-33210).
RUN rm -rf /usr/local/lib/ruby/4.0.0/json* /usr/local/lib/ruby/4.0.0/*-linux/json \
    /usr/local/lib/ruby/gems/4.0.0/gems/json-* \
    /usr/local/lib/ruby/gems/4.0.0/specifications/default/json-*.gemspec

COPY . /app

ENV RACK_ENV=production NODE_ENV=production
RUN rake -T | grep 'rake build' && rake build || echo 'no build task'

FROM base AS tests
RUN bundle exec rspec

FROM ruby:4.0.9-slim-bookworm AS deploy
ARG NATS_SERVER_VERSION=2.14.9
ARG NATS_CLI_VERSION=0.5.1
ARG TARGETARCH=amd64

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends bash curl libssl-dev wget libpq-dev libyaml-0-2 ca-certificates unzip\
     && rm -rf /var/lib/apt/lists/*

# Install nats-server binary into runtime image; it is disabled by default.
RUN set -eux; \
  case "${TARGETARCH}" in \
    amd64) NATS_ARCH=amd64 ;; \
    arm64) NATS_ARCH=arm64 ;; \
    *) echo "Unsupported TARGETARCH=${TARGETARCH}"; exit 1 ;; \
  esac; \
  curl -fsSL "https://github.com/nats-io/nats-server/releases/download/v${NATS_SERVER_VERSION}/nats-server-v${NATS_SERVER_VERSION}-linux-${NATS_ARCH}.tar.gz" \
    | tar -xz -C /tmp; \
  cp "/tmp/nats-server-v${NATS_SERVER_VERSION}-linux-${NATS_ARCH}/nats-server" /usr/local/bin/nats-server; \
  chmod +x /usr/local/bin/nats-server; \
  curl -fsSL "https://github.com/nats-io/natscli/releases/download/v${NATS_CLI_VERSION}/nats-${NATS_CLI_VERSION}-linux-${NATS_ARCH}.zip" -o /tmp/nats.zip; \
  unzip -q /tmp/nats.zip -d /tmp; \
  cp "/tmp/nats-${NATS_CLI_VERSION}-linux-${NATS_ARCH}/nats" /usr/local/bin/nats; \
  chmod +x /usr/local/bin/nats; \
  rm -rf /tmp/nats-server* /tmp/nats* /tmp/nats.zip

COPY --from=base /usr/local/bundle /usr/local/bundle

# The bundle replaces the vulnerable default JSON gem (CVE-2026-33210).
RUN rm -rf /usr/local/lib/ruby/4.0.0/json* /usr/local/lib/ruby/4.0.0/*-linux/json \
    /usr/local/lib/ruby/gems/4.0.0/gems/json-* \
    /usr/local/lib/ruby/gems/4.0.0/specifications/default/json-*.gemspec

COPY --from=docker /ruby/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Force deploy stage to depend on successful test stage completion.
COPY --from=tests /app/spec/results /app/spec/results

COPY . /app

ENV RUBY_YJIT_ENABLE=1 \
    SERVER_ENV=production \
    RACK_ENV=production \
    PORT=7000 \
    EMBEDDED_NATS_ENABLED=false \
    EMBEDDED_NATS_CONFIG="" \
    EMBEDDED_NATS_GENERATE_CONFIG=false \
    EMBEDDED_NATS_GENERATED_CONFIG_PATH=/data/nats.conf \
    EMBEDDED_NATS_JETSTREAM_ENABLED="" \
    EMBEDDED_NATS_JETSTREAM_STORE_DIR=/data \
    EMBEDDED_NATS_JS_DOMAIN="" \
    EMBEDDED_NATS_LEAF_LISTEN_HOST=0.0.0.0 \
    EMBEDDED_NATS_LEAF_LISTEN_PORT=7422 \
    EMBEDDED_NATS_LEAF_USER="" \
    EMBEDDED_NATS_LEAF_PASSWORD="" \
    LEAF_REMOTE_HOST="" \
    LEAF_REMOTE_PORT=7422 \
    LEAF_REMOTE_USER="" \
    LEAF_REMOTE_PASSWORD="" \
    LEAF_REMOTE_NKEY=""

WORKDIR /app

# --start_period=5s (Unknown flag: start_period)
HEALTHCHECK --interval=15s --timeout=2s --retries=3 CMD curl --fail http://127.0.0.1:$PORT/healthcheck || exit 1
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD bundle exec rackup -o 0.0.0.0 -p $PORT -s falcon
# docker-compose build --progress plain
