#!/usr/bin/env bash
set -Eeuo pipefail

NATS_PID=""
APP_PID=""

log() {
  printf '[entrypoint] %s\n' "$*"
}

is_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

generate_secret() {
  ruby -e 'require "securerandom"; print SecureRandom.alphanumeric(20)'
}

resolve_jetstream_enabled() {
  if [[ -n "${EMBEDDED_NATS_JETSTREAM_ENABLED:-}" ]]; then
    printf '%s' "${EMBEDDED_NATS_JETSTREAM_ENABLED}"
    return 0
  fi

  case "${SERVICE_ROLE:-requester}" in
    receiver) printf 'true' ;;
    requester) printf 'false' ;;
    *) printf 'false' ;;
  esac
}

resolve_js_api_prefix() {
  if [[ -n "${NATS_JS_API_PREFIX:-}" ]]; then
    printf '%s' "${NATS_JS_API_PREFIX}"
    return 0
  fi

  if [[ -n "${EMBEDDED_NATS_JS_DOMAIN:-}" ]]; then
    printf '$JS.%s.API' "${EMBEDDED_NATS_JS_DOMAIN}"
    return 0
  fi

  printf '$JS.API'
}

fail() {
  echo "$*" >&2
  exit 1
}

wait_for_nats() {
  local url="$1"
  local attempts="${EMBEDDED_NATS_READY_RETRIES:-40}"
  local sleep_s="${EMBEDDED_NATS_READY_SLEEP_SEC:-1}"
  local i

  for ((i=1; i<=attempts; i++)); do
    if nats --server "$url" rtt >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_s"
  done

  return 1
}

resolve_bootstrap_subjects() {
  local request_root="${NATS_REQUEST_SUBJECT_ROOT:-to.proxy}"
  local response_root="${NATS_RESPONSE_SUBJECT_ROOT:-from.proxy}"

  if [[ "$request_root" == "$response_root" ]]; then
    printf '%s' "${request_root}.>"
    return 0
  fi

  printf '%s,%s' "${request_root}.>" "${response_root}.>"
}

bootstrap_stream_if_needed() {
  local bootstrap_url="${NATS_URL:-nats://127.0.0.1:4222}"
  local stream="${NATS_STREAM:-proxy}"
  local subjects
  subjects="$(resolve_bootstrap_subjects)"

  if ! wait_for_nats "$bootstrap_url"; then
    fail "Embedded NATS bootstrap failed: server is not reachable at ${bootstrap_url}"
  fi

  if nats --server "$bootstrap_url" stream info "$stream" >/dev/null 2>&1; then
    log "Embedded NATS stream already exists: ${stream}"
    return 0
  fi

  log "Creating embedded NATS stream ${stream} with subjects ${subjects}"
  nats --server "$bootstrap_url" stream add "$stream" \
    --subjects "$subjects" \
    --storage file \
    --retention limits \
    --discard old \
    --defaults >/dev/null
}

resolve_bootstrap_enabled() {
  if [[ "${SERVICE_ROLE:-requester}" != "receiver" ]]; then
    printf 'false'
    return 0
  fi

  case "${NATS_MODE:-auto}" in
    jetstream) printf 'true' ;;
    *) printf 'false' ;;
  esac
}

validate_embedded_nats_generation_inputs() {
  local role="${SERVICE_ROLE:-}"
  local leaf_user="${LEAF_REMOTE_USER:-}"
  local leaf_pass="${LEAF_REMOTE_PASSWORD:-}"
  local leaf_nkey="${LEAF_REMOTE_NKEY:-}"
  local leaf_host="${LEAF_REMOTE_HOST:-}"
  local local_leaf_user="${EMBEDDED_NATS_LEAF_USER:-}"
  local local_leaf_pass="${EMBEDDED_NATS_LEAF_PASSWORD:-}"

  if [[ -z "$role" ]]; then
    fail "Embedded NATS config generation requires SERVICE_ROLE=requester|receiver"
  fi

  case "$role" in
    requester|receiver) ;;
    *) fail "Invalid SERVICE_ROLE=${role}. Allowed: requester, receiver" ;;
  esac

  if [[ -n "$local_leaf_user" && -z "$local_leaf_pass" ]]; then
    fail "EMBEDDED_NATS_LEAF_PASSWORD must be set when EMBEDDED_NATS_LEAF_USER is provided"
  fi
  if [[ -z "$local_leaf_user" && -n "$local_leaf_pass" ]]; then
    fail "EMBEDDED_NATS_LEAF_USER must be set when EMBEDDED_NATS_LEAF_PASSWORD is provided"
  fi

  if [[ "$role" == "requester" ]]; then
    if [[ -z "$leaf_host" ]]; then
      fail "Requester embedded mode requires LEAF_REMOTE_HOST"
    fi

    if [[ -n "$leaf_nkey" && ( -n "$leaf_user" || -n "$leaf_pass" ) ]]; then
      fail "Set either LEAF_REMOTE_NKEY or LEAF_REMOTE_USER/LEAF_REMOTE_PASSWORD, not both"
    fi

    if [[ -z "$leaf_nkey" ]]; then
      if [[ -z "$leaf_user" || -z "$leaf_pass" ]]; then
        fail "Requester embedded mode requires LEAF_REMOTE_USER and LEAF_REMOTE_PASSWORD (or LEAF_REMOTE_NKEY)"
      fi
    fi
  fi
}

shutdown() {
  if [[ -n "${APP_PID}" ]]; then
    kill -TERM "${APP_PID}" 2>/dev/null || true
    wait "${APP_PID}" 2>/dev/null || true
  fi

  if [[ -n "${NATS_PID}" ]]; then
    kill -TERM "${NATS_PID}" 2>/dev/null || true
    wait "${NATS_PID}" 2>/dev/null || true
  fi
}

trap shutdown TERM INT

generate_nats_config() {
  local config_path="$1"
  local jetstream_enabled
  jetstream_enabled="$(resolve_jetstream_enabled)"
  local role="${SERVICE_ROLE:-requester}"
  local jetstream_store="${EMBEDDED_NATS_JETSTREAM_STORE_DIR:-/data}"
  local jetstream_domain="${EMBEDDED_NATS_JS_DOMAIN:-}"
  local leaf_listen_host="${EMBEDDED_NATS_LEAF_LISTEN_HOST:-0.0.0.0}"
  local leaf_listen_port="${EMBEDDED_NATS_LEAF_LISTEN_PORT:-7422}"
  local leaf_local_user="${EMBEDDED_NATS_LEAF_USER:-}"
  local leaf_local_password="${EMBEDDED_NATS_LEAF_PASSWORD:-}"
  local generated_user="false"
  local generated_password="false"
  local leaf_host="${LEAF_REMOTE_HOST:-}"
  local leaf_port="${LEAF_REMOTE_PORT:-7422}"
  local leaf_user="${LEAF_REMOTE_USER:-}"
  local leaf_pass="${LEAF_REMOTE_PASSWORD:-}"
  local leaf_nkey="${LEAF_REMOTE_NKEY:-}"
  local existing_user=""
  local existing_password=""

  if [[ -f "$config_path" ]]; then
    existing_user="$(sed -n 's/^[[:space:]]*user:[[:space:]]*"\([^"]*\)".*/\1/p' "$config_path" | head -n1)"
    existing_password="$(sed -n 's/^[[:space:]]*password:[[:space:]]*"\([^"]*\)".*/\1/p' "$config_path" | head -n1)"
  fi

  if [[ -z "$leaf_local_user" ]]; then
    if [[ -n "$existing_user" ]]; then
      leaf_local_user="$existing_user"
    else
      leaf_local_user="leaf_$(generate_secret)"
      generated_user="true"
    fi
  fi
  if [[ -z "$leaf_local_password" ]]; then
    if [[ -n "$existing_password" ]]; then
      leaf_local_password="$existing_password"
    else
      leaf_local_password="$(generate_secret)"
      generated_password="true"
    fi
  fi

  mkdir -p "$(dirname "$config_path")"

  {
    if is_true "$jetstream_enabled"; then
      if [[ -n "$jetstream_domain" ]]; then
        echo "jetstream: { store_dir: \"$jetstream_store\", domain: \"$jetstream_domain\" }"
      else
        echo "jetstream: { store_dir: \"$jetstream_store\" }"
      fi
    fi
    echo ""
    echo "leafnodes {"
    echo "  listen: \"${leaf_listen_host}:${leaf_listen_port}\""
    cat <<EOF
  authorization {
    user: "${leaf_local_user}"
    password: "${leaf_local_password}"
  }
EOF
    if [[ "$role" == "requester" ]]; then
      echo "  remotes: ["
      if [[ -n "$leaf_host" ]]; then
        if [[ -n "$leaf_nkey" ]]; then
          cat <<EOF
    {
      url: "nats://${leaf_host}:${leaf_port}",
      nkey: "${leaf_nkey}"
    }
EOF
        elif [[ -n "$leaf_user" && -n "$leaf_pass" ]]; then
          cat <<EOF
    {
      url: "nats://${leaf_user}:${leaf_pass}@${leaf_host}:${leaf_port}"
    }
EOF
        else
          cat <<EOF
    {
      url: "nats://${leaf_host}:${leaf_port}"
    }
EOF
        fi
      fi
      echo "  ]"
    fi
    echo "}"
  } >"$config_path"

  if [[ "$generated_user" == "true" || "$generated_password" == "true" ]]; then
    log "Generated embedded leafnode credentials:"
    log "EMBEDDED_NATS_LEAF_USER=${leaf_local_user}"
    log "EMBEDDED_NATS_LEAF_PASSWORD=${leaf_local_password}"
    log "Use these values on the peer side to connect via leafnode."
  fi
}

if is_true "${EMBEDDED_NATS_ENABLED:-false}"; then
  if [[ -z "${EMBEDDED_NATS_CONFIG:-}" ]]; then
    if is_true "${EMBEDDED_NATS_GENERATE_CONFIG:-false}"; then
      validate_embedded_nats_generation_inputs
      EMBEDDED_NATS_CONFIG="${EMBEDDED_NATS_GENERATED_CONFIG_PATH:-/data/nats.conf}"
      generate_nats_config "${EMBEDDED_NATS_CONFIG}"
    else
      echo "Embedded NATS is enabled, but neither EMBEDDED_NATS_CONFIG nor EMBEDDED_NATS_GENERATE_CONFIG=true is provided" >&2
      exit 1
    fi
  fi

  if [[ ! -f "${EMBEDDED_NATS_CONFIG}" ]]; then
    echo "Embedded NATS config not found: ${EMBEDDED_NATS_CONFIG}" >&2
    exit 1
  fi

  log "Starting embedded nats-server with config: ${EMBEDDED_NATS_CONFIG}"
  nats-server -c "${EMBEDDED_NATS_CONFIG}" &

  if is_true "$(resolve_bootstrap_enabled)"; then
    bootstrap_stream_if_needed
  fi

  export NATS_JS_API_PREFIX
  NATS_JS_API_PREFIX="$(resolve_js_api_prefix)"
  log "Using NATS_JS_API_PREFIX=${NATS_JS_API_PREFIX}"

  NATS_PID="$!"
  "$@" &
  APP_PID="$!"
  wait "${APP_PID}"
  APP_EXIT=$?
  shutdown
  exit "${APP_EXIT}"
fi

exec "$@"
