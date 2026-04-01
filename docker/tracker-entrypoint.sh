#!/bin/sh
set -eu

CERTS_SRC_DIR="${TRACKER_CERTS_DIR:-/opt/finance/certs}"
CERTS_DST_DIR="/usr/local/share/ca-certificates/finance_tracker"

export REQUESTS_CA_BUNDLE="${REQUESTS_CA_BUNDLE:-/etc/ssl/certs/ca-certificates.crt}"
export SSL_CERT_FILE="${SSL_CERT_FILE:-/etc/ssl/certs/ca-certificates.crt}"

if [ -d "$CERTS_SRC_DIR" ]; then
  mkdir -p "$CERTS_DST_DIR"

  certs_found=0
  for cert_path in "$CERTS_SRC_DIR"/*.crt; do
    if [ -f "$cert_path" ]; then
      cp "$cert_path" "$CERTS_DST_DIR"/
      certs_found=1
    fi
  done

  if [ "$certs_found" -eq 1 ]; then
    update-ca-certificates >/dev/null 2>&1
  fi
fi

exec python -u app.py
