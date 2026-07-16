#!/usr/bin/env bash
set -euo pipefail

root="${1:-artifacts/postgres-tls-ci}"
image="postgres:16@sha256:be01cf82fc7dbba824acf0a82e150b4b360f3ff93c6631d7844af431e841a95c"
rm -rf "$root"
mkdir -p "$root"

make_ca() {
  local name="$1"
  mkdir -p "$root/$name/newcerts"
  : > "$root/$name/index.txt"
  printf '1000\n' > "$root/$name/serial"
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 7 \
    -subj "/CN=AETHER ${name} test CA" \
    -keyout "$root/$name/ca.key" -out "$root/$name/ca.crt" >/dev/null 2>&1
  cat > "$root/$name/ca.cnf" <<EOF
[ ca ]
default_ca = CA_default
[ CA_default ]
database = $PWD/$root/$name/index.txt
serial = $PWD/$root/$name/serial
new_certs_dir = $PWD/$root/$name/newcerts
certificate = $PWD/$root/$name/ca.crt
private_key = $PWD/$root/$name/ca.key
default_md = sha256
default_days = 7
policy = policy_any
copy_extensions = copy
[ policy_any ]
commonName = supplied
EOF
}

sign_cert() {
  local ca="$1"
  local name="$2"
  local subject="$3"
  local dates="${4:-valid}"
  openssl req -new -newkey rsa:2048 -nodes -sha256 -subj "/CN=$subject" \
    -keyout "$root/$name.key" -out "$root/$name.csr" >/dev/null 2>&1
  if [[ "$name" == server-* ]]; then
    printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' > "$root/$name.ext"
  else
    printf 'extendedKeyUsage=clientAuth\n' > "$root/$name.ext"
  fi
  if [[ "$dates" == expired ]]; then
    openssl ca -batch -config "$root/$ca/ca.cnf" \
      -startdate 20200101000000Z -enddate 20200102000000Z \
      -extfile "$root/$name.ext" -in "$root/$name.csr" -out "$root/$name.crt" \
      >/dev/null 2>&1
  else
    openssl ca -batch -config "$root/$ca/ca.cnf" -days 7 \
      -extfile "$root/$name.ext" -in "$root/$name.csr" -out "$root/$name.crt" \
      >/dev/null 2>&1
  fi
}

for ca in old-ca new-ca expired-ca untrusted-ca; do
  make_ca "$ca"
done
sign_cert old-ca server-old localhost
sign_cert new-ca server-new localhost
sign_cert expired-ca server-expired localhost expired
sign_cert old-ca client-mtls mtls

cat > "$root/pg_hba.conf" <<'EOF'
local all all trust
hostssl all mtls 0.0.0.0/0 cert
hostssl all postgres 0.0.0.0/0 scram-sha-256
hostnossl all all 0.0.0.0/0 reject
hostssl all mtls ::0/0 cert
hostssl all postgres ::0/0 scram-sha-256
hostnossl all all ::0/0 reject
EOF

start_postgres() {
  local name="$1"
  local port="$2"
  local cert="$3"
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" -p "$port:5432" \
    -e POSTGRES_DB=aether -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres \
    -v "$PWD/$root:/tls-source:ro" \
    --entrypoint bash "$image" -c \
    "mkdir -p /var/lib/postgresql/tls && cp /tls-source/$cert.crt /var/lib/postgresql/tls/server.crt && cp /tls-source/$cert.key /var/lib/postgresql/tls/server.key && cp /tls-source/pg_hba.conf /var/lib/postgresql/tls/pg_hba.conf && cp /tls-source/old-ca/ca.crt /var/lib/postgresql/tls/old-ca.crt && chown -R postgres:postgres /var/lib/postgresql/tls && chmod 600 /var/lib/postgresql/tls/server.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/var/lib/postgresql/tls/server.crt -c ssl_key_file=/var/lib/postgresql/tls/server.key -c ssl_ca_file=/var/lib/postgresql/tls/old-ca.crt -c hba_file=/var/lib/postgresql/tls/pg_hba.conf" \
    >/dev/null
}

start_postgres aether-postgres-tls-old 5432 server-old
start_postgres aether-postgres-tls-expired 5433 server-expired
start_postgres aether-postgres-tls-new 5434 server-new

for container in aether-postgres-tls-old aether-postgres-tls-expired aether-postgres-tls-new; do
  for attempt in $(seq 1 30); do
    if docker exec "$container" pg_isready -U postgres -d aether >/dev/null 2>&1; then
      break
    fi
    if [[ "$attempt" == 30 ]]; then
      docker logs "$container"
      exit 1
    fi
    sleep 1
  done
done

docker exec aether-postgres-tls-old psql -U postgres -d aether \
  -c "CREATE ROLE mtls LOGIN SUPERUSER" >/dev/null

cat <<EOF
AETHER_POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5432/aether
AETHER_POSTGRES_TLS_TEST_URL=postgres://postgres:postgres@localhost:5432/aether
AETHER_POSTGRES_TLS_HOSTNAME_MISMATCH_URL=postgres://postgres:postgres@127.0.0.1:5432/aether
AETHER_POSTGRES_TLS_EXPIRED_URL=postgres://postgres:postgres@localhost:5433/aether
AETHER_POSTGRES_TLS_ROTATED_URL=postgres://postgres:postgres@localhost:5434/aether
AETHER_POSTGRES_MTLS_TEST_URL=postgres://mtls@localhost:5432/aether
AETHER_POSTGRES_TLS_CA=$PWD/$root/old-ca/ca.crt
AETHER_POSTGRES_TLS_UNTRUSTED_CA=$PWD/$root/untrusted-ca/ca.crt
AETHER_POSTGRES_TLS_EXPIRED_CA=$PWD/$root/expired-ca/ca.crt
AETHER_POSTGRES_TLS_ROTATED_CA=$PWD/$root/new-ca/ca.crt
AETHER_POSTGRES_MTLS_CLIENT_CERT=$PWD/$root/client-mtls.crt
AETHER_POSTGRES_MTLS_CLIENT_KEY=$PWD/$root/client-mtls.key
SSL_CERT_FILE=$PWD/$root/old-ca/ca.crt
EOF
