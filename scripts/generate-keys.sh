#!/usr/bin/env bash
# Generates the cryptographic material of the system:
#  - one key store per replica (node1.ks, ..., nodeN.ks) and a trust store with
#    the certificates of all replicas (truststore.ks). The trust store defines the
#    (static) membership of the system;
#  - a single key store with the keys of all clients (clients.ks) and a trust
#    store with the certificates of all clients (clients-truststore.ks). Replicas
#    use the latter to verify the signatures of transfers.
#
# Usage: scripts/generate-keys.sh <number of replicas> <number of clients> [output folder]
# Example: scripts/generate-keys.sh 4 50 crypto-material

set -euo pipefail

command -v keytool >/dev/null || { echo "keytool not found (it comes with the JDK)" >&2; exit 1; }

REPLICAS=${1:?number of replicas}
CLIENTS=${2:?number of clients}
OUT=${3:-crypto-material}
PASS=password

mkdir -p "$OUT"
rm -f "$OUT"/*.ks "$OUT"/*.cert

# Runs a command quietly, showing its output only if it fails
quiet() {
    local out
    if ! out=$("$@" 2>&1); then
        echo "$out" >&2
        echo "Failed: $*" >&2
        exit 1
    fi
}

gen() { # alias keystore
    quiet keytool -genkeypair -alias "$1" -keyalg Ed25519 -validity 3650 -keystore "$2" -storetype pkcs12 \
        -storepass "$PASS" -keypass "$PASS" -dname "CN=$1, OU=SDC, O=FCT NOVA, C=PT"
}

trust() { # alias source-keystore truststore
    quiet keytool -exportcert -alias "$1" -keystore "$2" -storepass "$PASS" -file "$OUT/$1.cert"
    quiet keytool -importcert -noprompt -alias "$1" -file "$OUT/$1.cert" -keystore "$3" -storetype pkcs12 \
        -storepass "$PASS"
    rm -f "$OUT/$1.cert"
}

for i in $(seq 1 "$REPLICAS"); do
    gen "node$i" "$OUT/node$i.ks"
    trust "node$i" "$OUT/node$i.ks" "$OUT/truststore.ks"
done

for i in $(seq 1 "$CLIENTS"); do
    gen "client$i" "$OUT/clients.ks"
    trust "client$i" "$OUT/clients.ks" "$OUT/clients-truststore.ks"
done

echo "Generated $REPLICAS replicas and $CLIENTS clients in $OUT (password: $PASS)"
echo "Remember to define <alias>.replica_address and <alias>.client_address for every replica in babel.conf"
