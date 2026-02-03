#!/bin/bash
#
# Generate self-signed certificates for TLS testing
#
# Creates:
#   - certs/ca.crt, certs/ca.key       - Certificate Authority
#   - certs/server.crt, certs/server.key - Server certificate signed by CA
#
# The server certificate is valid for localhost and 127.0.0.1

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="$SCRIPT_DIR/../certs"

echo "Generating TLS certificates for testing..."
echo "Output directory: $CERT_DIR"
echo ""

# Create certs directory
mkdir -p "$CERT_DIR"

# Generate CA private key
echo "[1/4] Generating CA private key..."
openssl genrsa -out "$CERT_DIR/ca.key" 4096 2>/dev/null

# Generate CA certificate
echo "[2/4] Generating CA certificate..."
openssl req -x509 -new -nodes \
    -key "$CERT_DIR/ca.key" \
    -sha256 -days 365 \
    -out "$CERT_DIR/ca.crt" \
    -subj "/C=CH/ST=Zurich/L=Zurich/O=Test CA/CN=Test CA" \
    2>/dev/null

# Generate server private key
echo "[3/4] Generating server private key..."
openssl genrsa -out "$CERT_DIR/server.key" 4096 2>/dev/null

# Create server certificate signing request with SANs
echo "[4/4] Generating server certificate..."
cat > "$CERT_DIR/server.cnf" << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = CH
ST = Zurich
L = Zurich
O = Test Server
CN = localhost

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

# Generate CSR
openssl req -new \
    -key "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.csr" \
    -config "$CERT_DIR/server.cnf" \
    2>/dev/null

# Sign the certificate with CA
openssl x509 -req \
    -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.crt" \
    -CAkey "$CERT_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERT_DIR/server.crt" \
    -days 365 \
    -sha256 \
    -extensions v3_req \
    -extfile "$CERT_DIR/server.cnf" \
    2>/dev/null

# Clean up CSR and config
rm -f "$CERT_DIR/server.csr" "$CERT_DIR/server.cnf" "$CERT_DIR/ca.srl"

# Set permissions
chmod 644 "$CERT_DIR"/*.crt
chmod 600 "$CERT_DIR"/*.key

echo ""
echo "Certificates generated successfully!"
echo ""
echo "Files created:"
echo "  $CERT_DIR/ca.crt      - CA certificate"
echo "  $CERT_DIR/ca.key      - CA private key"
echo "  $CERT_DIR/server.crt  - Server certificate (signed by CA)"
echo "  $CERT_DIR/server.key  - Server private key"
echo ""
echo "To start mock service with TLS:"
echo "  python3 scripts/mock_service.py 8443 --tls --cert certs/server.crt --key certs/server.key"
