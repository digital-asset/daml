#!/usr/bin/env python3
"""
Mock extension service for testing. Behavior controlled by function_id:

  echo            returns input
  error-{code}    returns HTTP error (400, 401, 403, 404, 500, 502, 503, 504)
  delay-{ms}      delays response
  retry-once      503 first, then 200
  retry-always    always 503
  rate-limit      429 with Retry-After
  jwt-required    needs Bearer test-token
  jwt-echo        echoes auth header
  jwt-invalid     always 401
  tls-test        confirms TLS works
  large-output    returns 100KB

Endpoints: POST /api/v1/external-call, GET /health, GET /counts, POST /reset
Stats written to /tmp/external_call_test_counts.json
"""

import json
import sys
import os
import time
import re
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
import threading
import ssl

# Global state
request_counts = {
    "submission": 0,
    "validation": 0,
    "unknown": 0,
    "total": 0,
    "by_function": {},
    "by_status": {},
}
request_log = []
retry_tracker = {}  # Track retry attempts by request pattern
lock = threading.Lock()

# Configuration
REQUIRE_JWT = os.environ.get("REQUIRE_JWT", "0") == "1"
EXPECTED_JWT = os.environ.get("EXPECTED_JWT", "test-token")
COUNTS_FILE = "/tmp/external_call_test_counts.json"


def save_counts():
    """Save current counts to file for test assertions."""
    with open(COUNTS_FILE, "w") as f:
        json.dump({
            "counts": request_counts,
            "log": request_log[-100:],  # Keep last 100 entries
            "retry_tracker": retry_tracker,
        }, f, indent=2)


class MockExtensionHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        """Custom logging to include timestamp."""
        print(f"[{datetime.now().isoformat()}] {args[0]}")

    def send_error_response(self, status_code, message, retry_after=None):
        """Send an error response with optional Retry-After header."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'text/plain')
        if retry_after is not None:
            self.send_header('Retry-After', str(retry_after))
        self.end_headers()
        self.wfile.write(message.encode('utf-8'))

    def validate_jwt(self):
        """Validate JWT if required. Returns True if valid or not required."""
        if not REQUIRE_JWT:
            return True

        auth_header = self.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return False

        token = auth_header[7:]  # Remove 'Bearer ' prefix
        return token == EXPECTED_JWT

    def do_POST(self):
        global request_counts, request_log, retry_tracker

        if self.path.startswith('/api/v1/external-call'):
            # Extract headers
            function_id = self.headers.get('X-Daml-External-Function-Id', 'unknown')
            config_hash = self.headers.get('X-Daml-External-Config-Hash', '')
            mode = self.headers.get('X-Daml-External-Mode', 'unknown')
            request_id = self.headers.get('X-Request-Id', 'none')

            # Mock control headers
            mock_status = self.headers.get('X-Mock-Status')
            mock_delay = self.headers.get('X-Mock-Delay')
            mock_retry_after = self.headers.get('X-Mock-Retry-After')

            # Read body
            content_length = int(self.headers.get('Content-Length', 0))
            input_hex = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else ''

            # Update counters
            with lock:
                request_counts["total"] += 1
                if mode in ["submission", "validation"]:
                    request_counts[mode] += 1
                else:
                    request_counts["unknown"] += 1

                # Count by function
                request_counts["by_function"][function_id] = \
                    request_counts["by_function"].get(function_id, 0) + 1

                request_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "mode": mode,
                    "function_id": function_id,
                    "config_hash": config_hash,
                    "input": input_hex[:100] + ("..." if len(input_hex) > 100 else ""),
                    "request_id": request_id,
                })
                save_counts()

            # Log the request
            print(f">>> REQUEST #{request_counts['total']}")
            print(f"    Mode: {mode}")
            print(f"    Function: {function_id}")
            print(f"    Config Hash: {config_hash}")
            print(f"    Input: {input_hex[:50]}{'...' if len(input_hex) > 50 else ''}")
            print(f"    Request ID: {request_id}")

            # JWT validation
            if not self.validate_jwt():
                print(f"<<< RESPONSE: 401 Unauthorized (JWT validation failed)")
                with lock:
                    request_counts["by_status"]["401"] = request_counts["by_status"].get("401", 0) + 1
                    save_counts()
                self.send_error_response(401, "Unauthorized: Invalid or missing JWT token")
                return

            # Apply mock delay if specified
            delay_ms = 0
            if mock_delay:
                delay_ms = int(mock_delay)
            elif function_id.startswith("delay-"):
                try:
                    delay_ms = int(function_id.split("-")[1])
                except (IndexError, ValueError):
                    pass

            if delay_ms > 0:
                print(f"    Delaying response by {delay_ms}ms...")
                time.sleep(delay_ms / 1000.0)

            # Determine response based on function_id or mock headers
            status_code = 200
            response_body = input_hex
            retry_after = None

            # Mock status override
            if mock_status:
                status_code = int(mock_status)
            # Function-based error simulation
            elif function_id.startswith("error-"):
                try:
                    status_code = int(function_id.split("-")[1])
                except (IndexError, ValueError):
                    status_code = 500

            # Special function handlers
            elif function_id == "retry-once":
                # Track by a pattern (use first 8 chars of input as key)
                retry_key = f"retry-once:{input_hex[:8]}"
                with lock:
                    attempt = retry_tracker.get(retry_key, 0)
                    retry_tracker[retry_key] = attempt + 1
                    save_counts()

                if attempt == 0:
                    status_code = 503
                    retry_after = 1
                    print(f"    retry-once: First attempt, returning 503")
                else:
                    status_code = 200
                    print(f"    retry-once: Subsequent attempt ({attempt + 1}), returning 200")

            elif function_id == "retry-always":
                status_code = 503
                retry_after = 1

            elif function_id == "rate-limit":
                status_code = 429
                retry_after = 2

            # JWT-specific function handlers (check JWT per-function, not globally)
            elif function_id == "jwt-required":
                # Requires valid JWT token
                auth_header = self.headers.get('Authorization', '')
                if auth_header == 'Bearer test-token':
                    status_code = 200
                    response_body = f"jwt-valid:{input_hex}"
                    print(f"    jwt-required: Valid JWT, returning 200")
                else:
                    status_code = 401
                    print(f"    jwt-required: Invalid/missing JWT '{auth_header}', returning 401")

            elif function_id == "jwt-echo":
                # Echo back the Authorization header (for verification)
                auth_header = self.headers.get('Authorization', 'NO_AUTH_HEADER')
                response_body = f"auth:{auth_header}|input:{input_hex}"
                print(f"    jwt-echo: Echoing auth header: {auth_header}")

            elif function_id == "jwt-invalid":
                # Always return 401 (simulates server rejecting token)
                status_code = 401
                print(f"    jwt-invalid: Simulating server-side JWT rejection")

            elif function_id == "tls-test":
                # Return info about the TLS connection for verification
                # This function exists to verify TLS connectivity works
                response_body = f"tls-ok:{input_hex}"
                print(f"    tls-test: TLS connection verified, echoing input")

            elif function_id.startswith("large-output"):
                # Return a large response (for testing large output handling)
                # Format: large-output-{size_kb}, e.g., large-output-100 for 100KB
                try:
                    size_kb = int(function_id.split("-")[2]) if "-" in function_id[12:] else 100
                except (IndexError, ValueError):
                    size_kb = 100
                # Generate large hex output (2 hex chars = 1 byte)
                target_bytes = size_kb * 1024
                # Use a pattern that's easy to verify: repeat "deadbeef"
                pattern = "deadbeef"
                repeats = (target_bytes * 2) // len(pattern) + 1
                response_body = (pattern * repeats)[:target_bytes * 2]
                print(f"    large-output: Returning {len(response_body)} hex chars ({len(response_body)//2} bytes)")

            # Apply mock retry-after header if specified
            if mock_retry_after:
                retry_after = int(mock_retry_after)

            # Count by status
            with lock:
                request_counts["by_status"][str(status_code)] = \
                    request_counts["by_status"].get(str(status_code), 0) + 1
                save_counts()

            # Send response
            print(f"<<< RESPONSE: {status_code}")

            if status_code == 200:
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('X-Request-Id', request_id)
                self.end_headers()
                self.wfile.write(response_body.encode('utf-8'))
            else:
                error_messages = {
                    400: "Bad Request",
                    401: "Unauthorized",
                    403: "Forbidden",
                    404: "Not Found",
                    408: "Request Timeout",
                    429: "Rate Limit Exceeded",
                    500: "Internal Server Error",
                    502: "Bad Gateway",
                    503: "Service Unavailable",
                    504: "Gateway Timeout",
                }
                message = error_messages.get(status_code, f"Error {status_code}")
                self.send_error_response(status_code, message, retry_after)

            print()

        elif self.path == '/reset':
            # Reset all counters and state
            with lock:
                request_counts["submission"] = 0
                request_counts["validation"] = 0
                request_counts["unknown"] = 0
                request_counts["total"] = 0
                request_counts["by_function"] = {}
                request_counts["by_status"] = {}
                request_log.clear()
                retry_tracker.clear()
                save_counts()

            print(">>> RESET: All counters and state cleared")
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Reset complete")

        else:
            self.send_error(404, 'Not found')

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"OK")

        elif self.path == '/counts':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            with lock:
                self.wfile.write(json.dumps({
                    "counts": request_counts,
                    "log": request_log[-20:],
                }, indent=2).encode('utf-8'))

        elif self.path == '/reset':
            # Also support GET for easy browser/curl reset
            with lock:
                request_counts["submission"] = 0
                request_counts["validation"] = 0
                request_counts["unknown"] = 0
                request_counts["total"] = 0
                request_counts["by_function"] = {}
                request_counts["by_status"] = {}
                request_log.clear()
                retry_tracker.clear()
                save_counts()

            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Reset complete")

        else:
            self.send_error(404, 'Not found')


def run(port=8080, use_tls=False, cert_file=None, key_file=None):
    # Initialize counts file
    save_counts()

    server = HTTPServer(('127.0.0.1', port), MockExtensionHandler)

    if use_tls:
        if not cert_file or not key_file:
            print("ERROR: TLS requires --cert and --key arguments")
            sys.exit(1)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert_file, key_file)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        scheme = "https"
    else:
        scheme = "http"

    print("=" * 60)
    print("Mock Extension Service (Enhanced)")
    print("=" * 60)
    print(f"Listening on: {scheme}://127.0.0.1:{port}")
    print(f"Counts file:  {COUNTS_FILE}")
    print(f"JWT Required: {REQUIRE_JWT}")
    if REQUIRE_JWT:
        print(f"Expected JWT: {EXPECTED_JWT[:10]}...")
    print()
    print("Endpoints:")
    print("  POST /api/v1/external-call  - Handle external calls")
    print("  GET  /health                - Health check")
    print("  GET  /counts                - Get request counts")
    print("  POST /reset                 - Reset all state")
    print()
    print("Function IDs for testing:")
    print("  echo           - Echo input (default)")
    print("  error-{code}   - Return HTTP error (e.g., error-503)")
    print("  delay-{ms}     - Delay response (e.g., delay-5000)")
    print("  retry-once     - Return 503 first, 200 second")
    print("  retry-always   - Always return 503")
    print("  rate-limit     - Return 429 with Retry-After")
    print("  jwt-required   - Require valid JWT (Bearer test-token)")
    print("  jwt-echo       - Echo the Authorization header")
    print("  jwt-invalid    - Always return 401")
    print("  large-output   - Return 100KB response")
    print()
    print("Waiting for requests...")
    print("-" * 60)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        with lock:
            print(f"\nFinal counts: {json.dumps(request_counts, indent=2)}")
        server.shutdown()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Mock Extension Service')
    parser.add_argument('port', type=int, nargs='?', default=8080, help='Port to listen on')
    parser.add_argument('--tls', action='store_true', help='Enable TLS')
    parser.add_argument('--cert', type=str, help='TLS certificate file')
    parser.add_argument('--key', type=str, help='TLS private key file')

    args = parser.parse_args()

    run(port=args.port, use_tls=args.tls, cert_file=args.cert, key_file=args.key)
