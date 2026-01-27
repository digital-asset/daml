#!/usr/bin/env python3
"""
Mock Extension Service for external call integration test.

Key features:
1. Counts requests by mode (submission vs validation)
2. Logs all requests for debugging
3. Writes request counts to a file for test assertions

The test verifies that:
- Submission from signatory: 1 request with mode=submission
- Observer validation: 0 requests (uses stored results)
"""

import json
import sys
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
import threading

# Global counters
request_counts = {
    "submission": 0,
    "validation": 0,
    "unknown": 0,
    "total": 0,
}
request_log = []
lock = threading.Lock()

# File to write counts for test assertions
COUNTS_FILE = "/tmp/external_call_test_counts.json"


def save_counts():
    """Save current counts to file for test assertions."""
    with open(COUNTS_FILE, "w") as f:
        json.dump({
            "counts": request_counts,
            "log": request_log,
        }, f, indent=2)


class MockExtensionHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        """Custom logging to include timestamp."""
        print(f"[{datetime.now().isoformat()}] {args[0]}")

    def do_POST(self):
        global request_counts, request_log

        if self.path == '/api/v1/external-call':
            # Extract headers
            function_id = self.headers.get('X-Daml-External-Function-Id', 'unknown')
            config_hash = self.headers.get('X-Daml-External-Config-Hash', '')
            mode = self.headers.get('X-Daml-External-Mode', 'unknown')
            request_id = self.headers.get('X-Request-Id', 'none')

            # Read body
            content_length = int(self.headers.get('Content-Length', 0))
            input_hex = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else ''

            # Update counters
            with lock:
                request_counts["total"] += 1
                if mode in request_counts:
                    request_counts[mode] += 1
                else:
                    request_counts["unknown"] += 1

                request_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "mode": mode,
                    "function_id": function_id,
                    "config_hash": config_hash,
                    "input": input_hex,
                    "request_id": request_id,
                })
                save_counts()

            # Log the request
            print(f">>> REQUEST #{request_counts['total']}")
            print(f"    Mode: {mode}")
            print(f"    Function: {function_id}")
            print(f"    Config Hash: {config_hash}")
            print(f"    Input: {input_hex}")
            print(f"    Request ID: {request_id}")
            print(f"    Counts: submission={request_counts['submission']}, validation={request_counts['validation']}")

            # Echo mode: return input as output
            response = input_hex

            # Send response
            self.send_response(200)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('X-Request-Id', request_id)
            self.end_headers()
            self.wfile.write(response.encode('utf-8'))

            print(f"<<< RESPONSE: {response}")
            print()

        elif self.path == '/counts':
            # Endpoint to get current counts (for debugging)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            with lock:
                self.wfile.write(json.dumps(request_counts).encode('utf-8'))

        elif self.path == '/reset':
            # Reset counters
            with lock:
                request_counts["submission"] = 0
                request_counts["validation"] = 0
                request_counts["unknown"] = 0
                request_counts["total"] = 0
                request_log.clear()
                save_counts()
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Counters reset")

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
                self.wfile.write(json.dumps(request_counts).encode('utf-8'))
        else:
            self.send_error(404, 'Not found')


def run(port=8080):
    # Initialize counts file
    save_counts()

    server = HTTPServer(('127.0.0.1', port), MockExtensionHandler)
    print(f"=" * 60)
    print(f"Mock Extension Service")
    print(f"=" * 60)
    print(f"Listening on: http://127.0.0.1:{port}")
    print(f"Counts file:  {COUNTS_FILE}")
    print()
    print("Endpoints:")
    print("  POST /api/v1/external-call  - Handle external calls (echo mode)")
    print("  GET  /health                - Health check")
    print("  GET  /counts                - Get request counts")
    print("  POST /reset                 - Reset counters")
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
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    run(port)
