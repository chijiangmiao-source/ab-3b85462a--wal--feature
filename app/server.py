"""HTTP front-end for the WAL recovery engine.

POST /recover
    JSON body: {"database_base64": "...", "wal_base64": "...",
                "page_order": "numeric" | "frame"}
GET  /health
    liveness probe used by Compose.

The listen port is configurable through the PORT environment variable so the
host-side port published by Compose can be changed without rebuilding.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from wal_recover import MAX_WAL_SIZE, RecoveryError, recover

SERVICE_NAME = "wal-recovery"
MAX_BODY_BYTES = 8 * 1024 * 1024  # base64 inflates the <=2 MiB WAL somewhat
VALID_PAGE_ORDERS = ("numeric", "frame")


class RecoveryHandler(BaseHTTPRequestHandler):
    server_version = "WALRecovery/1.0"

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        if self.path.split("?", 1)[0] == "/health":
            self._send_json(200, {"status": "ok", "service": SERVICE_NAME})
        else:
            self._send_json(404, {"status": "error", "error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] != "/recover":
            self._send_json(404, {"status": "error", "error": "not found"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json(400, {"status": "error", "error": "invalid Content-Length"})
            return
        if length <= 0:
            self._send_json(400, {"status": "error", "error": "empty request body"})
            return
        if length > MAX_BODY_BYTES:
            self._send_json(
                413,
                {"status": "error", "error": "request body too large"},
            )
            return

        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send_json(400, {"status": "error", "error": "body must be JSON"})
            return
        if not isinstance(payload, dict):
            self._send_json(400, {"status": "error", "error": "JSON body must be an object"})
            return

        db_b64 = payload.get("database_base64", "")
        wal_b64 = payload.get("wal_base64", "")
        page_order = payload.get("page_order", "numeric")
        if not isinstance(db_b64, str) or not isinstance(wal_b64, str):
            self._send_json(
                400,
                {"status": "error", "error": "database_base64/wal_base64 must be strings"},
            )
            return
        if page_order not in VALID_PAGE_ORDERS:
            self._send_json(
                400,
                {
                    "status": "error",
                    "error": "page_order must be one of %s" % list(VALID_PAGE_ORDERS),
                },
            )
            return

        try:
            db = base64.b64decode(db_b64, validate=True)
            wal = base64.b64decode(wal_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            self._send_json(
                400,
                {"status": "error", "error": "invalid base64 input: %s" % exc},
            )
            return
        if len(wal) > MAX_WAL_SIZE:
            self._send_json(
                413,
                {
                    "status": "error",
                    "error": "WAL exceeds %d-byte limit (%d bytes encoded)"
                    % (MAX_WAL_SIZE, len(wal)),
                    "offset": MAX_WAL_SIZE,
                },
            )
            return

        try:
            result = recover(db, wal)
        except RecoveryError as exc:
            # No partial image is ever returned on failure.
            self._send_json(
                409,
                {
                    "status": "unrecoverable",
                    "error": exc.message,
                    "offset": exc.offset,
                    "offset_scope": exc.scope,
                },
            )
            return

        self._send_json(200, result.to_report(page_order=page_order))

    def log_message(self, fmt: str, *args) -> None:  # quieter, structured logs
        if os.environ.get("QUIET_LOGS") != "1":
            super().log_message(fmt, *args)


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    host = os.environ.get("HOST", "0.0.0.0")
    server = ThreadingHTTPServer((host, port), RecoveryHandler)
    print("%s listening on %s:%d" % (SERVICE_NAME, host, port), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
