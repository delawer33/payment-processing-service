"""Приёмник webhook для демо: печатает каждый POST и отвечает 200.

Только stdlib. `WEBHOOK_SINK_FAIL_FIRST=N` — первые N запросов получают 500, чтобы показать
повторные попытки consumer'а.
"""

import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = 9000
FAIL_FIRST = int(os.environ.get("WEBHOOK_SINK_FAIL_FIRST", "0"))
_seen = 0


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        global _seen
        _seen += 1
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode(errors="replace")
        status = 500 if _seen <= FAIL_FIRST else 200
        print(
            f"#{_seen} {self.command} {self.path} -> {status} "
            f"X-Signature={self.headers.get('X-Signature')} body={body}",
            flush=True,
        )
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        """Стандартный access-лог не нужен: запрос уже напечатан в do_POST."""


if __name__ == "__main__":
    print(f"webhook sink on :{PORT}, fail first {FAIL_FIRST}", file=sys.stderr, flush=True)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
