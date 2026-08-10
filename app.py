from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import time
import uuid

START_TIME = time.time()


class SaaSHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="static", **kwargs)

    def do_GET(self):
        if self.path == "/api/health":
            request_id = str(uuid.uuid4())

            response = {
                "status": "online",
                "service": "BitsCore API",
                "version": "1.0.0",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "uptime_seconds": round(time.time() - START_TIME, 2),
                "request_id": request_id,
            }

            body = json.dumps(response).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Request-ID", request_id)
            self.end_headers()
            self.wfile.write(body)
            return

        super().do_GET()


server = ThreadingHTTPServer(("127.0.0.1", 8010), SaaSHandler)

print("BitsCore Health Monitor: http://localhost:8010")
server.serve_forever()
