from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import time
import uuid

START_TIME = time.time()

USAGE = {
    "plan": "Start",
    "used": 72,
    "limit": 100,
}


def usage_data():
    percentage = round((USAGE["used"] / USAGE["limit"]) * 100, 1)

    return {
        **USAGE,
        "percentage": percentage,
        "status": "blocked" if USAGE["used"] >= USAGE["limit"] else "active",
    }


class SaaSHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="static", **kwargs)

    def send_json(self, status_code, data):
        request_id = str(uuid.uuid4())
        data["request_id"] = request_id
        body = json.dumps(data).encode("utf-8")

        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Request-ID", request_id)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/health":
            self.send_json(200, {
                "status": "online",
                "service": "BitsCore API",
                "version": "1.1.0",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "uptime_seconds": round(time.time() - START_TIME, 2),
            })
            return

        if self.path == "/api/usage":
            self.send_json(200, usage_data())
            return

        super().do_GET()

    def do_POST(self):
        if self.path == "/api/usage/consume":
            if USAGE["used"] >= USAGE["limit"]:
                self.send_json(403, {
                    "error": "Limite do plano atingido",
                    "usage": usage_data(),
                })
                return

            USAGE["used"] += 1
            self.send_json(200, usage_data())
            return

        self.send_json(404, {"error": "Endpoint não encontrado"})


server = ThreadingHTTPServer(("127.0.0.1", 8010), SaaSHandler)

print("BitsCore SaaS Monitor: http://localhost:8010")
server.serve_forever()
