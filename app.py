from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import json
import sqlite3
import time
import uuid

DATABASE = "bitscore.db"
START_TIME = time.time()


def connect_database():
    connection = sqlite3.connect(DATABASE)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database():
    with connect_database() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS usage_counters (
                tenant_id TEXT PRIMARY KEY,
                plan TEXT NOT NULL,
                used INTEGER NOT NULL DEFAULT 0,
                usage_limit INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        connection.execute("""
            INSERT OR IGNORE INTO usage_counters
            (tenant_id, plan, used, usage_limit, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            "engenharia-de-bits",
            "Start",
            72,
            100,
            datetime.now(timezone.utc).isoformat()
        ))


def get_usage(tenant_id):
    with connect_database() as connection:
        row = connection.execute("""
            SELECT tenant_id, plan, used, usage_limit, updated_at
            FROM usage_counters
            WHERE tenant_id = ?
        """, (tenant_id,)).fetchone()

    if row is None:
        return None

    percentage = round((row["used"] / row["usage_limit"]) * 100, 1)

    return {
        "tenant_id": row["tenant_id"],
        "plan": row["plan"],
        "used": row["used"],
        "limit": row["usage_limit"],
        "percentage": percentage,
        "status": "blocked" if row["used"] >= row["usage_limit"] else "active",
        "updated_at": row["updated_at"],
    }


def consume_usage(tenant_id):
    timestamp = datetime.now(timezone.utc).isoformat()

    with connect_database() as connection:
        result = connection.execute("""
            UPDATE usage_counters
            SET used = used + 1, updated_at = ?
            WHERE tenant_id = ? AND used < usage_limit
        """, (timestamp, tenant_id))

    return result.rowcount == 1


class SaaSHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="static", **kwargs)

    def send_json(self, status_code, data):
        request_id = str(uuid.uuid4())
        response = {**data, "request_id": request_id}
        body = json.dumps(response).encode("utf-8")

        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Request-ID", request_id)
        self.end_headers()
        self.wfile.write(body)

    def request_data(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        return parsed.path, query.get(
            "tenant_id",
            ["engenharia-de-bits"]
        )[0]

    def do_GET(self):
        path, tenant_id = self.request_data()

        if path == "/api/health":
            self.send_json(200, {
                "status": "online",
                "service": "BitsCore API",
                "version": "1.2.0",
                "uptime_seconds": round(time.time() - START_TIME, 2),
            })
            return

        if path == "/api/usage":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {"error": "Empresa não encontrada"})
                return

            self.send_json(200, usage)
            return

        super().do_GET()

    def do_POST(self):
        path, tenant_id = self.request_data()

        if path != "/api/usage/consume":
            self.send_json(404, {"error": "Endpoint não encontrado"})
            return

        usage = get_usage(tenant_id)

        if usage is None:
            self.send_json(404, {"error": "Empresa não encontrada"})
            return

        if not consume_usage(tenant_id):
            self.send_json(403, {
                "error": "Limite do plano atingido",
                "usage": get_usage(tenant_id),
            })
            return

        self.send_json(200, get_usage(tenant_id))


initialize_database()

server = ThreadingHTTPServer(("127.0.0.1", 8010), SaaSHandler)
print("BitsCore SaaS Monitor: http://localhost:8010")
server.serve_forever()
