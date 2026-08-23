from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import json
import os
import sqlite3
import time
import uuid

DATABASE = os.getenv(
    "BITSCORE_DATABASE",
    "bitscore.db",
)
HOST = os.getenv("BITSCORE_HOST", "127.0.0.1")
PORT = int(os.getenv("BITSCORE_PORT", "8010"))
START_TIME = time.time()


PLAN_LIMITS = {
    "Start": 100,
    "Growth": 500,
    "Scale": 2000,
}


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



def usage_alert(percentage, plan):
    next_plans = {
        "Start": "Growth",
        "Growth": "Scale",
    }

    if percentage >= 100:
        level = "blocked"
    elif percentage >= 80:
        level = "warning"
    else:
        level = "normal"

    return {
        "alert_level": level,
        "upgrade_recommended": level in {"warning", "blocked"},
        "recommended_plan": next_plans.get(plan),
    }


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
        **usage_alert(percentage, row["plan"]),
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


def list_tenants():
    with connect_database() as connection:
        rows = connection.execute("""
            SELECT
                tenant_id,
                plan,
                used,
                usage_limit,
                updated_at
            FROM usage_counters
            ORDER BY updated_at DESC
        """).fetchall()

    tenants = []

    for row in rows:
        percentage = round(
            (row["used"] / row["usage_limit"]) * 100,
            1,
        )

        tenants.append({
            "tenant_id": row["tenant_id"],
            "plan": row["plan"],
            "used": row["used"],
            "limit": row["usage_limit"],
            "percentage": percentage,
        **usage_alert(percentage, row["plan"]),
            "status": (
                "blocked"
                if row["used"] >= row["usage_limit"]
                else "active"
            ),
            "updated_at": row["updated_at"],
        })

    return tenants


def upgrade_tenant(tenant_id, new_plan):
    timestamp = datetime.now(timezone.utc).isoformat()
    new_limit = PLAN_LIMITS[new_plan]

    with connect_database() as connection:
        result = connection.execute("""
            UPDATE usage_counters
            SET plan = ?, usage_limit = ?, updated_at = ?
            WHERE tenant_id = ?
        """, (
            new_plan,
            new_limit,
            timestamp,
            tenant_id,
        ))

    if result.rowcount == 0:
        return None

    return get_usage(tenant_id)


def create_tenant(tenant_id, plan, usage_limit):
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        with connect_database() as connection:
            connection.execute("""
                INSERT INTO usage_counters
                (tenant_id, plan, used, usage_limit, updated_at)
                VALUES (?, ?, 0, ?, ?)
            """, (tenant_id, plan, usage_limit, timestamp))

        return get_usage(tenant_id)

    except sqlite3.IntegrityError:
        return None


class SaaSHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="static", **kwargs)

    def send_json(self, status_code, data):
        request_id = str(uuid.uuid4())
        response = {**data, "request_id": request_id}
        body = json.dumps(response).encode("utf-8")

        self.send_response(status_code)
        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8",
        )
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Request-ID", request_id)
        self.end_headers()
        self.wfile.write(body)

    def request_data(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        tenant_id = query.get(
            "tenant_id",
            ["engenharia-de-bits"],
        )[0]

        return parsed.path, tenant_id

    def read_json(self):
        try:
            content_length = int(
                self.headers.get("Content-Length", 0)
            )

            if content_length <= 0:
                return None

            body = self.rfile.read(content_length)
            data = json.loads(body.decode("utf-8"))

            return data if isinstance(data, dict) else None

        except (ValueError, json.JSONDecodeError):
            return None

    def do_GET(self):
        path, tenant_id = self.request_data()

        if path == "/api/health":
            self.send_json(200, {
                "status": "online",
                "service": "BitsCore API",
                "version": "1.3.0",
                "uptime_seconds": round(
                    time.time() - START_TIME,
                    2,
                ),
            })
            return


        if path == "/api/tenants":
            tenants = list_tenants()

            self.send_json(200, {
                "tenants": tenants,
                "total": len(tenants),
            })
            return

        if path == "/api/usage":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa n?o encontrada"
                })
                return

            self.send_json(200, usage)
            return

        super().do_GET()

    def do_POST(self):
        path, tenant_id = self.request_data()

        if path == "/api/tenants":
            data = self.read_json()

            if data is None:
                self.send_json(400, {
                    "error": "JSON inv?lido"
                })
                return

            new_tenant_id = str(
                data.get("tenant_id", "")
            ).strip().lower()

            plan = str(
                data.get("plan", "")
            ).strip()

            try:
                usage_limit = int(data.get("limit", 0))
            except (TypeError, ValueError):
                usage_limit = 0

            valid_tenant_id = (
                3 <= len(new_tenant_id) <= 50
                and all(
                    character.islower()
                    or character.isdigit()
                    or character == "-"
                    for character in new_tenant_id
                )
            )

            if not valid_tenant_id:
                self.send_json(400, {
                    "error": (
                        "tenant_id deve usar letras min?sculas, "
                        "n?meros ou h?fen"
                    )
                })
                return

            if not plan or usage_limit <= 0:
                self.send_json(400, {
                    "error": (
                        "Plano e limite positivo s?o obrigat?rios"
                    )
                })
                return

            tenant = create_tenant(
                new_tenant_id,
                plan,
                usage_limit,
            )

            if tenant is None:
                self.send_json(409, {
                    "error": "Empresa já cadastrada"
                })
                return

            self.send_json(201, {
                "message": "Empresa cadastrada",
                "tenant": tenant,
            })
            return


        if path == "/api/usage/upgrade":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa nao encontrada"
                })
                return

            data = self.read_json()

            if data is None:
                self.send_json(400, {
                    "error": "JSON invalido"
                })
                return

            new_plan = str(
                data.get("plan", "")
            ).strip()

            if new_plan not in PLAN_LIMITS:
                self.send_json(400, {
                    "error": "Plano invalido"
                })
                return

            expected_plan = usage["recommended_plan"]

            if expected_plan is None:
                self.send_json(409, {
                    "error": "Empresa ja esta no maior plano"
                })
                return

            if new_plan != expected_plan:
                self.send_json(400, {
                    "error": (
                        "Upgrade permitido apenas para "
                        f"o plano {expected_plan}"
                    )
                })
                return

            upgraded = upgrade_tenant(
                tenant_id,
                new_plan,
            )

            self.send_json(200, {
                "message": "Plano atualizado",
                "tenant": upgraded,
            })
            return

        if path == "/api/usage/consume":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa n?o encontrada"
                })
                return

            if not consume_usage(tenant_id):
                self.send_json(403, {
                    "error": "Limite do plano atingido",
                    "usage": get_usage(tenant_id),
                })
                return

            self.send_json(200, get_usage(tenant_id))
            return

        self.send_json(404, {
            "error": "Endpoint n?o encontrado"
        })


def run_server():
    initialize_database()

    server = ThreadingHTTPServer(
        (HOST, PORT),
        SaaSHandler,
    )

    print(
        f"BitsCore SaaS Monitor: http://{HOST}:{PORT}",
        flush=True,
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Servidor encerrado.")
    finally:
        server.server_close()


if __name__ == "__main__":
    run_server()
