from datetime import datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import csv
import io
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
            CREATE TABLE IF NOT EXISTS idempotency_records (
                tenant_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                response_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (tenant_id, idempotency_key),
                FOREIGN KEY (tenant_id)
                    REFERENCES usage_counters (tenant_id)
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS subscription_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                previous_plan TEXT NOT NULL,
                new_plan TEXT NOT NULL,
                previous_limit INTEGER NOT NULL,
                new_limit INTEGER NOT NULL,
                actor_type TEXT NOT NULL DEFAULT 'system',
                actor_id TEXT NOT NULL DEFAULT 'legacy',
                created_at TEXT NOT NULL,
                FOREIGN KEY (tenant_id)
                    REFERENCES usage_counters (tenant_id)
            )
        """)

        event_columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(subscription_events)"
            ).fetchall()
        }

        if "actor_type" not in event_columns:
            connection.execute("""
                ALTER TABLE subscription_events
                ADD COLUMN actor_type TEXT
                NOT NULL DEFAULT 'system'
            """)

        if "actor_id" not in event_columns:
            connection.execute("""
                ALTER TABLE subscription_events
                ADD COLUMN actor_id TEXT
                NOT NULL DEFAULT 'legacy'
            """)

        connection.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_subscription_events_tenant_created
            ON subscription_events (
                tenant_id,
                created_at DESC
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


def consume_usage(tenant_id, idempotency_key):
    timestamp = datetime.now(timezone.utc).isoformat()

    with connect_database() as connection:
        connection.execute("BEGIN IMMEDIATE")

        stored = connection.execute("""
            SELECT response_json
            FROM idempotency_records
            WHERE tenant_id = ? AND idempotency_key = ?
        """, (
            tenant_id,
            idempotency_key,
        )).fetchone()

        if stored is not None:
            return {
                "status": "replay",
                "usage": json.loads(
                    stored["response_json"]
                ),
            }

        result = connection.execute("""
            UPDATE usage_counters
            SET used = used + 1, updated_at = ?
            WHERE tenant_id = ? AND used < usage_limit
        """, (
            timestamp,
            tenant_id,
        ))

        if result.rowcount != 1:
            return {
                "status": "blocked",
                "usage": None,
            }

        row = connection.execute("""
            SELECT tenant_id, plan, used, usage_limit, updated_at
            FROM usage_counters
            WHERE tenant_id = ?
        """, (tenant_id,)).fetchone()

        percentage = round(
            (row["used"] / row["usage_limit"]) * 100,
            1,
        )

        usage = {
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
        }

        connection.execute("""
            INSERT INTO idempotency_records (
                tenant_id,
                idempotency_key,
                response_json,
                created_at
            )
            VALUES (?, ?, ?, ?)
        """, (
            tenant_id,
            idempotency_key,
            json.dumps(usage),
            timestamp,
        ))

    return {
        "status": "consumed",
        "usage": usage,
    }


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


def parse_actor_filter(query):
    actor_type = str(
        query.get("actor_type", [""])[0]
    ).strip().lower()

    valid_actor_types = {
        "customer",
        "admin",
        "system",
    }

    if (
        actor_type
        and actor_type not in valid_actor_types
    ):
        return None, (
            "Filtro actor_type invalido: use "
            "customer, admin ou system"
        )

    return actor_type, None


def parse_date_filters(query):
    date_from = str(
        query.get("date_from", [""])[0]
    ).strip()

    date_to = str(
        query.get("date_to", [""])[0]
    ).strip()

    parsed_from = None
    parsed_to = None

    try:
        if date_from:
            parsed_from = datetime.strptime(
                date_from,
                "%Y-%m-%d",
            ).date()

        if date_to:
            parsed_to = datetime.strptime(
                date_to,
                "%Y-%m-%d",
            ).date()
    except ValueError:
        return None, (
            "Periodo invalido: use datas no "
            "formato YYYY-MM-DD"
        )

    if (
        parsed_from is not None
        and parsed_to is not None
        and parsed_from > parsed_to
    ):
        return None, (
            "Periodo invalido: date_from nao "
            "pode ser posterior a date_to"
        )

    start_timestamp = (
        f"{date_from}T00:00:00"
        if parsed_from is not None
        else ""
    )

    end_timestamp = (
        f"{parsed_to + timedelta(days=1)}T00:00:00"
        if parsed_to is not None
        else ""
    )

    return {
        "date_from": date_from,
        "date_to": date_to,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
    }, None


def list_subscription_events(
    tenant_id,
    limit=10,
    offset=0,
    actor_type="",
    date_from="",
    date_to_exclusive="",
):
    with connect_database() as connection:
        parameters = (
            tenant_id,
            actor_type,
            actor_type,
            date_from,
            date_from,
            date_to_exclusive,
            date_to_exclusive,
        )

        rows = connection.execute("""
            SELECT
                id,
                tenant_id,
                event_type,
                previous_plan,
                new_plan,
                previous_limit,
                new_limit,
                actor_type,
                actor_id,
                created_at
            FROM subscription_events
            WHERE tenant_id = ?
              AND (? = '' OR actor_type = ?)
              AND (? = '' OR created_at >= ?)
              AND (? = '' OR created_at < ?)
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
        """, (
            *parameters,
            limit,
            offset,
        )).fetchall()

        total = connection.execute("""
            SELECT COUNT(*) AS total
            FROM subscription_events
            WHERE tenant_id = ?
              AND (? = '' OR actor_type = ?)
              AND (? = '' OR created_at >= ?)
              AND (? = '' OR created_at < ?)
        """, parameters).fetchone()["total"]

    events = [
        {
            "id": row["id"],
            "tenant_id": row["tenant_id"],
            "event_type": row["event_type"],
            "previous_plan": row["previous_plan"],
            "new_plan": row["new_plan"],
            "previous_limit": row["previous_limit"],
            "new_limit": row["new_limit"],
            "actor_type": row["actor_type"],
            "actor_id": row["actor_id"],
            "created_at": row["created_at"],
        }
        for row in rows
    ]

    return events, total


def upgrade_tenant(
    tenant_id,
    new_plan,
    actor_type,
    actor_id,
):
    timestamp = datetime.now(timezone.utc).isoformat()
    new_limit = PLAN_LIMITS[new_plan]

    with connect_database() as connection:
        current = connection.execute("""
            SELECT plan, usage_limit
            FROM usage_counters
            WHERE tenant_id = ?
        """, (tenant_id,)).fetchone()

        if current is None:
            return None

        connection.execute("""
            UPDATE usage_counters
            SET plan = ?, usage_limit = ?, updated_at = ?
            WHERE tenant_id = ?
        """, (
            new_plan,
            new_limit,
            timestamp,
            tenant_id,
        ))

        connection.execute("""
            INSERT INTO subscription_events (
                tenant_id,
                event_type,
                previous_plan,
                new_plan,
                previous_limit,
                new_limit,
                actor_type,
                actor_id,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tenant_id,
            "plan_upgraded",
            current["plan"],
            new_plan,
            current["usage_limit"],
            new_limit,
            actor_type,
            actor_id,
            timestamp,
        ))

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

    def send_csv(self, filename, content):
        request_id = str(uuid.uuid4())
        body = content.encode("utf-8-sig")

        self.send_response(200)
        self.send_header(
            "Content-Type",
            "text/csv; charset=utf-8",
        )
        self.send_header(
            "Content-Disposition",
            f'attachment; filename="{filename}"',
        )
        self.send_header(
            "Content-Length",
            str(len(body)),
        )
        self.send_header(
            "X-Request-ID",
            request_id,
        )
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

        if path == "/api/subscription/events/export":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa nao encontrada",
                })
                return

            query = parse_qs(
                urlparse(self.path).query
            )

            actor_type, filter_error = (
                parse_actor_filter(query)
            )

            if filter_error:
                self.send_json(400, {
                    "error": filter_error,
                })
                return

            date_filters, date_filter_error = (
                parse_date_filters(query)
            )

            if date_filter_error:
                self.send_json(400, {
                    "error": date_filter_error,
                })
                return

            events = []
            offset = 0
            page_size = 50

            while True:
                batch, total = list_subscription_events(
                    tenant_id,
                    page_size,
                    offset,
                    actor_type,
                    date_filters["start_timestamp"],
                    date_filters["end_timestamp"],
                )

                events.extend(batch)
                offset += len(batch)

                if offset >= total or not batch:
                    break

            output = io.StringIO()

            fields = [
                "id",
                "tenant_id",
                "event_type",
                "previous_plan",
                "new_plan",
                "previous_limit",
                "new_limit",
                "actor_type",
                "actor_id",
                "created_at",
            ]

            writer = csv.DictWriter(
                output,
                fieldnames=fields,
                lineterminator="\n",
            )

            writer.writeheader()
            writer.writerows(events)

            filename = (
                "subscription-events-"
                f"{tenant_id}.csv"
            )

            self.send_csv(
                filename,
                output.getvalue(),
            )
            return

        if path == "/api/subscription/events":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa nao encontrada"
                })
                return

            query = parse_qs(
                urlparse(self.path).query
            )

            actor_type, filter_error = (
                parse_actor_filter(query)
            )

            if filter_error:
                self.send_json(400, {
                    "error": filter_error,
                })
                return

            date_filters, date_filter_error = (
                parse_date_filters(query)
            )

            if date_filter_error:
                self.send_json(400, {
                    "error": date_filter_error,
                })
                return

            try:
                limit = int(
                    query.get("limit", ["10"])[0]
                )
                offset = int(
                    query.get("offset", ["0"])[0]
                )
            except (TypeError, ValueError):
                self.send_json(400, {
                    "error": "Paginacao invalida"
                })
                return

            if (
                limit < 1
                or limit > 50
                or offset < 0
            ):
                self.send_json(400, {
                    "error": (
                        "Paginacao invalida: limit deve estar "
                        "entre 1 e 50 e offset deve ser "
                        "maior ou igual a 0"
                    )
                })
                return

            events, total = list_subscription_events(
                tenant_id,
                limit,
                offset,
                actor_type,
                date_filters["start_timestamp"],
                date_filters["end_timestamp"],
            )

            has_more = (
                offset + len(events) < total
            )

            next_offset = (
                offset + len(events)
                if has_more
                else None
            )

            self.send_json(200, {
                "tenant_id": tenant_id,
                "events": events,
                "total": total,
                "filters": {
                    "actor_type": actor_type or None,
                    "date_from": (
                        date_filters["date_from"] or None
                    ),
                    "date_to": (
                        date_filters["date_to"] or None
                    ),
                },
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "returned": len(events),
                    "total": total,
                    "has_more": has_more,
                    "next_offset": next_offset,
                },
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

            actor_type = str(
                data.get("actor_type", "customer")
            ).strip().lower()

            actor_id = str(
                data.get("actor_id", "self-service-api")
            ).strip()

            valid_actor_types = {
                "customer",
                "admin",
                "system",
            }

            if actor_type not in valid_actor_types:
                self.send_json(400, {
                    "error": "Tipo de responsavel invalido"
                })
                return

            if not actor_id or len(actor_id) > 100:
                self.send_json(400, {
                    "error": (
                        "Identificador do responsavel "
                        "deve ter entre 1 e 100 caracteres"
                    )
                })
                return

            upgraded = upgrade_tenant(
                tenant_id,
                new_plan,
                actor_type,
                actor_id,
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

            idempotency_key = self.headers.get(
                "Idempotency-Key",
                "",
            ).strip()

            if (
                not idempotency_key
                or len(idempotency_key) > 100
            ):
                self.send_json(400, {
                    "error": (
                        "Idempotency-Key deve ter "
                        "entre 1 e 100 caracteres"
                    )
                })
                return

            result = consume_usage(
                tenant_id,
                idempotency_key,
            )

            if result["status"] == "blocked":
                self.send_json(403, {
                    "error": "Limite do plano atingido",
                    "usage": get_usage(tenant_id),
                })
                return

            self.send_json(200, {
                **result["usage"],
                "idempotent_replay": (
                    result["status"] == "replay"
                ),
            })
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
