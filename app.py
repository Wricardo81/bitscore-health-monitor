from datetime import datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from math import ceil
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
            CREATE TABLE IF NOT EXISTS usage_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                amount INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (tenant_id)
                    REFERENCES usage_counters (tenant_id)
            )
        """)

        connection.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_usage_events_tenant_created
            ON usage_events (
                tenant_id,
                created_at
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS usage_risk_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                previous_risk_level TEXT,
                risk_level TEXT NOT NULL,
                days_to_limit INTEGER,
                recommendation TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (tenant_id)
                    REFERENCES usage_counters (tenant_id)
            )
        """)

        connection.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_usage_risk_alerts_tenant_created
            ON usage_risk_alerts (
                tenant_id,
                created_at DESC
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS notification_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                alert_id INTEGER NOT NULL,
                channel TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                delivered_at TEXT,
                UNIQUE (alert_id, channel),
                FOREIGN KEY (tenant_id)
                    REFERENCES usage_counters (tenant_id),
                FOREIGN KEY (alert_id)
                    REFERENCES usage_risk_alerts (id)
            )
        """)

        connection.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_notification_outbox_tenant_status
            ON notification_outbox (
                tenant_id,
                status,
                created_at
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
            INSERT INTO usage_events (
                tenant_id,
                amount,
                created_at
            )
            VALUES (?, ?, ?)
        """, (
            tenant_id,
            1,
            timestamp,
        ))

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


def get_usage_trend(tenant_id, days=7):
    today = datetime.now(timezone.utc).date()
    first_day = today - timedelta(days=days - 1)
    day_after_period = today + timedelta(days=1)

    start_timestamp = f"{first_day}T00:00:00"
    end_timestamp = f"{day_after_period}T00:00:00"

    with connect_database() as connection:
        rows = connection.execute("""
            SELECT
                substr(created_at, 1, 10) AS event_date,
                SUM(amount) AS total
            FROM usage_events
            WHERE tenant_id = ?
              AND created_at >= ?
              AND created_at < ?
            GROUP BY event_date
            ORDER BY event_date
        """, (
            tenant_id,
            start_timestamp,
            end_timestamp,
        )).fetchall()

    totals_by_date = {
        row["event_date"]: row["total"]
        for row in rows
    }

    trend = []

    for day_offset in range(days):
        current_date = (
            first_day + timedelta(days=day_offset)
        )

        date_text = current_date.isoformat()

        trend.append({
            "date": date_text,
            "amount": totals_by_date.get(date_text, 0),
        })

    total_consumed = sum(
        point["amount"]
        for point in trend
    )

    return {
        "days": days,
        "date_from": first_day.isoformat(),
        "date_to": today.isoformat(),
        "total_consumed": total_consumed,
        "daily_average": round(
            total_consumed / days,
            2,
        ),
        "trend": trend,
    }


def get_usage_forecast(tenant_id, window=7):
    usage = get_usage(tenant_id)
    trend = get_usage_trend(tenant_id, window)

    remaining = max(
        usage["limit"] - usage["used"],
        0,
    )

    consumed = trend["total_consumed"]
    raw_daily_rate = consumed / window
    today = datetime.now(timezone.utc).date()

    if remaining == 0:
        days_to_limit = 0
        projected_date = today.isoformat()
        risk_level = "blocked"
        recommendation = (
            "Limite atingido. Realize o upgrade "
            "para continuar o consumo."
        )
    elif consumed == 0:
        days_to_limit = None
        projected_date = None
        risk_level = "insufficient_data"
        recommendation = (
            "Ainda nao existem consumos suficientes "
            "para calcular uma previsao."
        )
    else:
        days_to_limit = ceil(
            remaining / raw_daily_rate
        )

        projected_date = (
            today + timedelta(days=days_to_limit)
        ).isoformat()

        if days_to_limit <= 3:
            risk_level = "critical"
            recommendation = (
                "Upgrade urgente recomendado."
            )
        elif days_to_limit <= 7:
            risk_level = "warning"
            recommendation = (
                "Planeje o upgrade nesta semana."
            )
        else:
            risk_level = "stable"
            recommendation = (
                "Consumo dentro da previsao segura."
            )

    return {
        "window_days": window,
        "remaining": remaining,
        "daily_rate": round(raw_daily_rate, 2),
        "days_to_limit": days_to_limit,
        "projected_exhaustion_date": projected_date,
        "risk_level": risk_level,
        "recommendation": recommendation,
    }


def serialize_usage_risk_alert(row):
    if row is None:
        return None

    return {
        "id": row["id"],
        "tenant_id": row["tenant_id"],
        "previous_risk_level": (
            row["previous_risk_level"]
        ),
        "risk_level": row["risk_level"],
        "days_to_limit": row["days_to_limit"],
        "recommendation": row["recommendation"],
        "created_at": row["created_at"],
    }


def list_usage_risk_alerts(tenant_id, limit=10):
    with connect_database() as connection:
        rows = connection.execute("""
            SELECT
                id,
                tenant_id,
                previous_risk_level,
                risk_level,
                days_to_limit,
                recommendation,
                created_at
            FROM usage_risk_alerts
            WHERE tenant_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
        """, (
            tenant_id,
            limit,
        )).fetchall()

    return [
        serialize_usage_risk_alert(row)
        for row in rows
    ]


def evaluate_usage_risk(tenant_id, window=7):
    forecast = get_usage_forecast(
        tenant_id,
        window,
    )

    current_risk = forecast["risk_level"]
    timestamp = datetime.now(timezone.utc).isoformat()

    with connect_database() as connection:
        connection.execute("BEGIN IMMEDIATE")

        latest = connection.execute("""
            SELECT
                id,
                tenant_id,
                previous_risk_level,
                risk_level,
                days_to_limit,
                recommendation,
                created_at
            FROM usage_risk_alerts
            WHERE tenant_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        """, (tenant_id,)).fetchone()

        previous_risk = (
            latest["risk_level"]
            if latest is not None
            else None
        )

        if previous_risk == current_risk:
            return {
                "created": False,
                "alert": serialize_usage_risk_alert(
                    latest
                ),
                "forecast": forecast,
            }

        if (
            previous_risk is None
            and current_risk in {
                "stable",
                "insufficient_data",
            }
        ):
            return {
                "created": False,
                "alert": None,
                "forecast": forecast,
            }

        result = connection.execute("""
            INSERT INTO usage_risk_alerts (
                tenant_id,
                previous_risk_level,
                risk_level,
                days_to_limit,
                recommendation,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            tenant_id,
            previous_risk,
            current_risk,
            forecast["days_to_limit"],
            forecast["recommendation"],
            timestamp,
        ))

        alert = connection.execute("""
            SELECT
                id,
                tenant_id,
                previous_risk_level,
                risk_level,
                days_to_limit,
                recommendation,
                created_at
            FROM usage_risk_alerts
            WHERE id = ?
        """, (
            result.lastrowid,
        )).fetchone()

        payload = json.dumps(
            {
                "tenant_id": tenant_id,
                "previous_risk_level": previous_risk,
                "risk_level": current_risk,
                "days_to_limit": forecast["days_to_limit"],
                "recommendation": forecast["recommendation"],
            },
            ensure_ascii=False,
        )

        connection.execute("""
            INSERT OR IGNORE INTO notification_outbox (
                tenant_id,
                alert_id,
                channel,
                event_type,
                payload,
                status,
                attempts,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, 'pending', 0, ?)
        """, (
            tenant_id,
            alert["id"],
            "email",
            "usage_risk_changed",
            payload,
            timestamp,
        ))

    return {
        "created": True,
        "alert": serialize_usage_risk_alert(alert),
        "forecast": forecast,
    }


def serialize_notification(row):
    if row is None:
        return None

    return {
        "id": row["id"],
        "tenant_id": row["tenant_id"],
        "alert_id": row["alert_id"],
        "channel": row["channel"],
        "event_type": row["event_type"],
        "payload": json.loads(row["payload"]),
        "status": row["status"],
        "attempts": row["attempts"],
        "created_at": row["created_at"],
        "delivered_at": row["delivered_at"],
    }


def list_notifications(
    tenant_id,
    status="pending",
    limit=20,
):
    with connect_database() as connection:
        rows = connection.execute("""
            SELECT
                id,
                tenant_id,
                alert_id,
                channel,
                event_type,
                payload,
                status,
                attempts,
                created_at,
                delivered_at
            FROM notification_outbox
            WHERE tenant_id = ?
              AND (? = 'all' OR status = ?)
            ORDER BY created_at DESC, id DESC
            LIMIT ?
        """, (
            tenant_id,
            status,
            status,
            limit,
        )).fetchall()

    return [
        serialize_notification(row)
        for row in rows
    ]


def dispatch_next_notification(tenant_id):
    timestamp = datetime.now(timezone.utc).isoformat()

    with connect_database() as connection:
        connection.execute("BEGIN IMMEDIATE")

        pending = connection.execute("""
            SELECT id
            FROM notification_outbox
            WHERE tenant_id = ?
              AND status = 'pending'
            ORDER BY created_at, id
            LIMIT 1
        """, (tenant_id,)).fetchone()

        if pending is None:
            return None

        connection.execute("""
            UPDATE notification_outbox
            SET
                status = 'delivered',
                attempts = attempts + 1,
                delivered_at = ?
            WHERE id = ?
              AND tenant_id = ?
              AND status = 'pending'
        """, (
            timestamp,
            pending["id"],
            tenant_id,
        ))

        delivered = connection.execute("""
            SELECT
                id,
                tenant_id,
                alert_id,
                channel,
                event_type,
                payload,
                status,
                attempts,
                created_at,
                delivered_at
            FROM notification_outbox
            WHERE id = ?
              AND tenant_id = ?
        """, (
            pending["id"],
            tenant_id,
        )).fetchone()

    return serialize_notification(delivered)



def get_operational_metrics():
    with connect_database() as connection:
        totals = connection.execute("""
            SELECT
                (SELECT COUNT(*) FROM usage_counters)
                    AS tenants_total,
                (SELECT COUNT(*) FROM usage_events)
                    AS usage_events_total,
                (SELECT COUNT(*) FROM subscription_events)
                    AS subscription_events_total,
                (SELECT COUNT(*) FROM usage_risk_alerts)
                    AS risk_alerts_total
        """).fetchone()

        notification_rows = connection.execute("""
            SELECT
                status,
                COUNT(*) AS total
            FROM notification_outbox
            GROUP BY status
        """).fetchall()

    notifications = {
        "pending": 0,
        "delivered": 0,
        "failed": 0,
    }

    for row in notification_rows:
        notifications[row["status"]] = row["total"]

    return {
        "uptime_seconds": round(
            time.time() - START_TIME,
            2,
        ),
        "tenants_total": totals["tenants_total"],
        "usage_events_total": (
            totals["usage_events_total"]
        ),
        "subscription_events_total": (
            totals["subscription_events_total"]
        ),
        "risk_alerts_total": (
            totals["risk_alerts_total"]
        ),
        "notifications": notifications,
    }


def render_prometheus_metrics(metrics):
    lines = [
        "# HELP bitscore_process_uptime_seconds "
        "Tempo de execucao do processo.",
        "# TYPE bitscore_process_uptime_seconds gauge",
        (
            "bitscore_process_uptime_seconds "
            f"{metrics['uptime_seconds']}"
        ),
        "# HELP bitscore_tenants_total "
        "Quantidade de tenants cadastrados.",
        "# TYPE bitscore_tenants_total gauge",
        (
            "bitscore_tenants_total "
            f"{metrics['tenants_total']}"
        ),
        "# HELP bitscore_usage_events_total "
        "Eventos validos de consumo.",
        "# TYPE bitscore_usage_events_total counter",
        (
            "bitscore_usage_events_total "
            f"{metrics['usage_events_total']}"
        ),
        "# HELP bitscore_subscription_events_total "
        "Eventos de assinatura.",
        "# TYPE bitscore_subscription_events_total counter",
        (
            "bitscore_subscription_events_total "
            f"{metrics['subscription_events_total']}"
        ),
        "# HELP bitscore_risk_alerts_total "
        "Transicoes de risco registradas.",
        "# TYPE bitscore_risk_alerts_total counter",
        (
            "bitscore_risk_alerts_total "
            f"{metrics['risk_alerts_total']}"
        ),
        "# HELP bitscore_notifications_total "
        "Notificacoes por estado.",
        "# TYPE bitscore_notifications_total gauge",
    ]

    for status in [
        "pending",
        "delivered",
        "failed",
    ]:
        lines.append(
            "bitscore_notifications_total"
            f'{{status="{status}"}} '
            f"{metrics['notifications'].get(status, 0)}"
        )

    return "\n".join(lines) + "\n"


def get_database_readiness():
    started_at = time.perf_counter()

    try:
        with connect_database() as connection:
            check = connection.execute(
                "PRAGMA quick_check"
            ).fetchone()[0]

            tenant_count = connection.execute(
                "SELECT COUNT(*) FROM usage_counters"
            ).fetchone()[0]

        ready = check == "ok"

        return {
            "ready": ready,
            "database": "sqlite",
            "check": check,
            "tenant_count": tenant_count,
            "latency_ms": round(
                (time.perf_counter() - started_at) * 1000,
                2,
            ),
        }

    except sqlite3.Error:
        return {
            "ready": False,
            "database": "sqlite",
            "check": "database_error",
            "tenant_count": None,
            "latency_ms": round(
                (time.perf_counter() - started_at) * 1000,
                2,
            ),
        }


def get_platform_summary():
    with connect_database() as connection:
        totals = connection.execute("""
            SELECT
                COUNT(*) AS total_tenants,
                COALESCE(SUM(used), 0) AS total_used,
                COALESCE(SUM(usage_limit), 0)
                    AS total_capacity,
                COALESCE(SUM(
                    CASE
                        WHEN used >= usage_limit
                        THEN 1
                        ELSE 0
                    END
                ), 0) AS blocked_tenants,
                COALESCE(SUM(
                    CASE
                        WHEN used < usage_limit
                         AND (
                            used * 1.0 / usage_limit
                         ) >= 0.8
                        THEN 1
                        ELSE 0
                    END
                ), 0) AS warning_tenants
            FROM usage_counters
        """).fetchone()

        plan_rows = connection.execute("""
            SELECT
                plan,
                COUNT(*) AS total
            FROM usage_counters
            GROUP BY plan
            ORDER BY plan
        """).fetchall()

    total_capacity = totals["total_capacity"]
    total_used = totals["total_used"]

    usage_percentage = (
        round(
            (total_used / total_capacity) * 100,
            1,
        )
        if total_capacity > 0
        else 0.0
    )

    plans = {
        "Start": 0,
        "Growth": 0,
        "Scale": 0,
    }

    for row in plan_rows:
        plans[row["plan"]] = row["total"]

    return {
        "total_tenants": totals["total_tenants"],
        "total_used": total_used,
        "total_capacity": total_capacity,
        "usage_percentage": usage_percentage,
        "warning_tenants": totals["warning_tenants"],
        "blocked_tenants": totals["blocked_tenants"],
        "plans": plans,
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


def get_subscription_summary(
    tenant_id,
    actor_type="",
    date_from="",
    date_to_exclusive="",
):
    parameters = (
        tenant_id,
        actor_type,
        actor_type,
        date_from,
        date_from,
        date_to_exclusive,
        date_to_exclusive,
    )

    filters_sql = """
        WHERE tenant_id = ?
          AND event_type = 'plan_upgraded'
          AND (? = '' OR actor_type = ?)
          AND (? = '' OR created_at >= ?)
          AND (? = '' OR created_at < ?)
    """

    with connect_database() as connection:
        actor_rows = connection.execute(
            f"""
            SELECT actor_type, COUNT(*) AS total
            FROM subscription_events
            {filters_sql}
            GROUP BY actor_type
            """,
            parameters,
        ).fetchall()

        transition_rows = connection.execute(
            f"""
            SELECT
                previous_plan,
                new_plan,
                COUNT(*) AS total
            FROM subscription_events
            {filters_sql}
            GROUP BY previous_plan, new_plan
            ORDER BY
                total DESC,
                previous_plan,
                new_plan
            """,
            parameters,
        ).fetchall()

    actors = {
        "customer": 0,
        "admin": 0,
        "system": 0,
    }

    for row in actor_rows:
        actors[row["actor_type"]] = row["total"]

    transitions = [
        {
            "previous_plan": row["previous_plan"],
            "new_plan": row["new_plan"],
            "total": row["total"],
        }
        for row in transition_rows
    ]

    return {
        "total_upgrades": sum(actors.values()),
        "actors": actors,
        "transitions": transitions,
        "most_common_transition": (
            transitions[0] if transitions else None
        ),
    }


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
        self.request_started_at = time.perf_counter()
        super().__init__(*args, directory="static", **kwargs)

    def get_server_timing(self):
        duration_ms = (
            time.perf_counter()
            - self.request_started_at
        ) * 1000

        return f"app;dur={duration_ms:.2f}"

    def send_observability_headers(self, request_id):
        self.send_header(
            "X-Request-ID",
            request_id,
        )
        self.send_header(
            "Server-Timing",
            self.get_server_timing(),
        )

    def get_request_id(self):
        candidate = self.headers.get(
            "X-Request-ID",
            "",
        ).strip()

        try:
            return str(uuid.UUID(candidate))
        except (ValueError, AttributeError):
            return str(uuid.uuid4())

    def send_json(self, status_code, data):
        request_id = self.get_request_id()
        response = {**data, "request_id": request_id}
        body = json.dumps(response).encode("utf-8")

        self.send_response(status_code)
        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8",
        )
        self.send_header("Content-Length", str(len(body)))
        self.send_observability_headers(
            request_id
        )
        self.end_headers()
        self.wfile.write(body)

    def send_text(
        self,
        status_code,
        content,
        content_type="text/plain; charset=utf-8",
    ):
        request_id = self.get_request_id()
        body = content.encode("utf-8")

        self.send_response(status_code)
        self.send_header(
            "Content-Type",
            content_type,
        )
        self.send_header(
            "Content-Length",
            str(len(body)),
        )
        self.send_observability_headers(
            request_id
        )
        self.end_headers()
        self.wfile.write(body)

    def send_csv(self, filename, content):
        request_id = self.get_request_id()
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
        self.send_observability_headers(
            request_id
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

        if path == "/metrics":
            metrics = get_operational_metrics()
            content = render_prometheus_metrics(metrics)

            self.send_text(
                200,
                content,
                (
                    "text/plain; version=0.0.4; "
                    "charset=utf-8"
                ),
            )
            return

        if path == "/api/ready":
            readiness = get_database_readiness()

            self.send_json(
                200 if readiness["ready"] else 503,
                {
                    "status": (
                        "ready"
                        if readiness["ready"]
                        else "not_ready"
                    ),
                    **readiness,
                },
            )
            return

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


        if path == "/api/platform/summary":
            summary = get_platform_summary()

            self.send_json(200, {
                "scope": "platform",
                "summary": summary,
            })
            return

        if path == "/api/tenants":
            tenants = list_tenants()

            self.send_json(200, {
                "tenants": tenants,
                "total": len(tenants),
            })
            return

        if path == "/api/subscription/summary":
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

            summary = get_subscription_summary(
                tenant_id,
                actor_type,
                date_filters["start_timestamp"],
                date_filters["end_timestamp"],
            )

            self.send_json(200, {
                "tenant_id": tenant_id,
                "filters": {
                    "actor_type": actor_type or None,
                    "date_from": (
                        date_filters["date_from"] or None
                    ),
                    "date_to": (
                        date_filters["date_to"] or None
                    ),
                },
                "summary": summary,
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

        if path == "/api/notifications":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa nao encontrada",
                })
                return

            query = parse_qs(
                urlparse(self.path).query
            )

            status = str(
                query.get("status", ["pending"])[0]
            ).strip().lower()

            if status not in {
                "pending",
                "delivered",
                "all",
            }:
                self.send_json(400, {
                    "error": (
                        "status deve ser pending, "
                        "delivered ou all"
                    ),
                })
                return

            notifications = list_notifications(
                tenant_id,
                status,
            )

            self.send_json(200, {
                "tenant_id": tenant_id,
                "status": status,
                "notifications": notifications,
                "total": len(notifications),
            })
            return

        if path == "/api/usage/alerts":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa nao encontrada",
                })
                return

            query = parse_qs(
                urlparse(self.path).query
            )

            try:
                limit = int(
                    query.get("limit", ["10"])[0]
                )
            except (TypeError, ValueError):
                self.send_json(400, {
                    "error": "Limite de alertas invalido",
                })
                return

            if limit < 1 or limit > 50:
                self.send_json(400, {
                    "error": (
                        "limit deve estar entre 1 e 50"
                    ),
                })
                return

            alerts = list_usage_risk_alerts(
                tenant_id,
                limit,
            )

            self.send_json(200, {
                "tenant_id": tenant_id,
                "alerts": alerts,
                "total": len(alerts),
            })
            return

        if path == "/api/usage/forecast":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa nao encontrada",
                })
                return

            query = parse_qs(
                urlparse(self.path).query
            )

            try:
                window = int(
                    query.get("window", ["7"])[0]
                )
            except (TypeError, ValueError):
                self.send_json(400, {
                    "error": "Janela de previsao invalida",
                })
                return

            if window < 1 or window > 30:
                self.send_json(400, {
                    "error": (
                        "window deve estar entre 1 e 30"
                    ),
                })
                return

            forecast = get_usage_forecast(
                tenant_id,
                window,
            )

            self.send_json(200, {
                "tenant_id": tenant_id,
                "plan": usage["plan"],
                "used": usage["used"],
                "limit": usage["limit"],
                "forecast": forecast,
            })
            return

        if path == "/api/usage/trend":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa nao encontrada",
                })
                return

            query = parse_qs(
                urlparse(self.path).query
            )

            try:
                days = int(
                    query.get("days", ["7"])[0]
                )
            except (TypeError, ValueError):
                self.send_json(400, {
                    "error": "Periodo de tendencia invalido",
                })
                return

            if days < 1 or days > 30:
                self.send_json(400, {
                    "error": (
                        "days deve estar entre 1 e 30"
                    ),
                })
                return

            trend = get_usage_trend(
                tenant_id,
                days,
            )

            self.send_json(200, {
                "tenant_id": tenant_id,
                **trend,
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

        if path == "/api/notifications/dispatch":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa nao encontrada",
                })
                return

            notification = dispatch_next_notification(
                tenant_id
            )

            self.send_json(200, {
                "tenant_id": tenant_id,
                "dispatched": notification is not None,
                "notification": notification,
            })
            return

        if path == "/api/usage/alerts/evaluate":
            usage = get_usage(tenant_id)

            if usage is None:
                self.send_json(404, {
                    "error": "Empresa nao encontrada",
                })
                return

            query = parse_qs(
                urlparse(self.path).query
            )

            try:
                window = int(
                    query.get("window", ["7"])[0]
                )
            except (TypeError, ValueError):
                self.send_json(400, {
                    "error": "Janela de avaliacao invalida",
                })
                return

            if window < 1 or window > 30:
                self.send_json(400, {
                    "error": (
                        "window deve estar entre 1 e 30"
                    ),
                })
                return

            evaluation = evaluate_usage_risk(
                tenant_id,
                window,
            )

            self.send_json(
                201 if evaluation["created"] else 200,
                {
                    "tenant_id": tenant_id,
                    **evaluation,
                },
            )
            return

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
