from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def available_port():
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return server.getsockname()[1]


class TenantApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_directory = tempfile.TemporaryDirectory()
        cls.port = available_port()
        cls.base_url = f"http://127.0.0.1:{cls.port}"

        environment = os.environ.copy()
        environment["BITSCORE_PORT"] = str(cls.port)
        environment["BITSCORE_DATABASE"] = str(
            Path(cls.temp_directory.name) / "test.db"
        )

        cls.process = subprocess.Popen(
            [sys.executable, "app.py"],
            cwd=PROJECT_ROOT,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            text=True,
        )

        for _ in range(50):
            try:
                status, _, _ = cls.request("/api/health")

                if status == 200:
                    return
            except Exception:
                time.sleep(0.1)

        output = ""
        cls.process.terminate()

        raise RuntimeError(
            f"Servidor de teste não iniciou:\n{output}"
        )

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate()

        try:
            cls.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls.process.kill()
            cls.process.wait(timeout=5)

        cls.temp_directory.cleanup()

    @classmethod
    def request(
        cls,
        path,
        method="GET",
        payload=None,
        headers=None,
    ):
        body = None
        request_headers = dict(headers or {})

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            request_headers["Content-Type"] = "application/json"

        request = Request(
            cls.base_url + path,
            data=body,
            headers=request_headers,
            method=method,
        )

        try:
            response = urlopen(request, timeout=3)
        except HTTPError as error:
            response = error

        try:
            status = response.status
            response_headers = response.headers
            raw_body = response.read().decode("utf-8")
        finally:
            response.close()

        return (
            status,
            json.loads(raw_body),
            response_headers,
        )

    @classmethod
    def request_text(cls, path):
        response = urlopen(
            cls.base_url + path,
            timeout=3,
        )

        try:
            status = response.status
            response_headers = response.headers
            content = response.read().decode("utf-8-sig")
        finally:
            response.close()

        return (
            status,
            content,
            response_headers,
        )

    def create_tenant(self, tenant_id, plan="Start", limit=100):
        return self.request(
            "/api/tenants",
            method="POST",
            payload={
                "tenant_id": tenant_id,
                "plan": plan,
                "limit": limit,
            },
        )

    def consume(self, path, idempotency_key=None):
        return self.request(
            path,
            method="POST",
            headers={
                "Idempotency-Key": (
                    idempotency_key or str(uuid.uuid4())
                )
            },
        )

    def test_health_has_request_id(self):
        status, body, headers = self.request("/api/health")

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "online")
        self.assertIn("request_id", body)
        self.assertEqual(
            headers["X-Request-ID"],
            body["request_id"],
        )

    def test_onboarding_rejects_duplicate(self):
        status, body, _ = self.create_tenant(
            "tenant-onboarding",
            plan="Growth",
            limit=500,
        )

        self.assertEqual(status, 201)
        self.assertEqual(
            body["tenant"]["tenant_id"],
            "tenant-onboarding",
        )
        self.assertEqual(body["tenant"]["used"], 0)

        duplicate_status, duplicate_body, _ = (
            self.create_tenant(
                "tenant-onboarding",
                plan="Growth",
                limit=500,
            )
        )

        self.assertEqual(duplicate_status, 409)
        self.assertEqual(
            duplicate_body["error"],
            "Empresa já cadastrada",
        )

    def test_directory_contains_created_tenant(self):
        self.create_tenant(
            "tenant-directory",
            plan="Scale",
            limit=2000,
        )

        status, body, _ = self.request("/api/tenants")

        identifiers = {
            tenant["tenant_id"]
            for tenant in body["tenants"]
        }

        self.assertEqual(status, 200)
        self.assertEqual(
            body["total"],
            len(body["tenants"]),
        )
        self.assertIn("tenant-directory", identifiers)
        self.assertIn("request_id", body)

    def test_usage_is_isolated_by_tenant(self):
        self.create_tenant("tenant-alpha")
        self.create_tenant("tenant-beta")

        consume_status, _, _ = self.consume(
            "/api/usage/consume?tenant_id=tenant-alpha"
        )

        _, alpha, _ = self.request(
            "/api/usage?tenant_id=tenant-alpha"
        )

        _, beta, _ = self.request(
            "/api/usage?tenant_id=tenant-beta"
        )

        self.assertEqual(consume_status, 200)
        self.assertEqual(alpha["used"], 1)
        self.assertEqual(beta["used"], 0)


    def test_usage_starts_without_upgrade_alert(self):
        status, body, _ = self.create_tenant(
            "tenant-alert-normal",
            plan="Start",
            limit=5,
        )

        self.assertEqual(status, 201)
        self.assertEqual(body["tenant"]["alert_level"], "normal")
        self.assertFalse(
            body["tenant"]["upgrade_recommended"]
        )
        self.assertEqual(
            body["tenant"]["recommended_plan"],
            "Growth",
        )

    def test_usage_warns_and_blocks_at_plan_thresholds(self):
        self.create_tenant(
            "tenant-alert-threshold",
            plan="Start",
            limit=5,
        )

        endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-alert-threshold"
        )

        for _ in range(4):
            status, warning, _ = self.consume(
                endpoint
            )

        self.assertEqual(status, 200)
        self.assertEqual(warning["used"], 4)
        self.assertEqual(warning["percentage"], 80.0)
        self.assertEqual(warning["alert_level"], "warning")
        self.assertTrue(warning["upgrade_recommended"])
        self.assertEqual(
            warning["recommended_plan"],
            "Growth",
        )

        blocked_status, blocked, _ = self.consume(
            endpoint
        )

        self.assertEqual(blocked_status, 200)
        self.assertEqual(blocked["used"], 5)
        self.assertEqual(blocked["percentage"], 100.0)
        self.assertEqual(blocked["status"], "blocked")
        self.assertEqual(blocked["alert_level"], "blocked")
        self.assertTrue(blocked["upgrade_recommended"])

        denied_status, denied, _ = self.consume(
            endpoint
        )

        self.assertEqual(denied_status, 403)
        self.assertEqual(
            denied["usage"]["alert_level"],
            "blocked",
        )


    def test_upgrade_changes_plan_and_limit(self):
        self.create_tenant(
            "tenant-plan-upgrade",
            plan="Start",
            limit=100,
        )

        status, body, _ = self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-plan-upgrade"
            ),
            method="POST",
            payload={"plan": "Growth"},
        )

        tenant = body["tenant"]

        self.assertEqual(status, 200)
        self.assertEqual(tenant["plan"], "Growth")
        self.assertEqual(tenant["limit"], 500)
        self.assertEqual(tenant["used"], 0)
        self.assertEqual(
            tenant["recommended_plan"],
            "Scale",
        )

    def test_upgrade_rejects_plan_skipping(self):
        self.create_tenant(
            "tenant-plan-skip",
            plan="Start",
            limit=100,
        )

        status, body, _ = self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-plan-skip"
            ),
            method="POST",
            payload={"plan": "Scale"},
        )

        self.assertEqual(status, 400)
        self.assertIn("Growth", body["error"])


    def test_upgrade_creates_subscription_audit_event(self):
        self.create_tenant(
            "tenant-audit-event",
            plan="Start",
            limit=100,
        )

        upgrade_status, _, _ = self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-audit-event"
            ),
            method="POST",
            payload={
                "plan": "Growth",
                "actor_type": "admin",
                "actor_id": "wildson-ricardo",
            },
        )

        status, body, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-audit-event"
            )
        )

        self.assertEqual(upgrade_status, 200)
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 1)

        event = body["events"][0]

        self.assertEqual(
            event["tenant_id"],
            "tenant-audit-event",
        )
        self.assertEqual(
            event["event_type"],
            "plan_upgraded",
        )
        self.assertEqual(
            event["previous_plan"],
            "Start",
        )
        self.assertEqual(
            event["new_plan"],
            "Growth",
        )
        self.assertEqual(event["previous_limit"], 100)
        self.assertEqual(event["new_limit"], 500)
        self.assertEqual(event["actor_type"], "admin")
        self.assertEqual(
            event["actor_id"],
            "wildson-ricardo",
        )
        self.assertIn("created_at", event)

    def test_subscription_audit_is_isolated_by_tenant(self):
        self.create_tenant("tenant-audit-alpha")
        self.create_tenant("tenant-audit-beta")

        self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-audit-alpha"
            ),
            method="POST",
            payload={"plan": "Growth"},
        )

        alpha_status, alpha, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-audit-alpha"
            )
        )

        beta_status, beta, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-audit-beta"
            )
        )

        self.assertEqual(alpha_status, 200)
        self.assertEqual(beta_status, 200)
        self.assertEqual(alpha["total"], 1)
        self.assertEqual(beta["total"], 0)


    def test_upgrade_rejects_invalid_actor_type(self):
        self.create_tenant(
            "tenant-invalid-actor",
            plan="Start",
            limit=100,
        )

        status, body, _ = self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-invalid-actor"
            ),
            method="POST",
            payload={
                "plan": "Growth",
                "actor_type": "unknown",
                "actor_id": "invalid-user",
            },
        )

        self.assertEqual(status, 400)
        self.assertEqual(
            body["error"],
            "Tipo de responsavel invalido",
        )

        _, audit, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-invalid-actor"
            )
        )

        self.assertEqual(audit["total"], 0)


    def test_consume_requires_idempotency_key(self):
        self.create_tenant("tenant-key-required")

        status, body, _ = self.request(
            (
                "/api/usage/consume"
                "?tenant_id=tenant-key-required"
            ),
            method="POST",
        )

        self.assertEqual(status, 400)
        self.assertIn("Idempotency-Key", body["error"])

    def test_repeated_key_does_not_duplicate_usage(self):
        self.create_tenant("tenant-idempotent")
        endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-idempotent"
        )
        key = "same-operation-key"

        first_status, first, _ = self.consume(
            endpoint,
            key,
        )
        second_status, second, _ = self.consume(
            endpoint,
            key,
        )

        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 200)
        self.assertEqual(first["used"], 1)
        self.assertEqual(second["used"], 1)
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])

    def test_idempotency_key_is_isolated_by_tenant(self):
        self.create_tenant("tenant-key-alpha")
        self.create_tenant("tenant-key-beta")
        key = "shared-key"

        _, alpha, _ = self.consume(
            (
                "/api/usage/consume"
                "?tenant_id=tenant-key-alpha"
            ),
            key,
        )
        _, beta, _ = self.consume(
            (
                "/api/usage/consume"
                "?tenant_id=tenant-key-beta"
            ),
            key,
        )

        self.assertEqual(alpha["used"], 1)
        self.assertEqual(beta["used"], 1)
        self.assertFalse(alpha["idempotent_replay"])
        self.assertFalse(beta["idempotent_replay"])


    def create_two_subscription_events(self, tenant_id):
        self.create_tenant(
            tenant_id,
            plan="Start",
            limit=100,
        )

        first_status, _, _ = self.request(
            (
                "/api/usage/upgrade"
                f"?tenant_id={tenant_id}"
            ),
            method="POST",
            payload={"plan": "Growth"},
        )

        second_status, _, _ = self.request(
            (
                "/api/usage/upgrade"
                f"?tenant_id={tenant_id}"
            ),
            method="POST",
            payload={"plan": "Scale"},
        )

        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 200)

    def test_subscription_events_uses_default_pagination(self):
        self.create_tenant("tenant-page-default")

        self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-page-default"
            ),
            method="POST",
            payload={"plan": "Growth"},
        )

        status, body, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-page-default"
            )
        )

        pagination = body["pagination"]

        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 1)
        self.assertEqual(pagination["limit"], 10)
        self.assertEqual(pagination["offset"], 0)
        self.assertEqual(pagination["returned"], 1)
        self.assertEqual(pagination["total"], 1)
        self.assertFalse(pagination["has_more"])
        self.assertIsNone(pagination["next_offset"])

    def test_subscription_events_returns_next_page(self):
        self.create_two_subscription_events(
            "tenant-page-next"
        )

        first_status, first, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-page-next"
                "&limit=1&offset=0"
            )
        )

        second_status, second, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-page-next"
                "&limit=1&offset=1"
            )
        )

        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 200)

        self.assertEqual(first["total"], 2)
        self.assertEqual(len(first["events"]), 1)
        self.assertTrue(
            first["pagination"]["has_more"]
        )
        self.assertEqual(
            first["pagination"]["next_offset"],
            1,
        )

        self.assertEqual(second["total"], 2)
        self.assertEqual(len(second["events"]), 1)
        self.assertFalse(
            second["pagination"]["has_more"]
        )
        self.assertIsNone(
            second["pagination"]["next_offset"]
        )

        self.assertNotEqual(
            first["events"][0]["id"],
            second["events"][0]["id"],
        )

    def test_subscription_events_rejects_invalid_pagination(self):
        self.create_tenant("tenant-page-invalid")

        invalid_queries = [
            "limit=0&offset=0",
            "limit=51&offset=0",
            "limit=abc&offset=0",
            "limit=10&offset=-1",
        ]

        for query in invalid_queries:
            with self.subTest(query=query):
                status, body, _ = self.request(
                    (
                        "/api/subscription/events"
                        "?tenant_id=tenant-page-invalid"
                        f"&{query}"
                    )
                )

                self.assertEqual(status, 400)
                self.assertIn(
                    "Paginacao",
                    body["error"].capitalize(),
                )

    def test_subscription_pagination_remains_tenant_isolated(self):
        self.create_two_subscription_events(
            "tenant-page-alpha"
        )

        self.create_tenant("tenant-page-beta")

        self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-page-beta"
            ),
            method="POST",
            payload={"plan": "Growth"},
        )

        _, alpha, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-page-alpha"
                "&limit=1&offset=0"
            )
        )

        _, beta, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-page-beta"
                "&limit=1&offset=0"
            )
        )

        self.assertEqual(
            alpha["pagination"]["total"],
            2,
        )
        self.assertEqual(
            beta["pagination"]["total"],
            1,
        )

        self.assertEqual(
            alpha["events"][0]["tenant_id"],
            "tenant-page-alpha",
        )
        self.assertEqual(
            beta["events"][0]["tenant_id"],
            "tenant-page-beta",
        )


    def test_subscription_events_can_be_exported_as_csv(self):
        self.create_tenant("tenant-export")

        upgrade_status, _, _ = self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-export"
            ),
            method="POST",
            payload={"plan": "Growth"},
        )

        status, content, headers = self.request_text(
            (
                "/api/subscription/events/export"
                "?tenant_id=tenant-export"
            )
        )

        self.assertEqual(upgrade_status, 200)
        self.assertEqual(status, 200)
        self.assertIn(
            "text/csv",
            headers["Content-Type"],
        )
        self.assertIn(
            "subscription-events-tenant-export.csv",
            headers["Content-Disposition"],
        )
        self.assertIn(
            "X-Request-ID",
            headers,
        )
        self.assertIn(
            "tenant_id,event_type",
            content,
        )
        self.assertIn(
            "tenant-export",
            content,
        )
        self.assertIn(
            "plan_upgraded",
            content,
        )

    def test_csv_export_remains_tenant_isolated(self):
        self.create_tenant("tenant-export-alpha")
        self.create_tenant("tenant-export-beta")

        self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-export-alpha"
            ),
            method="POST",
            payload={"plan": "Growth"},
        )

        status, content, _ = self.request_text(
            (
                "/api/subscription/events/export"
                "?tenant_id=tenant-export-beta"
            )
        )

        self.assertEqual(status, 200)
        self.assertNotIn(
            "tenant-export-alpha",
            content,
        )
        self.assertIn(
            "tenant_id,event_type",
            content,
        )


    def test_subscription_events_filters_actor_type(self):
        self.create_two_subscription_events(
            "tenant-actor-filter"
        )

        status, body, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-actor-filter"
                "&actor_type=customer"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 2)
        self.assertEqual(
            body["filters"]["actor_type"],
            "customer",
        )
        self.assertTrue(
            all(
                event["actor_type"] == "customer"
                for event in body["events"]
            )
        )

    def test_subscription_events_rejects_invalid_actor_filter(self):
        self.create_tenant("tenant-filter-invalid")

        status, body, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-filter-invalid"
                "&actor_type=unknown"
            )
        )

        self.assertEqual(status, 400)
        self.assertIn("actor_type", body["error"])

    def test_csv_export_respects_actor_filter(self):
        self.create_tenant("tenant-export-filter")

        self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-export-filter"
            ),
            method="POST",
            payload={
                "plan": "Growth",
                "actor_type": "admin",
                "actor_id": "csv-admin",
            },
        )

        self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-export-filter"
            ),
            method="POST",
            payload={
                "plan": "Scale",
                "actor_type": "customer",
                "actor_id": "csv-customer",
            },
        )

        status, content, _ = self.request_text(
            (
                "/api/subscription/events/export"
                "?tenant_id=tenant-export-filter"
                "&actor_type=admin"
            )
        )

        self.assertEqual(status, 200)
        self.assertIn("csv-admin", content)
        self.assertNotIn("csv-customer", content)


    def test_subscription_events_filters_inclusive_date_range(self):
        self.create_two_subscription_events(
            "tenant-date-range"
        )

        today = (
            datetime.now(timezone.utc)
            .date()
            .isoformat()
        )

        status, body, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-date-range"
                f"&date_from={today}"
                f"&date_to={today}"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 2)
        self.assertEqual(
            body["filters"]["date_from"],
            today,
        )
        self.assertEqual(
            body["filters"]["date_to"],
            today,
        )

    def test_subscription_events_excludes_outside_period(self):
        self.create_two_subscription_events(
            "tenant-date-future"
        )

        status, body, _ = self.request(
            (
                "/api/subscription/events"
                "?tenant_id=tenant-date-future"
                "&date_from=2099-01-01"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 0)
        self.assertEqual(body["events"], [])

    def test_subscription_events_rejects_invalid_period(self):
        self.create_tenant("tenant-date-invalid")

        invalid_queries = [
            "date_from=2026-99-01",
            "date_to=02-09-2026",
            (
                "date_from=2026-09-10"
                "&date_to=2026-09-01"
            ),
        ]

        for query in invalid_queries:
            with self.subTest(query=query):
                status, body, _ = self.request(
                    (
                        "/api/subscription/events"
                        "?tenant_id=tenant-date-invalid"
                        f"&{query}"
                    )
                )

                self.assertEqual(status, 400)
                self.assertIn(
                    "Periodo invalido",
                    body["error"],
                )

    def test_csv_export_respects_date_range(self):
        self.create_two_subscription_events(
            "tenant-csv-date"
        )

        status, content, _ = self.request_text(
            (
                "/api/subscription/events/export"
                "?tenant_id=tenant-csv-date"
                "&date_from=2099-01-01"
            )
        )

        self.assertEqual(status, 200)
        self.assertIn(
            "tenant_id,event_type",
            content,
        )
        self.assertNotIn(
            "tenant-csv-date",
            content,
        )


if __name__ == "__main__":
    unittest.main()
