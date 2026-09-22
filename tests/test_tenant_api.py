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


    def test_dashboard_has_responsive_audit_filters(self):
        status, content, headers = self.request_text("/")

        self.assertEqual(status, 200)
        self.assertIn(
            "text/html",
            headers["Content-Type"],
        )

        required_elements = [
            'class="audit-filters"',
            'id="auditActorFilter"',
            'id="auditDateFrom"',
            'id="auditDateTo"',
            'id="applyAuditFilter"',
            'id="clearAuditFilter"',
            'class="audit-history-actions"',
            'aria-live="polite"',
        ]

        for element in required_elements:
            with self.subTest(element=element):
                self.assertIn(element, content)

        self.assertIn(
            'querySelector("#clearAuditFilter")',
            content,
        )
        self.assertIn(
            "color-scheme: dark",
            content,
        )


    def test_subscription_summary_counts_actors_and_transitions(self):
        self.create_tenant(
            "tenant-summary",
            plan="Start",
            limit=100,
        )

        self.request(
            "/api/usage/upgrade?tenant_id=tenant-summary",
            method="POST",
            payload={
                "plan": "Growth",
                "actor_type": "admin",
                "actor_id": "summary-admin",
            },
        )

        self.request(
            "/api/usage/upgrade?tenant_id=tenant-summary",
            method="POST",
            payload={
                "plan": "Scale",
                "actor_type": "customer",
                "actor_id": "summary-customer",
            },
        )

        status, body, _ = self.request(
            (
                "/api/subscription/summary"
                "?tenant_id=tenant-summary"
            )
        )

        summary = body["summary"]

        self.assertEqual(status, 200)
        self.assertEqual(summary["total_upgrades"], 2)
        self.assertEqual(summary["actors"]["admin"], 1)
        self.assertEqual(summary["actors"]["customer"], 1)
        self.assertEqual(summary["actors"]["system"], 0)

        transitions = {
            (
                item["previous_plan"],
                item["new_plan"],
            ): item["total"]
            for item in summary["transitions"]
        }

        self.assertEqual(
            transitions[("Start", "Growth")],
            1,
        )
        self.assertEqual(
            transitions[("Growth", "Scale")],
            1,
        )

    def test_subscription_summary_respects_actor_filter(self):
        self.create_two_subscription_events(
            "tenant-summary-filter"
        )

        status, body, _ = self.request(
            (
                "/api/subscription/summary"
                "?tenant_id=tenant-summary-filter"
                "&actor_type=customer"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            body["filters"]["actor_type"],
            "customer",
        )
        self.assertEqual(
            body["summary"]["total_upgrades"],
            2,
        )
        self.assertEqual(
            body["summary"]["actors"]["admin"],
            0,
        )

    def test_subscription_summary_respects_date_range(self):
        self.create_two_subscription_events(
            "tenant-summary-date"
        )

        status, body, _ = self.request(
            (
                "/api/subscription/summary"
                "?tenant_id=tenant-summary-date"
                "&date_from=2099-01-01"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            body["summary"]["total_upgrades"],
            0,
        )
        self.assertEqual(
            body["summary"]["transitions"],
            [],
        )
        self.assertIsNone(
            body["summary"]["most_common_transition"]
        )

    def test_subscription_summary_remains_tenant_isolated(self):
        self.create_two_subscription_events(
            "tenant-summary-alpha"
        )
        self.create_tenant("tenant-summary-beta")

        status, body, _ = self.request(
            (
                "/api/subscription/summary"
                "?tenant_id=tenant-summary-beta"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            body["summary"]["total_upgrades"],
            0,
        )


    def test_usage_trend_returns_seven_zero_filled_days(self):
        self.create_tenant("tenant-trend-seven")

        self.consume(
            (
                "/api/usage/consume"
                "?tenant_id=tenant-trend-seven"
            )
        )

        status, body, _ = self.request(
            (
                "/api/usage/trend"
                "?tenant_id=tenant-trend-seven"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(body["days"], 7)
        self.assertEqual(len(body["trend"]), 7)
        self.assertEqual(body["total_consumed"], 1)
        self.assertEqual(
            sum(
                point["amount"]
                for point in body["trend"]
            ),
            1,
        )

    def test_usage_trend_ignores_idempotent_replay(self):
        self.create_tenant("tenant-trend-replay")

        endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-trend-replay"
        )

        self.consume(endpoint, "trend-repeated-key")
        self.consume(endpoint, "trend-repeated-key")

        status, body, _ = self.request(
            (
                "/api/usage/trend"
                "?tenant_id=tenant-trend-replay"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(body["total_consumed"], 1)

    def test_usage_trend_rejects_invalid_days(self):
        self.create_tenant("tenant-trend-invalid")

        for days in ["0", "31", "invalid"]:
            with self.subTest(days=days):
                status, body, _ = self.request(
                    (
                        "/api/usage/trend"
                        "?tenant_id=tenant-trend-invalid"
                        f"&days={days}"
                    )
                )

                self.assertEqual(status, 400)
                self.assertIn(
                    "invalido"
                    if days == "invalid"
                    else "entre 1 e 30",
                    body["error"],
                )

    def test_usage_trend_remains_tenant_isolated(self):
        self.create_tenant("tenant-trend-alpha")
        self.create_tenant("tenant-trend-beta")

        self.consume(
            (
                "/api/usage/consume"
                "?tenant_id=tenant-trend-alpha"
            )
        )

        status, body, _ = self.request(
            (
                "/api/usage/trend"
                "?tenant_id=tenant-trend-beta"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(body["total_consumed"], 0)
        self.assertTrue(
            all(
                point["amount"] == 0
                for point in body["trend"]
            )
        )


    def test_usage_forecast_handles_insufficient_data(self):
        self.create_tenant("tenant-forecast-empty")

        status, body, _ = self.request(
            (
                "/api/usage/forecast"
                "?tenant_id=tenant-forecast-empty"
            )
        )

        forecast = body["forecast"]

        self.assertEqual(status, 200)
        self.assertEqual(
            forecast["risk_level"],
            "insufficient_data",
        )
        self.assertIsNone(forecast["days_to_limit"])
        self.assertIsNone(
            forecast["projected_exhaustion_date"]
        )

    def test_usage_forecast_calculates_days_to_limit(self):
        self.create_tenant(
            "tenant-forecast-calculation",
            limit=10,
        )

        endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-forecast-calculation"
        )

        self.consume(endpoint)
        self.consume(endpoint)

        status, body, _ = self.request(
            (
                "/api/usage/forecast"
                "?tenant_id=tenant-forecast-calculation"
                "&window=1"
            )
        )

        forecast = body["forecast"]

        self.assertEqual(status, 200)
        self.assertEqual(forecast["remaining"], 8)
        self.assertEqual(forecast["daily_rate"], 2.0)
        self.assertEqual(forecast["days_to_limit"], 4)
        self.assertEqual(
            forecast["risk_level"],
            "warning",
        )
        self.assertIsNotNone(
            forecast["projected_exhaustion_date"]
        )

    def test_usage_forecast_rejects_invalid_window(self):
        self.create_tenant("tenant-forecast-invalid")

        for window in ["0", "31", "invalid"]:
            with self.subTest(window=window):
                status, body, _ = self.request(
                    (
                        "/api/usage/forecast"
                        "?tenant_id=tenant-forecast-invalid"
                        f"&window={window}"
                    )
                )

                self.assertEqual(status, 400)
                self.assertIn(
                    "invalida"
                    if window == "invalid"
                    else "entre 1 e 30",
                    body["error"],
                )

    def test_usage_forecast_remains_tenant_isolated(self):
        self.create_tenant("tenant-forecast-alpha")
        self.create_tenant("tenant-forecast-beta")

        self.consume(
            (
                "/api/usage/consume"
                "?tenant_id=tenant-forecast-alpha"
            )
        )

        status, body, _ = self.request(
            (
                "/api/usage/forecast"
                "?tenant_id=tenant-forecast-beta"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            body["forecast"]["risk_level"],
            "insufficient_data",
        )
        self.assertEqual(
            body["forecast"]["daily_rate"],
            0.0,
        )


    def test_risk_evaluation_creates_warning_transition(self):
        self.create_tenant(
            "tenant-risk-warning",
            limit=10,
        )

        endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-risk-warning"
        )

        self.consume(endpoint)
        self.consume(endpoint)

        status, body, _ = self.request(
            (
                "/api/usage/alerts/evaluate"
                "?tenant_id=tenant-risk-warning"
                "&window=1"
            ),
            method="POST",
        )

        self.assertEqual(status, 201)
        self.assertTrue(body["created"])
        self.assertEqual(
            body["alert"]["risk_level"],
            "warning",
        )
        self.assertIsNone(
            body["alert"]["previous_risk_level"]
        )

    def test_risk_evaluation_does_not_duplicate_state(self):
        self.create_tenant(
            "tenant-risk-deduplication",
            limit=10,
        )

        consume_endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-risk-deduplication"
        )

        self.consume(consume_endpoint)
        self.consume(consume_endpoint)

        evaluate_endpoint = (
            "/api/usage/alerts/evaluate"
            "?tenant_id=tenant-risk-deduplication"
            "&window=1"
        )

        first_status, first, _ = self.request(
            evaluate_endpoint,
            method="POST",
        )
        second_status, second, _ = self.request(
            evaluate_endpoint,
            method="POST",
        )

        _, history, _ = self.request(
            (
                "/api/usage/alerts"
                "?tenant_id=tenant-risk-deduplication"
            )
        )

        self.assertEqual(first_status, 201)
        self.assertEqual(second_status, 200)
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(history["total"], 1)

    def test_risk_evaluation_records_recovery_after_upgrade(self):
        self.create_tenant(
            "tenant-risk-recovery",
            limit=10,
        )

        consume_endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-risk-recovery"
        )

        self.consume(consume_endpoint)
        self.consume(consume_endpoint)

        self.request(
            (
                "/api/usage/alerts/evaluate"
                "?tenant_id=tenant-risk-recovery"
                "&window=1"
            ),
            method="POST",
        )

        self.request(
            (
                "/api/usage/upgrade"
                "?tenant_id=tenant-risk-recovery"
            ),
            method="POST",
            payload={"plan": "Growth"},
        )

        status, body, _ = self.request(
            (
                "/api/usage/alerts/evaluate"
                "?tenant_id=tenant-risk-recovery"
                "&window=1"
            ),
            method="POST",
        )

        self.assertEqual(status, 201)
        self.assertEqual(
            body["alert"]["previous_risk_level"],
            "warning",
        )
        self.assertEqual(
            body["alert"]["risk_level"],
            "stable",
        )

    def test_risk_alert_history_remains_tenant_isolated(self):
        self.create_tenant(
            "tenant-risk-alpha",
            limit=10,
        )
        self.create_tenant("tenant-risk-beta")

        endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-risk-alpha"
        )

        self.consume(endpoint)
        self.consume(endpoint)

        self.request(
            (
                "/api/usage/alerts/evaluate"
                "?tenant_id=tenant-risk-alpha"
                "&window=1"
            ),
            method="POST",
        )

        status, body, _ = self.request(
            (
                "/api/usage/alerts"
                "?tenant_id=tenant-risk-beta"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 0)
        self.assertEqual(body["alerts"], [])


    def test_readiness_checks_sqlite_database(self):
        status, body, _ = self.request("/api/ready")

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ready")
        self.assertTrue(body["ready"])
        self.assertEqual(body["database"], "sqlite")
        self.assertEqual(body["check"], "ok")
        self.assertIsInstance(
            body["tenant_count"],
            int,
        )
        self.assertGreaterEqual(
            body["latency_ms"],
            0,
        )

    def test_readiness_has_request_id(self):
        status, body, headers = self.request(
            "/api/ready"
        )

        self.assertEqual(status, 200)
        self.assertIn("request_id", body)
        self.assertEqual(
            headers["X-Request-ID"],
            body["request_id"],
        )


    def test_dashboard_has_multi_tenant_switcher(self):
        status, content, _ = self.request_text("/")

        self.assertEqual(status, 200)
        self.assertIn(
            'id="tenantSelector"',
            content,
        )
        self.assertIn(
            "function tenantApiUrl(",
            content,
        )
        self.assertIn(
            '"tenant_id"',
            content,
        )
        self.assertIn(
            "async function loadTenantDirectory()",
            content,
        )
        self.assertIn(
            "async function refreshTenantDashboard()",
            content,
        )


    def test_platform_summary_counts_tenants_and_plans(self):
        _, before, _ = self.request(
            "/api/platform/summary"
        )

        before_summary = before["summary"]

        self.create_tenant(
            "tenant-platform-start",
            plan="Start",
            limit=100,
        )
        self.create_tenant(
            "tenant-platform-growth",
            plan="Growth",
            limit=500,
        )

        status, body, headers = self.request(
            "/api/platform/summary"
        )

        summary = body["summary"]

        self.assertEqual(status, 200)
        self.assertEqual(body["scope"], "platform")
        self.assertEqual(
            summary["total_tenants"],
            before_summary["total_tenants"] + 2,
        )
        self.assertEqual(
            summary["plans"]["Start"],
            before_summary["plans"]["Start"] + 1,
        )
        self.assertEqual(
            summary["plans"]["Growth"],
            before_summary["plans"]["Growth"] + 1,
        )
        self.assertEqual(
            headers["X-Request-ID"],
            body["request_id"],
        )

    def test_platform_summary_counts_risk_levels(self):
        _, before, _ = self.request(
            "/api/platform/summary"
        )

        before_summary = before["summary"]

        self.create_tenant(
            "tenant-platform-warning",
            limit=5,
        )
        self.create_tenant(
            "tenant-platform-blocked",
            limit=2,
        )

        warning_endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-platform-warning"
        )
        blocked_endpoint = (
            "/api/usage/consume"
            "?tenant_id=tenant-platform-blocked"
        )

        for _ in range(4):
            self.consume(warning_endpoint)

        for _ in range(2):
            self.consume(blocked_endpoint)

        status, body, _ = self.request(
            "/api/platform/summary"
        )

        summary = body["summary"]

        self.assertEqual(status, 200)
        self.assertEqual(
            summary["warning_tenants"],
            before_summary["warning_tenants"] + 1,
        )
        self.assertEqual(
            summary["blocked_tenants"],
            before_summary["blocked_tenants"] + 1,
        )
        self.assertEqual(
            summary["total_used"],
            before_summary["total_used"] + 6,
        )
        self.assertEqual(
            summary["total_capacity"],
            before_summary["total_capacity"] + 7,
        )


    def create_outbox_notification(self, tenant_id):
        self.create_tenant(
            tenant_id,
            limit=10,
        )

        endpoint = (
            "/api/usage/consume"
            f"?tenant_id={tenant_id}"
        )

        self.consume(endpoint)
        self.consume(endpoint)

        return self.request(
            (
                "/api/usage/alerts/evaluate"
                f"?tenant_id={tenant_id}"
                "&window=1"
            ),
            method="POST",
        )

    def test_risk_transition_creates_pending_notification(self):
        tenant_id = "tenant-outbox-created"

        status, evaluation, _ = (
            self.create_outbox_notification(
                tenant_id
            )
        )

        _, body, _ = self.request(
            (
                "/api/notifications"
                f"?tenant_id={tenant_id}"
                "&status=pending"
            )
        )

        self.assertEqual(status, 201)
        self.assertTrue(evaluation["created"])
        self.assertEqual(body["total"], 1)
        self.assertEqual(
            body["notifications"][0]["status"],
            "pending",
        )
        self.assertEqual(
            body["notifications"][0]["event_type"],
            "usage_risk_changed",
        )

    def test_repeated_risk_does_not_duplicate_notification(self):
        tenant_id = "tenant-outbox-deduplication"
        self.create_outbox_notification(tenant_id)

        self.request(
            (
                "/api/usage/alerts/evaluate"
                f"?tenant_id={tenant_id}"
                "&window=1"
            ),
            method="POST",
        )

        _, body, _ = self.request(
            (
                "/api/notifications"
                f"?tenant_id={tenant_id}"
                "&status=all"
            )
        )

        self.assertEqual(body["total"], 1)

    def test_pending_notification_can_be_dispatched(self):
        tenant_id = "tenant-outbox-dispatch"
        self.create_outbox_notification(tenant_id)

        status, body, _ = self.request(
            (
                "/api/notifications/dispatch"
                f"?tenant_id={tenant_id}"
            ),
            method="POST",
        )

        _, pending, _ = self.request(
            (
                "/api/notifications"
                f"?tenant_id={tenant_id}"
                "&status=pending"
            )
        )

        _, delivered, _ = self.request(
            (
                "/api/notifications"
                f"?tenant_id={tenant_id}"
                "&status=delivered"
            )
        )

        self.assertEqual(status, 200)
        self.assertTrue(body["dispatched"])
        self.assertEqual(
            body["notification"]["status"],
            "delivered",
        )
        self.assertEqual(
            body["notification"]["attempts"],
            1,
        )
        self.assertIsNotNone(
            body["notification"]["delivered_at"]
        )
        self.assertEqual(pending["total"], 0)
        self.assertEqual(delivered["total"], 1)

    def test_notification_outbox_remains_tenant_isolated(self):
        self.create_outbox_notification(
            "tenant-outbox-alpha"
        )
        self.create_tenant("tenant-outbox-beta")

        status, body, _ = self.request(
            (
                "/api/notifications"
                "?tenant_id=tenant-outbox-beta"
                "&status=all"
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 0)
        self.assertEqual(body["notifications"], [])


    def test_metrics_exposes_prometheus_format(self):
        status, content, headers = self.request_text(
            "/metrics"
        )

        self.assertEqual(status, 200)
        self.assertIn(
            "text/plain",
            headers["Content-Type"],
        )
        self.assertIn(
            "version=0.0.4",
            headers["Content-Type"],
        )
        self.assertIn(
            "X-Request-ID",
            headers,
        )
        self.assertIn(
            "# TYPE bitscore_tenants_total gauge",
            content,
        )
        self.assertIn(
            "bitscore_usage_events_total",
            content,
        )
        self.assertIn(
            'bitscore_notifications_total{status="pending"}',
            content,
        )

    def test_metrics_reflects_activity_without_tenant_ids(self):
        def metric_value(content, metric):
            line = next(
                item
                for item in content.splitlines()
                if item.startswith(f"{metric} ")
            )

            return float(line.split()[-1])

        _, before, _ = self.request_text("/metrics")

        tenant_id = "tenant-metrics-private"
        self.create_tenant(tenant_id)

        self.consume(
            (
                "/api/usage/consume"
                f"?tenant_id={tenant_id}"
            )
        )

        _, after, _ = self.request_text("/metrics")

        self.assertEqual(
            metric_value(
                after,
                "bitscore_tenants_total",
            ),
            metric_value(
                before,
                "bitscore_tenants_total",
            ) + 1,
        )
        self.assertEqual(
            metric_value(
                after,
                "bitscore_usage_events_total",
            ),
            metric_value(
                before,
                "bitscore_usage_events_total",
            ) + 1,
        )
        self.assertNotIn(tenant_id, after)

    def test_dashboard_has_operational_metrics_panel(self):
        dashboard = (
            PROJECT_ROOT
            / "static"
            / "index.html"
        ).read_text(encoding="utf-8")

        required_markers = [
            'id="metricsTenantTotal"',
            'id="metricsEventTotal"',
            'id="metricsPendingTotal"',
            "async function loadOperationalMetrics()",
            'fetch("/metrics")',
        ]

        for marker in required_markers:
            with self.subTest(marker=marker):
                self.assertIn(marker, dashboard)


    def test_valid_request_id_is_propagated(self):
        request_id = str(uuid.uuid4())

        status, body, headers = self.request(
            "/api/health",
            headers={
                "X-Request-ID": request_id,
            },
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            body["request_id"],
            request_id,
        )
        self.assertEqual(
            headers["X-Request-ID"],
            request_id,
        )

    def test_invalid_request_id_is_replaced(self):
        malicious_value = "<script>alert(1)</script>"

        status, body, headers = self.request(
            "/api/health",
            headers={
                "X-Request-ID": malicious_value,
            },
        )

        generated = body["request_id"]

        self.assertEqual(status, 200)
        self.assertNotEqual(
            generated,
            malicious_value,
        )
        self.assertEqual(
            headers["X-Request-ID"],
            generated,
        )

        parsed = uuid.UUID(generated)

        self.assertEqual(
            str(parsed),
            generated,
        )

    def test_dashboard_initiates_request_tracing(self):
        dashboard = (
            PROJECT_ROOT
            / "static"
            / "index.html"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "crypto.randomUUID()",
            dashboard,
        )
        self.assertIn(
            '"X-Request-ID": requestId',
            dashboard,
        )
        self.assertIn(
            "propagado pelo frontend",
            dashboard,
        )


if __name__ == "__main__":
    unittest.main()
