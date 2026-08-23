import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
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
            stdout=subprocess.PIPE,
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

        output = cls.process.stdout.read()
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
    def request(cls, path, method="GET", payload=None):
        body = None
        headers = {}

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(
            cls.base_url + path,
            data=body,
            headers=headers,
            method=method,
        )

        try:
            response = urlopen(request, timeout=3)
        except HTTPError as error:
            response = error

        raw_body = response.read().decode("utf-8")

        return (
            response.status,
            json.loads(raw_body),
            response.headers,
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

        consume_status, _, _ = self.request(
            "/api/usage/consume?tenant_id=tenant-alpha",
            method="POST",
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
            status, warning, _ = self.request(
                endpoint,
                method="POST",
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

        blocked_status, blocked, _ = self.request(
            endpoint,
            method="POST",
        )

        self.assertEqual(blocked_status, 200)
        self.assertEqual(blocked["used"], 5)
        self.assertEqual(blocked["percentage"], 100.0)
        self.assertEqual(blocked["status"], "blocked")
        self.assertEqual(blocked["alert_level"], "blocked")
        self.assertTrue(blocked["upgrade_recommended"])

        denied_status, denied, _ = self.request(
            endpoint,
            method="POST",
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
            payload={"plan": "Growth"},
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


if __name__ == "__main__":
    unittest.main()
