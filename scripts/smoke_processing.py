"""Run isolated Docker acceptance checks, including broker restart and persistent delivery."""

import json
import os
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from textwrap import dedent
from uuid import uuid4


def run_smoke(source: Path, artifacts: Path, project: str) -> None:
    environment = artifacts / "smoke.env"
    if not environment.exists():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        password, rabbit_password, key = (secrets.token_hex(20) for _ in range(3))
        environment.write_text(
            f"API_KEY={key}\nPOSTGRES_USER=payments\nPOSTGRES_PASSWORD={password}\n"
            f"POSTGRES_DB=payments\nDATABASE_URL=postgresql+asyncpg://payments:{password}@postgres:5432/payments\n"
            f"API_PORT={port}\nRABBITMQ_USER=payments\nRABBITMQ_PASSWORD={rabbit_password}\n"
            f"RABBITMQ_VHOST=payments\nRABBITMQ_URL=amqp://payments:{rabbit_password}@rabbitmq:5672/payments\n"
            "OUTBOX_POLL_INTERVAL=0.2\nOUTBOX_PUBLISH_TIMEOUT=1\n",
            encoding="utf-8",
        )
    cfg = dict(line.split("=", 1) for line in environment.read_text().splitlines())
    compose_env = {**os.environ, **cfg}
    base = "http://127.0.0.1:" + cfg["API_PORT"]
    prefix = [
        "docker",
        "compose",
        "-p",
        project,
        "--env-file",
        str(environment),
        "-f",
        str(source / "compose.yaml"),
    ]
    log = (artifacts / "compose-smoke.log").open("w", encoding="utf-8")

    def compose(*command):
        result = subprocess.run(
            prefix + list(command),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=compose_env,
        )
        log.write("COMMAND: " + " ".join(command[:5]) + "\n" + result.stdout + result.stderr + "\n")
        log.flush()
        if result.returncode:
            details = (result.stdout + result.stderr)[-4000:]
            for name in (
                "DATABASE_URL",
                "RABBITMQ_URL",
                "API_KEY",
                "POSTGRES_PASSWORD",
                "RABBITMQ_PASSWORD",
            ):
                details = details.replace(cfg[name], "[redacted]")
            raise RuntimeError("Compose command failed: " + " ".join(command[:5]) + "\n" + details)
        return result.stdout.strip()

    def api(path, body=None, key=None):
        headers = {"X-API-Key": cfg["API_KEY"], "Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(
            base + path, headers=headers, data=None if body is None else json.dumps(body).encode()
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.load(response)

    def create(key):
        status, payment = api(
            "/api/v1/payments",
            {
                "amount": "1500.00",
                "currency": "RUB",
                "description": key,
                "webhook_url": "http://api:9000/webhook",
            },
            key,
        )
        assert status == 202 and payment["status"] == "pending"
        return payment

    def final(payment):
        deadline = time.monotonic() + 55
        while time.monotonic() < deadline:
            _, result = api("/api/v1/payments/" + payment["payment_id"])
            if result["status"] in {"succeeded", "failed"}:
                assert result["processed_at"] is not None
                return result
            time.sleep(0.2)
        raise AssertionError("Payment did not complete")

    def sql(query):
        return compose(
            "exec", "-T", "postgres", "psql", "-U", "payments", "-d", "payments", "-Atc", query
        )

    def queue_size():
        output = compose(
            "exec",
            "-T",
            "rabbitmq",
            "rabbitmqctl",
            "-q",
            "list_queues",
            "-p",
            "payments",
            "name",
            "messages_ready",
            "messages_unacknowledged",
        )
        row = next(line.split() for line in output.splitlines() if line.startswith("payments.new"))
        return tuple(map(int, row[1:]))

    try:
        print("Building the API and consumer images", flush=True)
        compose("build", "api", "consumer")
        compose("up", "-d", "--wait", "postgres", "rabbitmq")
        compose("run", "--rm", "api", "python", "-m", "alembic", "upgrade", "0001")
        # Seed the old schema directly: the current ORM requires the latest columns.
        legacy = {"payment_id": str(uuid4())}
        payload = json.dumps(legacy)
        sql(
            "BEGIN; INSERT INTO payments "
            "(id, amount, currency, description, idempotency_key, webhook_url) "
            f"VALUES ('{legacy['payment_id']}', 1500, 'RUB', 'legacy', 'legacy', "
            "'http://api:9000/webhook'); "
            "INSERT INTO outbox (id, event_type, payload) "
            f"VALUES ('{uuid4()}', 'payments.new', '{payload}'::jsonb); COMMIT;"
        )
        compose("run", "--rm", "api", "python", "-m", "alembic", "upgrade", "head")
        compose("run", "--rm", "api", "python", "-m", "alembic", "check")
        compose("up", "-d", "api")
        for _ in range(50):
            try:
                api("/api/v1/payments/00000000-0000-0000-0000-000000000000")
            except urllib.error.HTTPError as error:
                if error.code == 404:
                    break
                raise
            except (urllib.error.URLError, ConnectionError):
                time.sleep(0.2)
        else:
            raise RuntimeError("API did not become ready")
        # A temporary receiver keeps the existing processing checks independent of retries.
        receiver = """
    from http.server import BaseHTTPRequestHandler, HTTPServer
    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            self.send_response(204)
            self.end_headers()
        def log_message(self, *args):
            pass
    HTTPServer(('0.0.0.0', 9000), Receiver).serve_forever()"""
        compose("exec", "-d", "api", "python", "-c", dedent(receiver))
        compose("up", "-d", "consumer")
        final(legacy)
        print("Migration 0001 -> head preserved and processed the existing payment", flush=True)

        normal = create("normal-processing")
        started = time.monotonic()
        final(normal)
        elapsed = time.monotonic() - started
        assert 2 <= elapsed < 12, elapsed
        print(f"Real asynchronous processing completed in {elapsed:.2f}s", flush=True)
        consumer_id = compose("ps", "-q", "consumer")
        restarts_before = subprocess.check_output(
            ["docker", "inspect", "--format", "{{.RestartCount}}", consumer_id],
            text=True,
            env=compose_env,
        ).strip()
        compose("stop", "rabbitmq")
        offline = create("created-while-broker-offline")
        assert sql("SELECT count(*) FROM outbox WHERE published_at IS NULL") == "1"
        assert api("/api/v1/payments/" + offline["payment_id"])[1]["status"] == "pending"
        compose("start", "rabbitmq")
        final(offline)
        restarts_after = subprocess.check_output(
            ["docker", "inspect", "--format", "{{.RestartCount}}", consumer_id],
            text=True,
            env=compose_env,
        ).strip()
        assert restarts_before == restarts_after, (restarts_before, restarts_after)
        print(
            "API accepted a payment with RabbitMQ offline; "
            "the same worker recovered and processed it",
            flush=True,
        )

        compose("stop", "consumer")
        persisted = create("persistent-delivery")
        publisher = """
    import asyncio
    from app.config import WorkerSettings,load_settings
    from app.db import create_engine,create_session_factory
    from app.messaging import create_broker,declare_topology
    from app.outbox import publish_one
    async def once():
        settings=load_settings(WorkerSettings)
        engine=create_engine(settings)
        broker=create_broker(settings)
        try:
            await broker.connect()
            await declare_topology(broker)
            assert await publish_one(create_session_factory(engine),broker,5)
        finally:
            await broker.stop()
            await engine.dispose()
    asyncio.run(once())"""
        compose("run", "--rm", "consumer", "python", "-c", dedent(publisher))
        assert queue_size() == (1, 0)
        compose("restart", "rabbitmq")
        compose("up", "-d", "--wait", "rabbitmq")
        assert queue_size() == (1, 0)
        compose("start", "consumer")
        final(persisted)
        assert sql("SELECT count(*) FROM payments WHERE processed_at IS NOT NULL") == "4"
        assert sql("SELECT count(*) FROM outbox WHERE published_at IS NOT NULL") == "4"
        compose("stop", "consumer")
        consumer_id = compose("ps", "-a", "-q", "consumer")
        exit_code = subprocess.check_output(
            ["docker", "inspect", "--format", "{{.State.ExitCode}}", consumer_id],
            text=True,
            env=compose_env,
        ).strip()
        assert exit_code == "0", exit_code
        application_logs = compose("logs", "--no-color", "api", "consumer")
        for name in (
            "DATABASE_URL",
            "RABBITMQ_URL",
            "API_KEY",
            "POSTGRES_PASSWORD",
            "RABBITMQ_PASSWORD",
        ):
            assert cfg[name] not in application_logs, "A configuration secret appeared in logs"
        print(
            "Persistent message survived RabbitMQ restart; worker stopped with exit code 0",
            flush=True,
        )
        print("COMPOSE ACCEPTANCE PASSED", flush=True)
    finally:
        with (artifacts / "services.log").open("w", encoding="utf-8") as services_log:
            subprocess.run(
                prefix + ["logs", "--no-color"],
                stdout=services_log,
                stderr=subprocess.STDOUT,
                text=True,
                env=compose_env,
            )
        compose("down", "-v")
        log.close()


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="mkk-payments-smoke-") as directory:
        run_smoke(
            Path(__file__).resolve().parents[1],
            Path(directory),
            "mkk-payments-smoke-" + uuid4().hex[:10],
        )


if __name__ == "__main__":
    main()
