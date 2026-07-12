"""Behavioral tests for the durable cron delivery outbox."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from cron import delivery_outbox
from cron import scheduler


def _job(script: str | None = None) -> dict:
    return {
        "id": "alerts-job",
        "name": "alerts",
        "deliver": "claw_messenger:+15551234567",
        "script": script,
    }


def _target() -> dict:
    return {
        "platform": "claw_messenger",
        "chat_id": "+15551234567",
        "thread_id": None,
    }


def _terminal_record(state: str):
    record = delivery_outbox.enqueue(_job(), f"terminal-{state}", _target())
    if state == "delivered":
        assert delivery_outbox.claim(record.delivery_id) is not None
        assert delivery_outbox.mark_confirmed(
            record.delivery_id,
            {**_target(), "message_id": f"message-{record.delivery_id}"},
        ) == "delivered"
    else:
        assert state == "quarantined"
        delivery_outbox.quarantine(record.delivery_id, "operator audit")
    return record


def test_cron_module_collection_never_binds_live_hermes_home():
    """Import-time constants must point at the test runner's session sandbox."""
    from cron import jobs

    live_jobs = (Path.home() / ".hermes" / "cron" / "jobs.json").resolve()
    assert jobs.JOBS_FILE.resolve() != live_jobs
    assert os.environ.get("HERMES_TESTING") == "1"


def test_outbox_override_accepts_registered_plugin_without_home_env():
    """Explicit plugin targets do not need an implicit home-channel env var."""
    from gateway.platform_registry import PlatformEntry, platform_registry

    name = "explicit_only_test_platform"
    platform_registry.register(
        PlatformEntry(
            name=name,
            label="Explicit only",
            adapter_factory=lambda cfg: None,
            check_fn=lambda: True,
            cron_deliver_env_var="",
        )
    )
    try:
        target = {"platform": name, "chat_id": "concrete-chat", "thread_id": None}
        assert scheduler._resolve_delivery_targets(
            {"id": "plugin-job", "_delivery_targets_override": [target]}
        ) == [target]
    finally:
        platform_registry.unregister(name)


def test_existing_outbox_schema_gets_additive_quarantine_audit_columns():
    db = delivery_outbox._db_path()
    db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE deliveries (
                delivery_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                job_json TEXT NOT NULL,
                content TEXT NOT NULL,
                target_json TEXT NOT NULL,
                ack_json TEXT,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                last_error TEXT,
                receipt_json TEXT,
                ack_attempts INTEGER NOT NULL DEFAULT 0,
                next_ack_at REAL NOT NULL DEFAULT 0,
                ack_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )

    record = delivery_outbox.enqueue(_job(), "migrate", _target())
    assert record.quarantine_reason is None
    assert record.quarantined_at is None
    with sqlite3.connect(db) as conn:
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(deliveries)")
        }
    assert {"quarantine_reason", "quarantined_at"} <= columns


def test_schema_migration_is_serialized_across_first_process_access():
    """Concurrent gateway/CLI startup must not race the same ALTER TABLE."""
    db = delivery_outbox._db_path()
    db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE deliveries (
                delivery_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                job_json TEXT NOT NULL,
                content TEXT NOT NULL,
                target_json TEXT NOT NULL,
                ack_json TEXT,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                last_error TEXT,
                receipt_json TEXT,
                ack_attempts INTEGER NOT NULL DEFAULT 0,
                next_ack_at REAL NOT NULL DEFAULT 0,
                ack_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )

    env = {**os.environ, "HERMES_HOME": str(db.parent.parent), "HERMES_TESTING": "1"}
    code = "from cron.delivery_outbox import state_counts; print(state_counts())"
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code],
            cwd=Path(__file__).parents[2],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for _ in range(6)
    ]
    results = [process.communicate(timeout=20) for process in processes]
    assert [process.returncode for process in processes] == [0] * len(processes), results
    with sqlite3.connect(db) as conn:
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(deliveries)")
        }
    assert {"quarantine_reason", "quarantined_at"} <= columns


def test_terminal_gc_preserves_recent_and_every_unresolved_state(monkeypatch):
    now = 10_000.0
    monkeypatch.setattr(delivery_outbox, "_TERMINAL_RETENTION_SECONDS", 100)
    monkeypatch.setattr(
        delivery_outbox, "_TERMINAL_MIN_AUDIT_TAIL_PER_STATE", 1
    )

    recent = [
        _terminal_record("delivered"),
        _terminal_record("delivered"),
        _terminal_record("quarantined"),
        _terminal_record("quarantined"),
    ]
    old_terminal = [
        _terminal_record("delivered"),
        _terminal_record("delivered"),
        _terminal_record("quarantined"),
        _terminal_record("quarantined"),
    ]
    unresolved = {
        state: delivery_outbox.enqueue(_job(), f"unresolved-{state}", _target())
        for state in delivery_outbox._UNRESOLVED_HEALTH_STATES
    }

    db = delivery_outbox._db_path()
    with sqlite3.connect(db) as conn:
        for index, record in enumerate(recent):
            conn.execute(
                "UPDATE deliveries SET updated_at = ? WHERE delivery_id = ?",
                (now - 10 - index, record.delivery_id),
            )
        for index, record in enumerate(old_terminal):
            conn.execute(
                "UPDATE deliveries SET updated_at = ? WHERE delivery_id = ?",
                (now - 200 - index, record.delivery_id),
            )
        for index, (state, record) in enumerate(unresolved.items()):
            conn.execute(
                "UPDATE deliveries SET state = ?, updated_at = ? "
                "WHERE delivery_id = ?",
                (state, now - 1_000 - index, record.delivery_id),
            )

    assert delivery_outbox.prune_terminal(now=now) == len(old_terminal)
    assert all(delivery_outbox.get(record.delivery_id) for record in recent)
    assert all(delivery_outbox.get(record.delivery_id) is None for record in old_terminal)
    for state, record in unresolved.items():
        stored = delivery_outbox.get(record.delivery_id)
        assert stored is not None
        assert stored.state == state


def test_terminal_gc_is_bounded_and_keeps_a_per_state_audit_tail(monkeypatch):
    now = 20_000.0
    monkeypatch.setattr(delivery_outbox, "_TERMINAL_RETENTION_SECONDS", 100)
    monkeypatch.setattr(
        delivery_outbox, "_TERMINAL_MIN_AUDIT_TAIL_PER_STATE", 2
    )
    monkeypatch.setattr(delivery_outbox, "_TERMINAL_GC_BATCH_SIZE", 3)

    by_state = {
        "delivered": [_terminal_record("delivered") for _ in range(6)],
        "quarantined": [_terminal_record("quarantined") for _ in range(5)],
    }
    db = delivery_outbox._db_path()
    with sqlite3.connect(db) as conn:
        for records in by_state.values():
            for index, record in enumerate(records):
                conn.execute(
                    "UPDATE deliveries SET created_at = ?, updated_at = ? "
                    "WHERE delivery_id = ?",
                    (now - 1_000 + index, now - 1_000 + index, record.delivery_id),
                )

    assert delivery_outbox.prune_terminal(now=now) == 3
    assert delivery_outbox.prune_terminal(now=now) == 3
    assert delivery_outbox.prune_terminal(now=now) == 1
    assert delivery_outbox.prune_terminal(now=now) == 0

    for records in by_state.values():
        assert all(
            delivery_outbox.get(record.delivery_id) is None for record in records[:-2]
        )
        assert all(
            delivery_outbox.get(record.delivery_id) is not None for record in records[-2:]
        )


def test_enqueue_gc_is_hourly_and_failure_cannot_undo_durable_row(monkeypatch):
    calls = []
    monotonic_values = iter([10_000.0, 10_001.0, 13_601.0, 20_000.0])
    monkeypatch.setattr(delivery_outbox, "_LAST_GC_ATTEMPT_MONOTONIC", 0.0)
    monkeypatch.setattr(delivery_outbox.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(delivery_outbox, "prune_terminal", lambda: calls.append(True))

    delivery_outbox.enqueue(_job(), "first", _target())
    delivery_outbox.enqueue(_job(), "throttled", _target())
    delivery_outbox.enqueue(_job(), "next-hour", _target())
    assert calls == [True, True]

    def broken_gc():
        raise sqlite3.OperationalError("maintenance unavailable")

    monkeypatch.setattr(delivery_outbox, "prune_terminal", broken_gc)
    record = delivery_outbox.enqueue(_job(), "still durable", _target())
    assert delivery_outbox.get(record.delivery_id) is not None


def test_claw_plugin_registers_cron_home_and_outbox_target_resolves():
    """Real-registry regression for the live Claw Messenger canary failure."""
    from hermes_cli.plugins import discover_plugins
    from gateway.platform_registry import platform_registry

    discover_plugins()
    entry = platform_registry.get("claw_messenger")
    assert entry is not None
    assert entry.cron_deliver_env_var == "CLAW_MESSENGER_HOME_CHANNEL"
    target = {
        "platform": "claw_messenger",
        "chat_id": "+15551234567",
        "thread_id": None,
    }
    assert scheduler._resolve_delivery_targets(
        {"id": "claw-job", "_delivery_targets_override": [target]}
    ) == [target]


def test_definite_failure_is_persisted_for_retry_without_rerunning_job():
    record = delivery_outbox.enqueue(_job(), "critical alert", _target())
    failure = scheduler.DeliveryReport(
        error="relay explicitly rejected send",
        failed=[{**_target(), "error": "rejected"}],
    )

    with patch("cron.scheduler._deliver_result", return_value=failure), \
         patch("cron.scheduler.mark_job_delivery"):
        result = scheduler._attempt_outbox_delivery(record)

    stored = delivery_outbox.get(record.delivery_id)
    assert result.confirmed is False
    assert stored is not None
    assert stored.state == "retry_wait"
    assert stored.attempts == 1
    assert stored.content == "critical alert"
    # The backoff prevents a tight resend loop.
    assert all(item.delivery_id != record.delivery_id for item in delivery_outbox.due())


def test_ambiguous_attempt_is_quarantined_and_never_auto_retried():
    record = delivery_outbox.enqueue(_job(), "possibly sent", _target())
    ambiguous = scheduler.DeliveryReport(
        error="timed out after dispatch; delivery outcome is unknown",
        ambiguous=[{**_target(), "error": "unknown"}],
    )

    with patch("cron.scheduler._deliver_result", return_value=ambiguous), \
         patch("cron.scheduler.mark_job_delivery"):
        scheduler._attempt_outbox_delivery(record)

    stored = delivery_outbox.get(record.delivery_id)
    assert stored is not None
    assert stored.state == "ambiguous"
    assert all(item.delivery_id != record.delivery_id for item in delivery_outbox.due())


def test_operator_quarantine_is_terminal_audited_and_health_neutral():
    record = delivery_outbox.enqueue(_job(), "known unsent", _target())
    assert delivery_outbox.claim(record.delivery_id) is not None
    delivery_outbox.mark_ambiguous(record.delivery_id, "resolver failed before send")

    result = delivery_outbox.quarantine(
        record.delivery_id,
        "canary resolver failed before platform dispatch",
    )

    assert result.changed is True
    assert result.record.state == "quarantined"
    assert result.record.receipt is None
    assert result.record.quarantine_reason == (
        "canary resolver failed before platform dispatch"
    )
    assert result.record.quarantined_at is not None
    assert delivery_outbox.due() == []
    assert delivery_outbox.due_acks() == []
    snapshot = delivery_outbox.health_snapshot()
    assert snapshot["healthy"] is True
    assert snapshot["unhealthy"] == []
    assert snapshot["quarantined"][0]["delivery_id"] == record.delivery_id
    assert snapshot["counts"]["quarantined"] == 1


def test_operator_quarantine_is_idempotent_and_preserves_first_reason():
    record = delivery_outbox.enqueue(_job(), "known unsent", _target())
    first = delivery_outbox.quarantine(record.delivery_id, "first audit reason")
    second = delivery_outbox.quarantine(record.delivery_id, "replacement reason")

    assert first.changed is True
    assert second.changed is False
    assert second.record.state == "quarantined"
    assert second.record.quarantine_reason == "first audit reason"


def test_operator_quarantine_refuses_receipt_or_active_send():
    active = delivery_outbox.enqueue(_job(), "active", _target())
    assert delivery_outbox.claim(active.delivery_id) is not None
    with pytest.raises(ValueError, match="not safely quarantinable"):
        delivery_outbox.quarantine(active.delivery_id, "unsafe active attempt")

    confirmed = delivery_outbox.enqueue(_job(), "confirmed", _target())
    assert delivery_outbox.claim(confirmed.delivery_id) is not None
    delivery_outbox.mark_confirmed(
        confirmed.delivery_id,
        {**_target(), "message_id": "m-confirmed"},
    )
    with pytest.raises(ValueError, match="delivery receipt is recorded"):
        delivery_outbox.quarantine(confirmed.delivery_id, "must refuse")


def test_stale_inflight_attempt_becomes_ambiguous_not_retryable():
    record = delivery_outbox.enqueue(_job(), "crash window", _target())
    assert delivery_outbox.claim(record.delivery_id) is not None

    db = delivery_outbox._db_path()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE deliveries SET updated_at = ? WHERE delivery_id = ?",
            (time.time() - 3600, record.delivery_id),
        )

    assert delivery_outbox.recover_interrupted() == 1
    assert delivery_outbox.get(record.delivery_id).state == "ambiguous"


def test_late_send_completion_cannot_overwrite_ambiguity_or_quarantine():
    record = delivery_outbox.enqueue(_job(), "late result", _target())
    assert delivery_outbox.claim(record.delivery_id) is not None
    db = delivery_outbox._db_path()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE deliveries SET updated_at = ? WHERE delivery_id = ?",
            (time.time() - 3600, record.delivery_id),
        )

    assert delivery_outbox.recover_interrupted() == 1
    assert delivery_outbox.mark_confirmed(
        record.delivery_id,
        {**_target(), "message_id": "too-late"},
    ) == "ambiguous"
    ambiguous = delivery_outbox.get(record.delivery_id)
    assert ambiguous is not None
    assert ambiguous.state == "ambiguous"
    assert ambiguous.receipt is None

    delivery_outbox.quarantine(record.delivery_id, "operator reconciled")
    assert delivery_outbox.mark_retry(record.delivery_id, "late failure") == "quarantined"
    assert delivery_outbox.mark_ambiguous(record.delivery_id, "late timeout") == "quarantined"
    assert delivery_outbox.mark_confirmed(
        record.delivery_id,
        {**_target(), "message_id": "later-still"},
    ) == "quarantined"
    terminal = delivery_outbox.get(record.delivery_id)
    assert terminal is not None
    assert terminal.state == "quarantined"
    assert terminal.receipt is None


def test_competing_send_completions_have_exactly_one_terminal_winner():
    record = delivery_outbox.enqueue(_job(), "race", _target())
    assert delivery_outbox.claim(record.delivery_id) is not None

    def confirm():
        return delivery_outbox.mark_confirmed(
            record.delivery_id,
            {**_target(), "message_id": "race-id"},
        )

    def make_ambiguous():
        return delivery_outbox.mark_ambiguous(
            record.delivery_id, "confirmation timed out"
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [pool.submit(confirm), pool.submit(make_ambiguous)]
        outcomes = [future.result() for future in results]

    final = delivery_outbox.get(record.delivery_id)
    assert final is not None
    assert final.state in {"delivered", "ambiguous"}
    if final.state == "delivered":
        assert final.receipt["message_id"] == "race-id"
        assert outcomes.count("delivered") == 2
    else:
        assert final.receipt is None
        assert outcomes.count("ambiguous") == 2


def test_ack_claim_is_atomic_and_late_completion_cannot_overwrite_recovery():
    record = delivery_outbox.enqueue(
        _job("alerts.py"),
        "confirmed",
        _target(),
        ack={"args": ["--ack", "token"]},
    )
    assert delivery_outbox.claim(record.delivery_id) is not None
    assert delivery_outbox.mark_confirmed(
        record.delivery_id,
        {**_target(), "message_id": "m-atomic"},
    ) == "ack_pending"

    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(
            pool.map(lambda _i: delivery_outbox.claim_ack(record.delivery_id), range(8))
        )
    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert winners[0].state == "ack_in_flight"
    assert winners[0].ack_attempts == 1

    db = delivery_outbox._db_path()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE deliveries SET updated_at = ? WHERE delivery_id = ?",
            (time.time() - 3600, record.delivery_id),
        )
    assert delivery_outbox.recover_interrupted() == 1
    assert delivery_outbox.mark_acked(record.delivery_id) == "ack_retry"
    assert delivery_outbox.get(record.delivery_id).state == "ack_retry"

    second_claim = delivery_outbox.claim_ack(record.delivery_id)
    assert second_claim is not None
    assert second_claim.ack_attempts == 2
    assert delivery_outbox.mark_acked(record.delivery_id) == "delivered"


def test_confirmed_delivery_runs_idempotent_source_ack_with_receipt_env(
    tmp_path, monkeypatch
):
    hermes_home = Path(os.environ["HERMES_HOME"])
    scripts_dir = hermes_home / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    ack_file = tmp_path / "acked.json"
    script = scripts_dir / "alerts.py"
    script.write_text(
        """
import json, os, pathlib, sys
if sys.argv[1:] == ["--ack", "batch-42"]:
    pathlib.Path(os.environ["ACK_TEST_FILE"]).write_text(json.dumps({
        "confirmation": os.environ.get("HERMES_DELIVERY_CONFIRMATION"),
        "level": os.environ.get("HERMES_DELIVERY_CONFIRMATION_LEVEL"),
        "message_id": os.environ.get("HERMES_DELIVERY_MESSAGE_ID"),
        "protocol": os.environ.get("HERMES_DELIVERY_ACK_PROTOCOL"),
    }))
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("ACK_TEST_FILE", str(ack_file))
    record = delivery_outbox.enqueue(
        _job("alerts.py"),
        "confirmed alert",
        _target(),
        ack={"args": ["--ack", "batch-42"]},
    )
    receipt = {
        **_target(),
        "message_id": "mac-message-123",
        "confirmation": "platform_message_id",
    }
    confirmed = scheduler.DeliveryReport(receipts=[receipt])

    with patch("cron.scheduler._deliver_result", return_value=confirmed), \
         patch("cron.scheduler.mark_job_delivery"):
        scheduler._attempt_outbox_delivery(record)

    stored = delivery_outbox.get(record.delivery_id)
    assert stored is not None and stored.state == "delivered"
    data = json.loads(ack_file.read_text(encoding="utf-8"))
    assert data == {
        "confirmation": "confirmed",
        "level": "platform_message_id",
        "message_id": "mac-message-123",
        "protocol": "1",
    }


def test_source_ack_failure_retries_ack_without_resending_message():
    record = delivery_outbox.enqueue(
        _job("missing.py"),
        "already delivered",
        _target(),
        ack={"args": ["--ack", "batch-9"]},
    )
    receipt = {**_target(), "message_id": "m9", "confirmation": "platform_message_id"}
    assert delivery_outbox.claim(record.delivery_id) is not None
    assert delivery_outbox.mark_confirmed(record.delivery_id, receipt) == "ack_pending"

    scheduler._run_outbox_ack(record.delivery_id, record.job, record.ack, receipt)

    stored = delivery_outbox.get(record.delivery_id)
    assert stored is not None
    assert stored.state == "ack_retry"
    assert stored.attempts == 1  # only the original user-visible send attempt
    assert stored.ack_attempts == 1


def test_ack_marker_is_removed_from_user_visible_script_output():
    output = (
        "P0 alert\n"
        '__HERMES_DELIVERY_ACK__={"args":["--ack","batch-123"]}\n'
    )
    cleaned, directive = scheduler._extract_delivery_ack_directive(output)
    assert cleaned == "P0 alert"
    assert directive == {"args": ["--ack", "batch-123"]}


def test_failed_script_cannot_arm_a_source_ack():
    marker = '__HERMES_DELIVERY_ACK__={"args":["--ack","unsafe"]}'
    job = {
        **_job("alerts.py"),
        "no_agent": True,
        "schedule_display": "every 1h",
    }
    with patch(
        "cron.scheduler._run_job_script",
        return_value=(False, f"script failed\n{marker}"),
    ):
        success, _doc, final, _error = scheduler.run_job(job)

    assert success is False
    assert marker not in final
    assert scheduler._delivery_ack_var.get() is None


def test_failed_llm_job_clears_prerun_ack_before_delivering_failure_alert():
    """A delivered failure notice must not ACK the unreported source batch."""
    job = {
        **_job("alerts.py"),
        "no_agent": False,
        "schedule_display": "every 1h",
    }

    def failed_agent(*_args, **_kwargs):
        scheduler._delivery_ack_var.set({"args": ["--ack", "source-token"]})
        return False, "failure output", "", "agent failed"

    fake_record = SimpleNamespace(delivery_id="failure-alert-delivery")
    confirmed = scheduler.DeliveryReport(
        receipts=[{**_target(), "message_id": "failure-alert-id"}]
    )
    with patch("cron.scheduler.claim_dispatch", return_value=True), \
         patch("cron.scheduler.run_job", side_effect=failed_agent), \
         patch("cron.scheduler.save_job_output", return_value="/tmp/output"), \
         patch("cron.scheduler._resolve_delivery_targets", return_value=[_target()]), \
         patch("cron.delivery_outbox.enqueue", return_value=fake_record) as enqueue, \
         patch("cron.scheduler._attempt_outbox_delivery", return_value=confirmed), \
         patch(
             "cron.delivery_outbox.get",
             return_value=SimpleNamespace(state="delivered"),
         ), \
         patch("cron.scheduler.mark_job_run"):
        assert scheduler.run_one_job(job) is True

    assert enqueue.call_args.kwargs["ack"] is None


def test_health_snapshot_is_receipt_backed_and_flags_ambiguous_rows():
    confirmed = delivery_outbox.enqueue(_job(), "ok", _target())
    assert delivery_outbox.claim(confirmed.delivery_id) is not None
    delivery_outbox.mark_confirmed(
        confirmed.delivery_id,
        {**_target(), "message_id": "m-ok", "confirmation": "platform_message_id"},
    )
    uncertain = delivery_outbox.enqueue(_job(), "unknown", _target())
    assert delivery_outbox.claim(uncertain.delivery_id) is not None
    delivery_outbox.mark_ambiguous(uncertain.delivery_id, "ack timed out")

    snapshot = delivery_outbox.health_snapshot()
    assert snapshot["healthy"] is False
    assert snapshot["latest_confirmed"]["receipt"]["message_id"] == "m-ok"
    assert [item["delivery_id"] for item in snapshot["unhealthy"]] == [
        uncertain.delivery_id
    ]


def test_health_snapshot_flags_every_unresolved_delivery_phase():
    pending = delivery_outbox.enqueue(_job(), "pending", _target())
    snapshot = delivery_outbox.health_snapshot()
    assert snapshot["healthy"] is False
    assert snapshot["unhealthy"][0]["delivery_id"] == pending.delivery_id
    assert snapshot["unhealthy"][0]["state"] == "pending"

    assert delivery_outbox.claim(pending.delivery_id) is not None
    snapshot = delivery_outbox.health_snapshot()
    assert snapshot["healthy"] is False
    assert snapshot["unhealthy"][0]["state"] == "in_flight"

    delivery_outbox.mark_confirmed(
        pending.delivery_id,
        {**_target(), "message_id": "m-awaiting-source-ack"},
    )
    # This row has no source-ack directive, so confirmation is terminal.
    assert delivery_outbox.health_snapshot()["healthy"] is True

    awaiting_ack = delivery_outbox.enqueue(
        _job("alerts.py"),
        "awaiting ack",
        _target(),
        ack={"args": ["--ack", "token"]},
    )
    assert delivery_outbox.claim(awaiting_ack.delivery_id) is not None
    delivery_outbox.mark_confirmed(
        awaiting_ack.delivery_id,
        {**_target(), "message_id": "m-ack-pending"},
    )
    snapshot = delivery_outbox.health_snapshot()
    assert snapshot["healthy"] is False
    assert snapshot["unhealthy"][0]["state"] == "ack_pending"


def test_job_can_require_platform_message_id_confirmation():
    from cron.jobs import create_job, get_job, update_job

    job = create_job(
        prompt="canary",
        schedule="every 1h",
        delivery_confirmation="message_id",
    )
    assert get_job(job["id"])["delivery_confirmation"] == "message_id"
    update_job(job["id"], {"delivery_confirmation": "adapter_ack"})
    assert get_job(job["id"])["delivery_confirmation"] == "adapter_ack"
    with pytest.raises(ValueError, match="delivery_confirmation"):
        update_job(job["id"], {"delivery_confirmation": "hope"})


def test_delivery_status_cli_reports_receipt_backed_json(capsys):
    from hermes_cli.cron import cron_delivery_status

    record = delivery_outbox.enqueue(_job(), "ok", _target())
    assert delivery_outbox.claim(record.delivery_id) is not None
    delivery_outbox.mark_confirmed(
        record.delivery_id,
        {**_target(), "message_id": "m-cli", "confirmation": "platform_message_id"},
    )

    assert cron_delivery_status(as_json=True) == 0
    snapshot = json.loads(capsys.readouterr().out)
    assert snapshot["healthy"] is True
    assert snapshot["latest_confirmed"]["receipt"]["message_id"] == "m-cli"


def test_delivery_quarantine_cli_is_guarded_and_idempotent(capsys):
    from hermes_cli.cron import cron_delivery_quarantine

    record = delivery_outbox.enqueue(_job(), "known unsent", _target())
    assert delivery_outbox.claim(record.delivery_id) is not None
    delivery_outbox.mark_ambiguous(record.delivery_id, "no target resolved")

    assert cron_delivery_quarantine(
        record.delivery_id,
        "operator verified resolver failed before dispatch",
        as_json=True,
    ) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["success"] is True
    assert first["changed"] is True
    assert first["state"] == "quarantined"
    assert first["receipt"] is None

    assert cron_delivery_quarantine(
        record.delivery_id,
        "operator verified resolver failed before dispatch",
        as_json=True,
    ) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["success"] is True
    assert second["changed"] is False
