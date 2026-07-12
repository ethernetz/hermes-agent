"""Durable cron delivery outbox.

Cron execution and message delivery are separate side effects.  A model/script
run can succeed while the platform send fails, and re-running the cron job to
recover would repeat the expensive (and potentially mutating) source work.

This module persists one row per concrete delivery target.  Only explicit
platform success moves a row past the send phase.  Definite failures are
retried with bounded backoff; an in-flight timeout or interrupted attempt is
quarantined as ``ambiguous`` instead of being resent, because the platform may
already have accepted it and a retry would create a duplicate user-visible
message.

The store is profile-scoped through :func:`get_hermes_home` and uses SQLite so
gateway and standalone cron processes can safely share it.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from hermes_constants import get_hermes_home


logger = logging.getLogger(__name__)

_RETRY_DELAYS_SECONDS = (30, 120, 600, 1800, 3600, 7200, 21600)
_MAX_DELIVERY_ATTEMPTS = 8
_MAX_ACK_ATTEMPTS = 12
_INTERRUPTED_ATTEMPT_AFTER_SECONDS = 5 * 60
_TERMINAL_RETENTION_SECONDS = 30 * 24 * 60 * 60
_TERMINAL_MIN_AUDIT_TAIL_PER_STATE = 25
_TERMINAL_GC_BATCH_SIZE = 200
_TERMINAL_GC_INTERVAL_SECONDS = 60 * 60
_TERMINAL_STATES = ("delivered", "quarantined")
_UNRESOLVED_HEALTH_STATES = (
    "pending",
    "in_flight",
    "retry_wait",
    "ack_pending",
    "ack_in_flight",
    "ack_retry",
    "ambiguous",
    "dead",
    "ack_dead",
)
_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY_PATHS: set[str] = set()
_GC_LOCK = threading.Lock()
_LAST_GC_ATTEMPT_MONOTONIC: Optional[float] = None


@dataclass(frozen=True)
class OutboxRecord:
    delivery_id: str
    job_id: str
    job: dict[str, Any]
    content: str
    target: dict[str, Any]
    ack: Optional[dict[str, Any]]
    state: str
    attempts: int
    ack_attempts: int = 0
    receipt: Optional[dict[str, Any]] = None
    last_error: Optional[str] = None
    quarantine_reason: Optional[str] = None
    quarantined_at: Optional[float] = None


@dataclass(frozen=True)
class QuarantineResult:
    record: OutboxRecord
    changed: bool


def _db_path():
    return get_hermes_home() / "cron" / "delivery_outbox.sqlite3"


def get(delivery_id: str) -> Optional[OutboxRecord]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
    return _record(row) if row is not None else None


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path.parent, 0o700)
    except (OSError, NotImplementedError):
        pass
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    try:
        _ensure_schema(conn, path)
        conn.execute("PRAGMA journal_mode = WAL")
    except BaseException:
        conn.close()
        raise
    try:
        os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass
    return conn


def _ensure_schema(conn: sqlite3.Connection, path) -> None:
    """Serialize first-create/additive migration across threads and processes."""
    path_key = str(path.resolve())
    with _SCHEMA_LOCK:
        if path_key in _SCHEMA_READY_PATHS:
            return

        # BEGIN IMMEDIATE is the cross-process migration lock. A second gateway
        # or CLI blocks here, then re-reads the post-migration schema instead of
        # racing the same ALTER TABLE statement.
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deliveries (
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
                    quarantine_reason TEXT,
                    quarantined_at REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(deliveries)").fetchall()
            }
            if "quarantine_reason" not in columns:
                conn.execute(
                    "ALTER TABLE deliveries ADD COLUMN quarantine_reason TEXT"
                )
            if "quarantined_at" not in columns:
                conn.execute("ALTER TABLE deliveries ADD COLUMN quarantined_at REAL")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS deliveries_due_idx "
                "ON deliveries(state, next_attempt_at, created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS deliveries_ack_due_idx "
                "ON deliveries(state, next_ack_at, created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS deliveries_terminal_gc_idx "
                "ON deliveries(state, updated_at DESC, created_at DESC)"
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        _SCHEMA_READY_PATHS.add(path_key)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _decode(raw: Optional[str], fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback


def _record(row: sqlite3.Row) -> OutboxRecord:
    return OutboxRecord(
        delivery_id=row["delivery_id"],
        job_id=row["job_id"],
        job=_decode(row["job_json"], {}),
        content=row["content"],
        target=_decode(row["target_json"], {}),
        ack=_decode(row["ack_json"], None),
        state=row["state"],
        attempts=int(row["attempts"] or 0),
        ack_attempts=int(row["ack_attempts"] or 0),
        receipt=_decode(row["receipt_json"], None),
        last_error=row["last_error"],
        quarantine_reason=row["quarantine_reason"],
        quarantined_at=row["quarantined_at"],
    )


def enqueue(
    job: dict[str, Any],
    content: str,
    target: dict[str, Any],
    *,
    ack: Optional[dict[str, Any]] = None,
) -> OutboxRecord:
    """Persist a delivery before its first platform attempt."""
    now = time.time()
    delivery_id = uuid.uuid4().hex
    # Persist a concrete target override so retries never resend to targets
    # that already succeeded in a multi-target fan-out.
    saved_job = dict(job)
    saved_job["_delivery_targets_override"] = [dict(target)]
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO deliveries (
                delivery_id, job_id, job_json, content, target_json, ack_json,
                state, attempts, next_attempt_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, 0, ?, ?)
            """,
            (
                delivery_id,
                str(job.get("id") or "unknown"),
                _json(saved_job),
                str(content),
                _json(target),
                _json(ack) if ack else None,
                now,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
    record = _record(row)
    # Maintenance runs only after the new delivery is durably committed. A GC
    # failure must never turn a successful enqueue into a failed cron run.
    _maybe_prune_terminal()
    return record


def prune_terminal(*, now: Optional[float] = None) -> int:
    """Delete one bounded batch of aged, terminal delivery history.

    Every non-terminal state is excluded by both the selection and deletion
    predicates. Recent terminal rows are retained, as is a minimum audit tail
    for each terminal outcome even when those rows are old. Repeated calls can
    drain a historical backlog without making any single enqueue hold the
    SQLite write lock for an unbounded amount of work.
    """
    current_time = time.time() if now is None else float(now)
    cutoff = current_time - _TERMINAL_RETENTION_SECONDS

    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        retained_ids: set[str] = set()
        for state in _TERMINAL_STATES:
            rows = conn.execute(
                """
                SELECT delivery_id
                  FROM deliveries
                 WHERE state = ?
                 ORDER BY updated_at DESC, created_at DESC, delivery_id DESC
                 LIMIT ?
                """,
                (state, _TERMINAL_MIN_AUDIT_TAIL_PER_STATE),
            ).fetchall()
            retained_ids.update(str(row["delivery_id"]) for row in rows)

        params: list[Any] = [*_TERMINAL_STATES, cutoff]
        retained_clause = ""
        if retained_ids:
            placeholders = ",".join("?" for _ in retained_ids)
            retained_clause = f"AND delivery_id NOT IN ({placeholders})"
            params.extend(sorted(retained_ids))
        params.append(_TERMINAL_GC_BATCH_SIZE)
        candidates = conn.execute(
            f"""
            SELECT delivery_id
              FROM deliveries
             WHERE state IN (?, ?)
               AND updated_at < ?
               {retained_clause}
             ORDER BY updated_at ASC, created_at ASC, delivery_id ASC
             LIMIT ?
            """,
            params,
        ).fetchall()
        delivery_ids = [str(row["delivery_id"]) for row in candidates]
        if not delivery_ids:
            conn.rollback()
            return 0

        placeholders = ",".join("?" for _ in delivery_ids)
        cur = conn.execute(
            f"""
            DELETE FROM deliveries
             WHERE state IN (?, ?)
               AND delivery_id IN ({placeholders})
            """,
            [*_TERMINAL_STATES, *delivery_ids],
        )
        conn.commit()
        return int(cur.rowcount or 0)


def _maybe_prune_terminal() -> None:
    """Run best-effort terminal GC at most once per process each hour."""
    global _LAST_GC_ATTEMPT_MONOTONIC

    monotonic_now = time.monotonic()
    with _GC_LOCK:
        if (
            _LAST_GC_ATTEMPT_MONOTONIC is not None
            and monotonic_now - _LAST_GC_ATTEMPT_MONOTONIC
            < _TERMINAL_GC_INTERVAL_SECONDS
        ):
            return
        _LAST_GC_ATTEMPT_MONOTONIC = monotonic_now

    try:
        prune_terminal()
    except Exception:
        # Delivery durability is more important than best-effort maintenance.
        # The throttle prevents a broken database from causing a tight retry
        # loop; the next process or hourly attempt can try again.
        logger.warning("Delivery outbox terminal GC failed", exc_info=True)
        return


def claim(delivery_id: str) -> Optional[OutboxRecord]:
    """Atomically claim one send attempt, returning the claimed row."""
    now = time.time()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
        if row is None or row["state"] not in {"pending", "retry_wait"}:
            conn.rollback()
            return None
        if float(row["next_attempt_at"] or 0) > now:
            conn.rollback()
            return None
        cur = conn.execute(
            """
            UPDATE deliveries
               SET state = 'in_flight', attempts = attempts + 1, updated_at = ?
             WHERE delivery_id = ? AND state = ? AND receipt_json IS NULL
            """,
            (now, delivery_id, row["state"]),
        )
        if cur.rowcount != 1:
            conn.rollback()
            return None
        conn.commit()
        claimed = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
    return _record(claimed)


def due(limit: int = 20) -> list[OutboxRecord]:
    """Return send rows whose definite-failure backoff has elapsed."""
    now = time.time()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM deliveries
             WHERE state IN ('pending', 'retry_wait') AND next_attempt_at <= ?
             ORDER BY created_at ASC
             LIMIT ?
            """,
            (now, max(1, int(limit))),
        ).fetchall()
    return [_record(row) for row in rows]


def recover_interrupted() -> int:
    """Quarantine stale in-flight attempts instead of blindly resending.

    A process can die after the platform accepted a message but before SQLite
    recorded the acknowledgement.  That outcome is unknowable without a
    platform idempotency/read-back API; automatic resend would risk a duplicate.
    """
    now = time.time()
    cutoff = now - _INTERRUPTED_ATTEMPT_AFTER_SECONDS
    with _connect() as conn:
        send_cur = conn.execute(
            """
            UPDATE deliveries
               SET state = 'ambiguous',
                   last_error = COALESCE(last_error,
                       'delivery process ended before acknowledgement was recorded'),
                   updated_at = ?
             WHERE state = 'in_flight' AND updated_at <= ?
            """,
            (now, cutoff),
        )
        ack_cur = conn.execute(
            """
            UPDATE deliveries
               SET state = CASE
                       WHEN ack_attempts >= ? THEN 'ack_dead'
                       ELSE 'ack_retry'
                   END,
                   next_ack_at = CASE
                       WHEN ack_attempts >= ? THEN 0
                       ELSE ?
                   END,
                   ack_error = COALESCE(ack_error,
                       'source acknowledgement process ended before completion'),
                   updated_at = ?
             WHERE state = 'ack_in_flight' AND updated_at <= ?
            """,
            (_MAX_ACK_ATTEMPTS, _MAX_ACK_ATTEMPTS, now, now, cutoff),
        )
        return int(send_cur.rowcount or 0) + int(ack_cur.rowcount or 0)


def mark_retry(delivery_id: str, error: str) -> str:
    """CAS ``in_flight`` to retry/dead after a definite failure."""
    now = time.time()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT state, attempts, receipt_json FROM deliveries WHERE delivery_id = ?",
            (delivery_id,),
        ).fetchone()
        if row is None:
            conn.rollback()
            return "missing"
        if row["state"] != "in_flight" or row["receipt_json"] is not None:
            conn.rollback()
            return str(row["state"])
        attempts = int(row["attempts"] or 0)
        if attempts >= _MAX_DELIVERY_ATTEMPTS:
            state = "dead"
            next_at = 0
        else:
            state = "retry_wait"
            delay_index = min(max(attempts - 1, 0), len(_RETRY_DELAYS_SECONDS) - 1)
            next_at = now + _RETRY_DELAYS_SECONDS[delay_index]
        cur = conn.execute(
            """
            UPDATE deliveries
               SET state = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
             WHERE delivery_id = ? AND state = 'in_flight'
                   AND receipt_json IS NULL
            """,
            (state, next_at, str(error)[:2000], now, delivery_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            current = get(delivery_id)
            return current.state if current is not None else "missing"
        conn.commit()
    return state


def mark_ambiguous(delivery_id: str, error: str) -> str:
    """CAS ``in_flight`` to ambiguous so late results cannot overwrite it."""
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT state, receipt_json FROM deliveries WHERE delivery_id = ?",
            (delivery_id,),
        ).fetchone()
        if row is None:
            conn.rollback()
            return "missing"
        if row["state"] != "in_flight" or row["receipt_json"] is not None:
            conn.rollback()
            return str(row["state"])
        cur = conn.execute(
            """
            UPDATE deliveries
               SET state = 'ambiguous', last_error = ?, updated_at = ?
             WHERE delivery_id = ? AND state = 'in_flight'
                   AND receipt_json IS NULL
            """,
            (str(error)[:2000], time.time(), delivery_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            current = get(delivery_id)
            return current.state if current is not None else "missing"
        conn.commit()
    return "ambiguous"


def quarantine(delivery_id: str, reason: str) -> QuarantineResult:
    """Terminally quarantine an unsent delivery with an operator audit reason.

    This is intentionally stricter than the internal state-marking helpers:
    it refuses any receipt-backed row, any row whose state implies confirmed
    delivery, and any active in-flight attempt. The transaction prevents a
    pending/retry row from being claimed concurrently. Repeating the command
    for an already-quarantined row is safe and preserves the original audit
    reason.

    No source acknowledgement callback is invoked here. Quarantine only
    suppresses future delivery retries and removes the row from active health
    alerts; it never claims that the user-visible message was delivered.
    """
    delivery_id = str(delivery_id or "").strip()
    reason = str(reason or "").strip()
    if not delivery_id:
        raise ValueError("delivery ID is required")
    if not reason:
        raise ValueError("quarantine reason is required")

    allowed_states = {"pending", "retry_wait", "ambiguous", "dead"}
    now = time.time()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
        if row is None:
            conn.rollback()
            raise ValueError(f"delivery not found: {delivery_id}")

        state = str(row["state"])
        if row["receipt_json"] is not None:
            conn.rollback()
            raise ValueError(
                f"refusing to quarantine {delivery_id}: a delivery receipt is recorded"
            )
        if state == "quarantined":
            conn.rollback()
            return QuarantineResult(record=_record(row), changed=False)
        if state not in allowed_states:
            conn.rollback()
            raise ValueError(
                f"refusing to quarantine {delivery_id}: state {state!r} is not safely quarantinable"
            )

        cur = conn.execute(
            """
            UPDATE deliveries
               SET state = 'quarantined', quarantine_reason = ?,
                   quarantined_at = ?, updated_at = ?
             WHERE delivery_id = ? AND state = ? AND receipt_json IS NULL
            """,
            (reason[:2000], now, now, delivery_id, state),
        )
        if cur.rowcount != 1:
            conn.rollback()
            current = get(delivery_id)
            if current is None:
                raise ValueError(f"delivery not found: {delivery_id}")
            raise ValueError(
                f"refusing to quarantine {delivery_id}: state changed to "
                f"{current.state!r} during quarantine"
            )
        conn.commit()
        quarantined = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
    return QuarantineResult(record=_record(quarantined), changed=True)


def mark_confirmed(delivery_id: str, receipt: dict[str, Any]) -> str:
    """CAS an active send to receipt-confirmed/ACK-pending state."""
    now = time.time()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT state, ack_json, receipt_json FROM deliveries WHERE delivery_id = ?",
            (delivery_id,),
        ).fetchone()
        if row is None:
            conn.rollback()
            return "missing"
        if row["state"] != "in_flight" or row["receipt_json"] is not None:
            conn.rollback()
            return str(row["state"])
        state = "ack_pending" if row["ack_json"] else "delivered"
        cur = conn.execute(
            """
            UPDATE deliveries
               SET state = ?, receipt_json = ?, last_error = NULL,
                   next_ack_at = ?, updated_at = ?
             WHERE delivery_id = ? AND state = 'in_flight'
                   AND receipt_json IS NULL
            """,
            (state, _json(receipt), now, now, delivery_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            current = get(delivery_id)
            return current.state if current is not None else "missing"
        conn.commit()
    return state


def due_acks(limit: int = 20) -> list[OutboxRecord]:
    """Return confirmed deliveries whose source acknowledgement is due."""
    now = time.time()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM deliveries
             WHERE state IN ('ack_pending', 'ack_retry') AND next_ack_at <= ?
             ORDER BY created_at ASC
             LIMIT ?
            """,
            (now, max(1, int(limit))),
        ).fetchall()
    return [_record(row) for row in rows]


def claim_ack(delivery_id: str) -> Optional[OutboxRecord]:
    """Atomically claim one idempotent source-ack attempt."""
    now = time.time()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
        if row is None or row["state"] not in {"ack_pending", "ack_retry"}:
            conn.rollback()
            return None
        if row["receipt_json"] is None or float(row["next_ack_at"] or 0) > now:
            conn.rollback()
            return None
        cur = conn.execute(
            """
            UPDATE deliveries
               SET state = 'ack_in_flight', ack_attempts = ack_attempts + 1,
                   updated_at = ?
             WHERE delivery_id = ? AND state = ? AND receipt_json IS NOT NULL
            """,
            (now, delivery_id, row["state"]),
        )
        if cur.rowcount != 1:
            conn.rollback()
            return None
        conn.commit()
        claimed = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
    return _record(claimed)


def mark_acked(delivery_id: str) -> str:
    """CAS a claimed ACK to terminal delivered state."""
    with _connect() as conn:
        cur = conn.execute(
            """
            UPDATE deliveries
               SET state = 'delivered', ack_error = NULL, updated_at = ?
             WHERE delivery_id = ? AND state = 'ack_in_flight'
                   AND receipt_json IS NOT NULL
            """,
            (time.time(), delivery_id),
        )
        if cur.rowcount == 1:
            return "delivered"
        row = conn.execute(
            "SELECT state FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
        return str(row["state"]) if row is not None else "missing"


def mark_ack_retry(delivery_id: str, error: str) -> str:
    """CAS a claimed ACK to retry/dead; never resend the user message."""
    now = time.time()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT state, ack_attempts, receipt_json FROM deliveries WHERE delivery_id = ?",
            (delivery_id,),
        ).fetchone()
        if row is None:
            conn.rollback()
            return "missing"
        if row["state"] != "ack_in_flight" or row["receipt_json"] is None:
            conn.rollback()
            return str(row["state"])
        attempts = int(row["ack_attempts"] or 0)
        if attempts >= _MAX_ACK_ATTEMPTS:
            state = "ack_dead"
            next_at = 0
        else:
            state = "ack_retry"
            delay_index = min(max(attempts - 1, 0), len(_RETRY_DELAYS_SECONDS) - 1)
            next_at = now + _RETRY_DELAYS_SECONDS[delay_index]
        cur = conn.execute(
            """
            UPDATE deliveries
               SET state = ?, next_ack_at = ?,
                   ack_error = ?, updated_at = ?
             WHERE delivery_id = ? AND state = 'ack_in_flight'
                   AND receipt_json IS NOT NULL
            """,
            (state, next_at, str(error)[:2000], now, delivery_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            current = get(delivery_id)
            return current.state if current is not None else "missing"
        conn.commit()
    return state


def state_counts() -> dict[str, int]:
    """Small health surface used by diagnostics/canaries."""
    with _connect() as conn:
        rows = conn.execute(
            "SELECT state, COUNT(*) AS n FROM deliveries GROUP BY state"
        ).fetchall()
    return {str(row["state"]): int(row["n"]) for row in rows}


def health_snapshot(limit: int = 20) -> dict[str, Any]:
    """Return receipt-backed delivery health without exposing message content."""
    with _connect() as conn:
        counts = {
            str(row["state"]): int(row["n"])
            for row in conn.execute(
                "SELECT state, COUNT(*) AS n FROM deliveries GROUP BY state"
            ).fetchall()
        }
        rows = conn.execute(
            """
            SELECT delivery_id, job_id, target_json, state, attempts,
                   last_error, receipt_json, ack_error, quarantine_reason,
                   quarantined_at, created_at, updated_at
              FROM deliveries
             WHERE state IN (
                 'pending', 'in_flight', 'retry_wait', 'ack_pending',
                 'ack_in_flight',
                 'ack_retry', 'ambiguous', 'dead', 'ack_dead'
             )
             ORDER BY updated_at DESC
             LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        latest = conn.execute(
            """
            SELECT delivery_id, job_id, target_json, receipt_json, updated_at
              FROM deliveries
             WHERE state = 'delivered' AND receipt_json IS NOT NULL
             ORDER BY updated_at DESC
             LIMIT 1
            """
        ).fetchone()
        quarantined = conn.execute(
            """
            SELECT delivery_id, job_id, target_json, state, attempts,
                   last_error, receipt_json, ack_error, quarantine_reason,
                   quarantined_at, created_at, updated_at
              FROM deliveries
             WHERE state = 'quarantined'
             ORDER BY quarantined_at DESC, updated_at DESC
             LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()

    def _summary(row: sqlite3.Row) -> dict[str, Any]:
        receipt = _decode(row["receipt_json"], None)
        has_error_columns = "last_error" in row.keys()
        error = None
        if has_error_columns:
            error = row["last_error"] or row["ack_error"]
        return {
            "delivery_id": row["delivery_id"],
            "job_id": row["job_id"],
            "target": _decode(row["target_json"], {}),
            "state": row["state"] if "state" in row.keys() else "delivered",
            "attempts": int(row["attempts"] or 0) if "attempts" in row.keys() else None,
            "error": error,
            "receipt": receipt,
            "quarantine_reason": (
                row["quarantine_reason"]
                if "quarantine_reason" in row.keys()
                else None
            ),
            "quarantined_at": (
                row["quarantined_at"] if "quarantined_at" in row.keys() else None
            ),
            "updated_at": row["updated_at"],
        }

    return {
        "healthy": not any(
            counts.get(state, 0) for state in _UNRESOLVED_HEALTH_STATES
        ),
        "counts": counts,
        "unhealthy": [_summary(row) for row in rows],
        "quarantined": [_summary(row) for row in quarantined],
        "latest_confirmed": _summary(latest) if latest is not None else None,
    }
