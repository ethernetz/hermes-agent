"""Tests for out-of-band delivery memory and session-reset carryover.

Regression suite for the 2026-07-04 "send" amnesia incident: a cron job
texted the user a draft-approval request, the user replied "send" 17 minutes
later, and the gateway agent — in a fresh session that had never seen the
cron text — answered "I'm here. What do you need?".

Two mechanisms under test:

1. gateway/outbound_memory.py — cron/webhook deliveries are recorded into
   the target chat's session transcript as assistant turns, so the agent
   sees what was sent to the user on its behalf.
2. Session-reset carryover — idle/daily auto-resets carry a recap of the
   expired conversation's tail into the new session instead of declaring
   "no prior context".
"""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import GatewayConfig, Platform, SessionResetPolicy
from gateway.outbound_memory import (
    DELIVERY_FAILED_PREFIX,
    OUT_OF_BAND_PREFIX,
    begin_outbound_record,
    finish_outbound_record,
    format_outbound_record,
    record_outbound,
    set_outbound_recorder,
    set_outbound_writeahead_recorder,
)
from gateway.session import SessionEntry, SessionSource, SessionStore


@pytest.fixture()
def store(tmp_path):
    config = GatewayConfig()
    with patch("gateway.session.SessionStore._ensure_loaded"):
        s = SessionStore(sessions_dir=tmp_path, config=config)
    # Upstream spec 002 removed the legacy JSONL transcript fallback; a real
    # (temp) SQLite SessionDB is now required for transcript reads, and
    # messages have a FK on sessions — register ids before appending.
    from hermes_state import SessionDB
    s._db = SessionDB(db_path=tmp_path / "state.db")
    _orig_append = s.append_to_transcript
    def _append(session_id, message, skip_db=False):
        s._db.ensure_session(session_id, source="gateway")
        _orig_append(session_id, message, skip_db=skip_db)
    s.append_to_transcript = _append
    s._loaded = True
    return s


@pytest.fixture(autouse=True)
def _clean_recorder():
    """Recorders are process-global state — never leak them between tests."""
    set_outbound_recorder(None)
    set_outbound_writeahead_recorder(None)
    yield
    set_outbound_recorder(None)
    set_outbound_writeahead_recorder(None)


class _FakeWriteaheadRecorder:
    """Capture-everything write-ahead recorder for ordering assertions."""

    def __init__(self, events=None):
        self.events = events if events is not None else []
        self.handles = []

    def begin(self, platform_name, chat_id, text, origin, thread_id):
        handle = {
            "platform": platform_name, "chat_id": chat_id,
            "text": text, "origin": origin, "thread_id": thread_id,
        }
        self.handles.append(handle)
        self.events.append(("begin", text))
        return handle

    def mark_delivered(self, handle, platform_message_id):
        self.events.append(("delivered", platform_message_id))

    def mark_failed(self, handle, error):
        self.events.append(("failed", error))


def _dm_source(chat_id="12345", platform=Platform.TELEGRAM, **kwargs):
    defaults = dict(
        platform=platform,
        chat_id=chat_id,
        chat_name=chat_id,
        chat_type="dm",
        user_id=chat_id,
        user_name="ethan",
    )
    defaults.update(kwargs)
    return SessionSource(**defaults)


class TestRecorderRegistry:
    def test_no_recorder_registered_is_noop(self):
        assert record_outbound("telegram", "123", "hello") is False

    def test_registered_recorder_receives_delivery(self):
        calls = []
        set_outbound_recorder(
            lambda platform, chat, text, origin, thread: calls.append(
                (platform, chat, text, origin, thread)
            )
        )
        assert record_outbound(
            "telegram", "123", "alert text", origin="cron job 'x'", thread_id="t9"
        ) is True
        assert calls == [("telegram", "123", "alert text", "cron job 'x'", "t9")]

    def test_recorder_exception_is_swallowed(self):
        def _boom(*args):
            raise RuntimeError("recorder died")

        set_outbound_recorder(_boom)
        # Must not raise — delivery already succeeded, recording is best-effort.
        assert record_outbound("telegram", "123", "text") is False

    def test_blank_text_or_chat_is_not_recorded(self):
        calls = []
        set_outbound_recorder(lambda *a: calls.append(a))
        assert record_outbound("telegram", "123", "   ") is False
        assert record_outbound("telegram", "", "text") is False
        assert record_outbound("", "123", "text") is False
        assert calls == []


class TestFindSourceForChat:
    def test_finds_existing_dm_origin(self, store):
        source = _dm_source("999")
        store.get_or_create_session(source)

        found = store.find_source_for_chat(Platform.TELEGRAM, "999")
        assert found is not None
        assert found.chat_id == "999"
        assert found.chat_type == "dm"

    def test_unknown_chat_returns_none(self, store):
        assert store.find_source_for_chat(Platform.TELEGRAM, "nope") is None

    def test_platform_must_match(self, store):
        store.get_or_create_session(_dm_source("999"))
        assert store.find_source_for_chat(Platform.DISCORD, "999") is None

    def test_most_recent_entry_wins(self, store):
        dm = _dm_source("777")
        group = _dm_source("777", chat_type="group", user_id="u1")
        dm_entry = store.get_or_create_session(dm)
        group_entry = store.get_or_create_session(group)
        # Backdate the group entry so the DM one is most recent
        group_entry.updated_at = dm_entry.updated_at - timedelta(hours=1)

        found = store.find_source_for_chat(Platform.TELEGRAM, "777")
        assert found.chat_type == "dm"


class TestRecordedDeliveryVisibleToReply:
    """The money test: a cron delivery must land in the exact session the
    user's next reply will hit, as an assistant turn the agent can see."""

    def _record_via_runner(self, store, platform_name, chat_id, text, origin,
                           thread_id=None):
        from gateway.run import GatewayRunner

        runner = MagicMock()
        runner.session_store = store
        runner._resolve_outbound_session_entry = (
            lambda p, c, t=None: GatewayRunner._resolve_outbound_session_entry(
                runner, p, c, t
            )
        )
        GatewayRunner._record_outbound_delivery(
            runner, platform_name, chat_id, text, origin, thread_id
        )

    def test_delivery_recorded_as_assistant_turn(self, store):
        self._record_via_runner(
            store, "telegram", "555",
            "edward porper replied — reply \"send\" to approve the draft",
            "cron job 'Chessreps critical alerts'",
        )

        # The user's reply resolves to the same session
        entry = store.get_or_create_session(_dm_source("555"))
        history = store.load_transcript(entry.session_id)
        assert len(history) == 1
        msg = history[0]
        assert msg["role"] == "assistant"
        assert msg["content"].startswith(OUT_OF_BAND_PREFIX)
        assert "cron job 'Chessreps critical alerts'" in msg["content"]
        assert 'reply "send" to approve' in msg["content"]

    def test_delivery_reuses_existing_session(self, store):
        existing = store.get_or_create_session(_dm_source("555"))
        store.append_to_transcript(
            existing.session_id, {"role": "user", "content": "earlier chat"}
        )

        self._record_via_runner(
            store, "telegram", "555", "new alert", "cron job 'x'"
        )

        entry = store.get_or_create_session(_dm_source("555"))
        assert entry.session_id == existing.session_id
        history = store.load_transcript(entry.session_id)
        assert [m["role"] for m in history] == ["user", "assistant"]

    def test_unknown_platform_is_noop(self, store):
        self._record_via_runner(
            store, "not-a-real-platform-xyz!!", "555", "text", "cron job 'x'"
        )
        assert store.find_source_for_chat(Platform.TELEGRAM, "555") is None

    def test_delivery_to_expired_session_starts_new_one_with_carryover_pointer(
        self, store
    ):
        """A delivery arriving after idle expiry creates the replacement
        session (with prev_session_id set), records into it, and the user's
        reply minutes later lands in that same session."""
        store.config.default_reset_policy = SessionResetPolicy(
            mode="idle", idle_minutes=20
        )
        old = store.get_or_create_session(_dm_source("555"))
        store.append_to_transcript(
            old.session_id, {"role": "user", "content": "old conversation"}
        )
        old.updated_at = old.updated_at - timedelta(minutes=45)

        self._record_via_runner(store, "telegram", "555", "alert", "cron job 'x'")

        entry = store.get_or_create_session(_dm_source("555"))
        assert entry.session_id != old.session_id
        assert entry.prev_session_id == old.session_id
        history = store.load_transcript(entry.session_id)
        assert [m["role"] for m in history] == ["assistant"]
        assert "alert" in history[0]["content"]


class TestResetCarryover:
    def test_idle_reset_records_prev_session_id(self, store):
        store.config.default_reset_policy = SessionResetPolicy(
            mode="idle", idle_minutes=20
        )
        old = store.get_or_create_session(_dm_source())
        old.updated_at = old.updated_at - timedelta(minutes=45)

        new = store.get_or_create_session(_dm_source())
        assert new.session_id != old.session_id
        assert new.was_auto_reset is True
        assert new.auto_reset_reason == "idle"
        assert new.prev_session_id == old.session_id

    def test_suspended_reset_has_no_carryover_pointer(self, store):
        old = store.get_or_create_session(_dm_source())
        old.suspended = True

        new = store.get_or_create_session(_dm_source())
        assert new.session_id != old.session_id
        assert new.prev_session_id is None

    def test_prev_session_id_survives_dict_roundtrip(self, store):
        store.config.default_reset_policy = SessionResetPolicy(
            mode="idle", idle_minutes=20
        )
        old = store.get_or_create_session(_dm_source())
        old.updated_at = old.updated_at - timedelta(minutes=45)
        new = store.get_or_create_session(_dm_source())

        restored = SessionEntry.from_dict(new.to_dict())
        assert restored.prev_session_id == old.session_id


class TestBuildCarryoverRecap:
    def test_recap_keeps_only_user_and_assistant_text(self, store):
        sid = "prev_1"
        for msg in [
            {"role": "session_meta", "tools": []},
            {"role": "system", "content": "system prompt"},
            {"role": "user", "content": "has dan texted me?"},
            {"role": "assistant", "content": None, "tool_calls": [{"id": "t1"}]},
            {"role": "tool", "content": "tool output", "tool_call_id": "t1"},
            {"role": "assistant", "content": "no, nothing from dan today"},
        ]:
            store.append_to_transcript(sid, msg)

        recap = store.build_carryover_recap(sid)
        assert recap == (
            "User: has dan texted me?\n"
            "You: no, nothing from dan today"
        )

    def test_recap_extracts_multimodal_text_parts(self, store):
        sid = "prev_2"
        store.append_to_transcript(sid, {
            "role": "user",
            "content": [
                {"type": "text", "text": "look at this"},
                {"type": "image_url", "image_url": {"url": "data:..."}},
            ],
        })
        recap = store.build_carryover_recap(sid)
        assert recap == "User: look at this"

    def test_recap_respects_max_messages(self, store):
        sid = "prev_3"
        for i in range(20):
            store.append_to_transcript(sid, {"role": "user", "content": f"msg {i}"})
        recap = store.build_carryover_recap(sid, max_messages=3)
        assert recap == "User: msg 17\nUser: msg 18\nUser: msg 19"

    def test_recap_keeps_unanswered_out_of_band_tail_beyond_cap(self, store):
        """An alert the user hasn't answered must never fall out of the recap.

        A burst of deliveries after the user's last message can exceed
        max_messages; all out-of-band turns newer than the last user turn
        are kept regardless of the cap.
        """
        sid = "prev_oob_tail"
        store.append_to_transcript(sid, {"role": "user", "content": "hi"})
        store.append_to_transcript(sid, {"role": "assistant", "content": "hey"})
        for i in range(8):
            store.append_to_transcript(sid, {
                "role": "assistant",
                "content": format_outbound_record(f"alert {i}", "cron job 'a'"),
            })
        recap = store.build_carryover_recap(sid, max_messages=3, max_chars=100000)
        # All 8 alerts survive even though the cap is 3.
        for i in range(8):
            assert f"alert {i}" in recap
        # The pre-burst small talk is beyond both the cap and the OOB rule.
        assert "User: hi" not in recap

    def test_recap_oob_before_last_user_message_not_exempt(self, store):
        sid = "prev_oob_answered"
        store.append_to_transcript(sid, {
            "role": "assistant",
            "content": format_outbound_record("old alert", "cron job 'a'"),
        })
        for i in range(6):
            store.append_to_transcript(sid, {"role": "user", "content": f"msg {i}"})
        recap = store.build_carryover_recap(sid, max_messages=3)
        # The user has spoken since; the old alert obeys the normal cap.
        assert "old alert" not in recap

    def test_recap_clips_long_messages(self, store):
        sid = "prev_4"
        store.append_to_transcript(sid, {"role": "user", "content": "x" * 2000})
        recap = store.build_carryover_recap(sid, per_message_chars=100)
        assert len(recap) < 120
        assert recap.endswith("…")

    def test_recap_empty_or_missing_returns_none(self, store):
        assert store.build_carryover_recap("does_not_exist") is None
        assert store.build_carryover_recap("") is None
        sid = "prev_5"
        store.append_to_transcript(sid, {"role": "session_meta", "tools": []})
        assert store.build_carryover_recap(sid) is None

    def test_recap_disabled_with_zero_messages(self, store):
        sid = "prev_6"
        store.append_to_transcript(sid, {"role": "user", "content": "hi"})
        assert store.build_carryover_recap(sid, max_messages=0) is None


class TestResetPolicyCarryoverConfig:
    def test_default_is_enabled(self):
        policy = SessionResetPolicy.from_dict({})
        assert policy.carryover_messages == 12

    def test_explicit_zero_disables(self):
        policy = SessionResetPolicy.from_dict({"carryover_messages": 0})
        assert policy.carryover_messages == 0

    def test_invalid_value_falls_back_to_default(self):
        policy = SessionResetPolicy.from_dict({"carryover_messages": "lots"})
        assert policy.carryover_messages == 12

    def test_negative_clamped_to_zero(self):
        policy = SessionResetPolicy.from_dict({"carryover_messages": -5})
        assert policy.carryover_messages == 0

    def test_roundtrip(self):
        policy = SessionResetPolicy.from_dict({"carryover_messages": 6})
        assert SessionResetPolicy.from_dict(policy.to_dict()).carryover_messages == 6


class TestCronDeliveryRecords:
    """cron/scheduler.py must record successful deliveries via record_outbound."""

    def _run_delivery(self, send_result, record_mock):
        from cron.scheduler import _deliver_result

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("tools.send_message_tool._send_to_platform",
                   new=AsyncMock(return_value=send_result)), \
             patch("gateway.outbound_memory.record_outbound", record_mock):
            job = {
                "id": "job-1",
                "name": "Chessreps critical alerts",
                "deliver": "origin",
                "origin": {"platform": "telegram", "chat_id": "123"},
            }
            return _deliver_result(job, "edward porper replied — pick a slot")

    def test_successful_delivery_is_recorded(self):
        record_mock = MagicMock(return_value=True)
        err = self._run_delivery({"success": True}, record_mock)
        assert err is None
        record_mock.assert_called_once()
        args, kwargs = record_mock.call_args
        assert args[0] == "telegram"
        assert args[1] == "123"
        assert "edward porper replied" in args[2]
        assert kwargs["origin"] == "cron job 'Chessreps critical alerts'"

    def test_failed_delivery_is_not_recorded(self):
        record_mock = MagicMock(return_value=True)
        err = self._run_delivery({"error": "boom"}, record_mock)
        assert err is not None
        record_mock.assert_not_called()


class TestCronLiveAdapterDeliveryRecords:
    """The LIVE-ADAPTER delivery path must also record via record_outbound.

    Regression test for the 2026-07-06 incident: after the v0.18 merge the
    record_outbound call survived only in the standalone fallback path, so
    every healthy delivery (gateway up, live adapter connected — the normal
    case) was texted to the user but never recorded. The agent then denied
    knowledge of a CS alert it had sent 5 minutes earlier.
    """

    def _run_live_delivery(self, record_mock, deliver_result=None):
        import asyncio
        import threading

        from cron.scheduler import _deliver_result

        pconfig = MagicMock()
        pconfig.enabled = True
        pconfig.extra = {}
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        try:
            with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
                 patch("gateway.delivery.DeliveryRouter._deliver_to_platform",
                       new=AsyncMock(return_value=deliver_result or {"success": True})), \
                 patch("gateway.outbound_memory.record_outbound", record_mock):
                job = {
                    "id": "job-1",
                    "name": "Chessreps critical alerts",
                    "deliver": "origin",
                    "origin": {"platform": "telegram", "chat_id": "123"},
                }
                adapters = {Platform.TELEGRAM: MagicMock()}
                return _deliver_result(
                    job,
                    "polborta@gmail.com's case needs you now",
                    adapters=adapters,
                    loop=loop,
                )
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)
            loop.close()

    def test_live_adapter_delivery_is_recorded(self):
        record_mock = MagicMock(return_value=True)
        err = self._run_live_delivery(record_mock)
        assert err is None
        record_mock.assert_called_once()
        args, kwargs = record_mock.call_args
        assert args[0] == "telegram"
        assert args[1] == "123"
        assert "polborta@gmail.com" in args[2]
        assert kwargs["origin"] == "cron job 'Chessreps critical alerts'"

    def test_failed_live_adapter_send_falls_back_and_records_once(self):
        record_mock = MagicMock(return_value=True)
        with patch("tools.send_message_tool._send_to_platform",
                   new=AsyncMock(return_value={"success": True})):
            err = self._run_live_delivery(
                record_mock, deliver_result={"success": False, "error": "boom"},
            )
        assert err is None
        record_mock.assert_called_once()


class TestWriteaheadRegistry:
    """Module-level begin/finish plumbing and record_outbound preference."""

    def test_begin_without_recorder_returns_none(self):
        assert begin_outbound_record("telegram", "123", "hi") is None

    def test_begin_finish_roundtrip(self):
        rec = _FakeWriteaheadRecorder()
        set_outbound_writeahead_recorder(rec)
        handle = begin_outbound_record(
            "telegram", "123", "alert text", origin="cron job 'x'",
        )
        assert handle is not None
        finish_outbound_record(handle, True, platform_message_id="m-9")
        assert rec.events == [("begin", "alert text"), ("delivered", "m-9")]

    def test_finish_failure_marks_failed(self):
        rec = _FakeWriteaheadRecorder()
        set_outbound_writeahead_recorder(rec)
        handle = begin_outbound_record("telegram", "123", "alert text")
        finish_outbound_record(handle, False, error="ws down")
        assert rec.events[-1] == ("failed", "ws down")

    def test_record_outbound_prefers_writeahead(self):
        rec = _FakeWriteaheadRecorder()
        legacy = MagicMock()
        set_outbound_writeahead_recorder(rec)
        set_outbound_recorder(legacy)
        assert record_outbound(
            "telegram", "123", "text", platform_message_id="m-1",
        ) is True
        assert rec.events == [("begin", "text"), ("delivered", "m-1")]
        legacy.assert_not_called()

    def test_record_outbound_falls_back_to_legacy(self):
        legacy = MagicMock()
        set_outbound_recorder(legacy)
        assert record_outbound("telegram", "123", "text") is True
        legacy.assert_called_once()

    def test_begin_blank_text_returns_none(self):
        rec = _FakeWriteaheadRecorder()
        set_outbound_writeahead_recorder(rec)
        assert begin_outbound_record("telegram", "123", "   ") is None
        assert rec.events == []


class TestGatewayWriteaheadRecorder:
    """The gateway-side recorder: write-ahead append, pmid stamp, failure note."""

    def _recorder(self, store):
        from types import SimpleNamespace

        from gateway.run import _OutboundWriteaheadRecorder

        entry = store.get_or_create_session(_dm_source())
        runner = SimpleNamespace(
            session_store=store,
            _resolve_outbound_session_entry=lambda p, c, t: entry,
        )
        return _OutboundWriteaheadRecorder(runner), entry

    def test_begin_appends_turn_before_outcome(self, store):
        recorder, entry = self._recorder(store)
        handle = recorder.begin(
            "telegram", "12345", "porper draft ready", "cron job 'alerts'", None,
        )
        assert handle["session_id"] == entry.session_id
        assert handle["row_id"]
        msgs = store._db.get_messages(entry.session_id)
        assert len(msgs) == 1
        assert msgs[0]["role"] == "assistant"
        assert msgs[0]["content"].startswith(OUT_OF_BAND_PREFIX)
        assert "porper draft ready" in msgs[0]["content"]

    def test_mark_delivered_stamps_platform_message_id(self, store):
        recorder, entry = self._recorder(store)
        handle = recorder.begin("telegram", "12345", "hello", "origin", None)
        recorder.mark_delivered(handle, "imsg-778")
        msgs = store._db.get_messages(entry.session_id)
        assert msgs[0].get("message_id") == "imsg-778" or \
            msgs[0].get("platform_message_id") == "imsg-778"

    def test_mark_failed_appends_failure_note(self, store):
        recorder, entry = self._recorder(store)
        handle = recorder.begin("telegram", "12345", "hello", "origin", None)
        recorder.mark_failed(handle, "bridge websocket closed")
        msgs = store._db.get_messages(entry.session_id)
        assert len(msgs) == 2
        assert msgs[1]["content"].startswith(DELIVERY_FAILED_PREFIX)
        assert "bridge websocket closed" in msgs[1]["content"]


class TestCronWriteaheadOrdering:
    """Cron deliveries must be recorded BEFORE the send and resolved after."""

    def _run(self, events, live_result=None, standalone_result=None,
             live_exception=None):
        import asyncio
        import threading

        from cron.scheduler import _deliver_result

        pconfig = MagicMock()
        pconfig.enabled = True
        pconfig.extra = {}
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        async def _send(*a, **k):
            events.append(("send",))
            if live_exception:
                raise live_exception
            return live_result

        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        try:
            with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
                 patch("gateway.delivery.DeliveryRouter._deliver_to_platform",
                       new=_send), \
                 patch("tools.send_message_tool._send_to_platform",
                       new=AsyncMock(return_value=standalone_result or {"error": "no standalone"})):
                job = {
                    "id": "job-1",
                    "name": "Chessreps critical alerts",
                    "deliver": "origin",
                    "origin": {"platform": "telegram", "chat_id": "123"},
                }
                return _deliver_result(
                    job, "polborta case needs you",
                    adapters={Platform.TELEGRAM: MagicMock()}, loop=loop,
                )
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)
            loop.close()

    def test_record_precedes_send_and_resolves_delivered(self):
        events = []
        rec = _FakeWriteaheadRecorder(events)
        set_outbound_writeahead_recorder(rec)
        err = self._run(
            events, live_result={"success": True, "message_id": "imsg-42"},
        )
        assert err is None
        kinds = [e[0] for e in events]
        assert kinds == ["begin", "send", "delivered"]
        assert events[-1] == ("delivered", "imsg-42")

    def test_all_attempts_failed_marks_failed_once(self):
        events = []
        rec = _FakeWriteaheadRecorder(events)
        set_outbound_writeahead_recorder(rec)
        err = self._run(
            events,
            live_result={"success": False, "error": "live boom"},
            standalone_result={"error": "standalone boom"},
        )
        assert err is not None
        kinds = [e[0] for e in events]
        assert kinds.count("begin") == 1
        assert kinds.count("failed") == 1
        assert "delivered" not in kinds
        assert "standalone boom" in events[-1][1]

    def test_standalone_fallback_success_resolves_delivered(self):
        events = []
        rec = _FakeWriteaheadRecorder(events)
        set_outbound_writeahead_recorder(rec)
        err = self._run(
            events,
            live_result={"success": False, "error": "live boom"},
            standalone_result={"success": True, "message_id": "sa-7"},
        )
        assert err is None
        assert events[-1] == ("delivered", "sa-7")
        assert [e[0] for e in events].count("begin") == 1


class TestWebhookWriteahead:
    """Webhook direct-deliveries record write-ahead and resolve outcomes."""

    def _make_adapter(self, send_result):
        from gateway.platforms.webhook import WebhookAdapter

        adapter = WebhookAdapter.__new__(WebhookAdapter)
        target_adapter = MagicMock()
        target_adapter.send = AsyncMock(return_value=send_result)
        runner = MagicMock()
        runner.adapters = {Platform.TELEGRAM: target_adapter}
        adapter.gateway_runner = runner
        return adapter

    @pytest.mark.asyncio
    async def test_success_begins_then_marks_delivered(self):
        from gateway.platforms.base import SendResult

        events = []
        rec = _FakeWriteaheadRecorder(events)
        set_outbound_writeahead_recorder(rec)
        adapter = self._make_adapter(SendResult(success=True, message_id="w-5"))
        result = await adapter._deliver_cross_platform(
            "telegram", "codex done", {"deliver_extra": {"chat_id": "123"}},
        )
        assert result.success
        assert [e[0] for e in events] == ["begin", "delivered"]
        assert events[-1] == ("delivered", "w-5")

    @pytest.mark.asyncio
    async def test_failure_marks_failed(self):
        from gateway.platforms.base import SendResult

        events = []
        rec = _FakeWriteaheadRecorder(events)
        set_outbound_writeahead_recorder(rec)
        adapter = self._make_adapter(SendResult(success=False, error="nope"))
        result = await adapter._deliver_cross_platform(
            "telegram", "text", {"deliver_extra": {"chat_id": "123"}},
        )
        assert not result.success
        assert events[0][0] == "begin"
        assert events[-1] == ("failed", "nope")


class TestWebhookDirectDeliverRecords:
    """Webhook deliver_only routes must record successful cross-platform sends."""

    def _make_adapter(self, send_result):
        from gateway.platforms.base import SendResult
        from gateway.platforms.webhook import WebhookAdapter

        adapter = WebhookAdapter.__new__(WebhookAdapter)
        target_adapter = MagicMock()
        target_adapter.send = AsyncMock(return_value=send_result)
        runner = MagicMock()
        runner.adapters = {Platform.TELEGRAM: target_adapter}
        adapter.gateway_runner = runner
        return adapter, SendResult

    @pytest.mark.asyncio
    async def test_successful_direct_deliver_is_recorded(self):
        from gateway.platforms.base import SendResult

        adapter, _ = self._make_adapter(SendResult(success=True))
        record_mock = MagicMock(return_value=True)
        with patch("gateway.outbound_memory.record_outbound", record_mock):
            result = await adapter._deliver_cross_platform(
                "telegram", "codex finished the task",
                {"deliver_extra": {"chat_id": "123"}},
            )
        assert result.success
        record_mock.assert_called_once()
        args, kwargs = record_mock.call_args
        assert args[0] == "telegram"
        assert args[1] == "123"
        assert args[2] == "codex finished the task"

    @pytest.mark.asyncio
    async def test_failed_direct_deliver_is_not_recorded(self):
        from gateway.platforms.base import SendResult

        adapter, _ = self._make_adapter(SendResult(success=False, error="nope"))
        record_mock = MagicMock(return_value=True)
        with patch("gateway.outbound_memory.record_outbound", record_mock):
            result = await adapter._deliver_cross_platform(
                "telegram", "text", {"deliver_extra": {"chat_id": "123"}},
            )
        assert not result.success
        record_mock.assert_not_called()


class TestFormatOutboundRecord:
    def test_marker_and_text_present(self):
        rendered = format_outbound_record("hello", "cron job 'x'")
        assert rendered.startswith(OUT_OF_BAND_PREFIX)
        assert "cron job 'x'" in rendered
        assert rendered.endswith("hello")
