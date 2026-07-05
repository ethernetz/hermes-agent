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
    OUT_OF_BAND_PREFIX,
    format_outbound_record,
    record_outbound,
    set_outbound_recorder,
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
    """Recorder is process-global state — never leak it between tests."""
    set_outbound_recorder(None)
    yield
    set_outbound_recorder(None)


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
