"""Silence-token suppression in the Claw Messenger adapter.

When the agent's entire response is a silence token (``[SILENT]`` or the
legacy ``NO_REPLY``), the adapter must confirm delivery without sending a
literal bubble over iMessage. Mixed content (prose + token) still delivers.
"""
import pytest

from gateway.config import PlatformConfig

from ._plugin_adapter_loader import load_plugin_adapter

adapter_mod = load_plugin_adapter("claw_messenger")


def _make_adapter():
    cfg = PlatformConfig(
        enabled=True,
        extra={
            "server_url": "wss://example.invalid",
            "api_key": "test-key",
        },
    )
    return adapter_mod.ClawMessengerAdapter(cfg)


class TestSilenceTokenDetection:
    @pytest.mark.parametrize(
        "text",
        [
            "[SILENT]",
            "[silent]",
            "NO_REPLY",
            "no_reply",
            "  NO_REPLY  ",
            "`NO_REPLY`",
            '"[SILENT]"',
            "NO_REPLY.",
        ],
    )
    def test_exact_tokens_detected(self, text):
        assert adapter_mod._is_silence_token(text) is True

    @pytest.mark.parametrize(
        "text",
        [
            "done, sent them all. NO_REPLY",
            "[SILENT] but also here's the answer",
            "reply NO_REPLY if you don't need anything",
            "no reply needed",
            "silent",
            "",
            "   ",
        ],
    )
    def test_mixed_or_plain_content_not_detected(self, text):
        assert adapter_mod._is_silence_token(text) is False


class TestSendSuppression:
    @pytest.mark.asyncio
    async def test_silence_token_send_is_suppressed(self):
        adapter = _make_adapter()

        async def _fail_request(payload, timeout=30):  # pragma: no cover
            raise AssertionError("silence token must not reach the relay")

        adapter._request = _fail_request
        result = await adapter.send(chat_id="+15551234567", content="NO_REPLY")
        assert result.success is True
        assert result.message_id == ""

    @pytest.mark.asyncio
    async def test_normal_content_still_sends(self):
        adapter = _make_adapter()
        sent = {}

        async def _capture_request(payload, timeout=30):
            sent.update(payload)
            return {"ok": True, "messageId": "m1"}

        adapter._request = _capture_request
        result = await adapter.send(chat_id="+15551234567", content="hey, build passed")
        assert result.success is True
        assert sent["parts"][0]["value"] == "hey, build passed"
