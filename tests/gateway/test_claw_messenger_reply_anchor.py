"""Swipe-reply metadata extraction in the Claw Messenger adapter.

An iMessage swipe-reply must surface the quoted message's id (and text when
the relay provides it) on the MessageEvent, so the gateway can inject the
"[Replying to: ...]" pointer — resolving GUID-only quotes from the transcript
via platform_message_id. Before 2026-07-06 the adapter dropped these fields
at ingestion, so replying to a specific alert carried no referent at all.
"""
from unittest.mock import AsyncMock

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
    adapter = adapter_mod.ClawMessengerAdapter(cfg)
    adapter.handle_message = AsyncMock()
    adapter._send_json = AsyncMock()
    return adapter


def _payload(**extra):
    base = {
        "type": "message",
        "from": "+15551234567",
        "text": "send",
        "messageId": "guid-inbound-1",
    }
    base.update(extra)
    return base


async def _inbound_event(adapter, payload):
    await adapter._handle_inbound_message(payload)
    adapter.handle_message.assert_awaited_once()
    return adapter.handle_message.await_args.args[0]


class TestReplyMetadataExtraction:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("key", [
        "replyToMessageId", "replyToId", "replyTo", "quotedMessageId",
        "threadOriginatorGuid", "associatedMessageGuid",
    ])
    async def test_guid_only_reply_key_variants(self, key):
        adapter = _make_adapter()
        event = await _inbound_event(adapter, _payload(**{key: "guid-quoted-9"}))
        assert event.reply_to_message_id == "guid-quoted-9"
        assert event.reply_to_text is None

    @pytest.mark.asyncio
    async def test_dict_shaped_reply_carries_text(self):
        adapter = _make_adapter()
        event = await _inbound_event(adapter, _payload(
            replyTo={"messageId": "guid-quoted-9", "text": "porper draft ready"},
        ))
        assert event.reply_to_message_id == "guid-quoted-9"
        assert event.reply_to_text == "porper draft ready"

    @pytest.mark.asyncio
    async def test_separate_quoted_text_key(self):
        adapter = _make_adapter()
        event = await _inbound_event(adapter, _payload(
            replyToMessageId="guid-quoted-9", replyToText="the alert body",
        ))
        assert event.reply_to_message_id == "guid-quoted-9"
        assert event.reply_to_text == "the alert body"

    @pytest.mark.asyncio
    async def test_plain_message_has_no_reply_fields(self):
        adapter = _make_adapter()
        event = await _inbound_event(adapter, _payload())
        assert event.reply_to_message_id is None
        assert event.reply_to_text is None
