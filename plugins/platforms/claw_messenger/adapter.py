"""
Claw Messenger platform adapter for Hermes.

This speaks the websocket protocol used by the OpenClaw
@emotion-machine/claw-messenger plugin:

- connect to {server_url}/ws?key={api_key}
- receive inbound {"type": "message", ...} events
- send outbound {"type": "send", "to"|"chatId": ..., "parts": [...]}

It intentionally keeps the platform surface small: direct text messages are the
primary workflow, with group chat IDs supported when Claw Messenger provides
them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

try:
    import websockets
except Exception:  # pragma: no cover - handled by check_requirements
    websockets = None

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

logger = logging.getLogger(__name__)

PHONE_RE = re.compile(r"^\+?\d{10,15}$")
NO_RECONNECT_CODES = {1008, 4003}


@dataclass
class ClawMessengerSettings:
    api_key: str
    server_url: str
    preferred_service: str
    allow_from: list[str]
    group_allow_from: list[str]
    dm_policy: str
    group_policy: str
    max_message_length: int


def _csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _settings(config: PlatformConfig) -> ClawMessengerSettings:
    extra = getattr(config, "extra", {}) or {}
    return ClawMessengerSettings(
        api_key=(
            os.getenv("CLAW_MESSENGER_API_KEY")
            or getattr(config, "api_key", None)
            or extra.get("api_key")
            or ""
        ).strip(),
        server_url=(
            os.getenv("CLAW_MESSENGER_SERVER_URL")
            or extra.get("server_url")
            or "wss://claw-messenger.onrender.com"
        ).strip(),
        preferred_service=(
            os.getenv("CLAW_MESSENGER_PREFERRED_SERVICE")
            or extra.get("preferred_service")
            or "iMessage"
        ).strip(),
        allow_from=(
            _csv(os.getenv("CLAW_MESSENGER_ALLOWED_USERS", ""))
            or [str(v).strip() for v in extra.get("allow_from", []) if str(v).strip()]
        ),
        group_allow_from=(
            _csv(os.getenv("CLAW_MESSENGER_GROUP_ALLOWED_CHATS", ""))
            or [str(v).strip() for v in extra.get("group_allow_from", []) if str(v).strip()]
        ),
        dm_policy=str(extra.get("dm_policy") or "allowlist").strip().lower(),
        group_policy=str(extra.get("group_policy") or "disabled").strip().lower(),
        max_message_length=int(extra.get("max_message_length") or 10000),
    )


def _normalize_server_url(server_url: str, api_key: str) -> str:
    raw = server_url.strip() or "wss://claw-messenger.onrender.com"
    parts = urlsplit(raw)
    scheme = parts.scheme
    if scheme == "https":
        scheme = "wss"
    elif scheme == "http":
        scheme = "ws"
    elif scheme not in {"ws", "wss"}:
        scheme = "wss"

    path = parts.path or ""
    if not path.endswith("/ws"):
        path = path.rstrip("/") + "/ws"

    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["key"] = api_key
    return urlunsplit((scheme, parts.netloc, path, urlencode(query), ""))


def _normalize_target(target: str) -> str:
    value = str(target or "").strip()
    for prefix in ("claw-messenger:", "linq:"):
        while value.startswith(prefix):
            value = value[len(prefix):].strip()
    return value


def _is_direct_phone(chat_id: str) -> bool:
    return bool(PHONE_RE.match(_normalize_target(chat_id)))


# Silence tokens: when the agent's ENTIRE response is one of these, the turn
# is intentionally silent (visible content was already delivered via the
# message tool or an out-of-band webhook). This fork's gateway has no shared
# silence filter (upstream added gateway/response_filters.py in 293c04fef,
# post-fork), so suppression lives here at the adapter boundary. The token is
# still recorded in session history so the model sees its own convention.
_SILENCE_TOKENS = {"[silent]", "no_reply"}


def _is_silence_token(text: str) -> bool:
    value = str(text or "").strip()
    # Tolerate model quirks: enclosing quotes/backticks and a trailing period.
    value = value.strip("`\"'").rstrip(".").strip()
    return value.lower() in _SILENCE_TOKENS


def _looks_like_image(value: str) -> bool:
    lower = str(value or "").lower().split("?", 1)[0]
    return lower.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".heic", ".heif"))


def _attachment_media_fields(attachments: Any) -> tuple[list[str], list[str], list[str]]:
    media_urls: list[str] = []
    media_types: list[str] = []
    summaries: list[str] = []
    if not isinstance(attachments, list):
        return media_urls, media_types, summaries

    for raw in attachments:
        att = raw if isinstance(raw, dict) else {"value": raw}
        url = (
            att.get("url")
            or att.get("downloadUrl")
            or att.get("download_url")
            or att.get("mediaUrl")
            or att.get("media_url")
            or att.get("path")
            or att.get("filePath")
            or att.get("localPath")
            or att.get("dataUrl")
            or att.get("value")
            or ""
        )
        mime = str(
            att.get("mimeType")
            or att.get("mime")
            or att.get("contentType")
            or att.get("content_type")
            or ""
        ).strip()
        name = str(
            att.get("name")
            or att.get("filename")
            or att.get("fileName")
            or att.get("displayName")
            or url
            or "attachment"
        ).strip()

        if url:
            media_urls.append(str(url))
            if not mime and _looks_like_image(str(url)):
                mime = "image/unknown"
            media_types.append(mime)

        bits = [name]
        if mime:
            bits.append(mime)
        if url and str(url) != name:
            bits.append(str(url))
        summaries.append(" (" + "; ".join(bits) + ")" if bits else "(attachment)")

    return media_urls, media_types, summaries


class ClawMessengerAdapter(BasePlatformAdapter):
    def __init__(self, config: PlatformConfig):
        platform = Platform("claw_messenger")
        super().__init__(config=config, platform=platform)
        self.settings = _settings(config)
        self.max_message_length = self.settings.max_message_length
        self._ws = None
        self._runner_task: Optional[asyncio.Task] = None
        self._connected_once = asyncio.Event()
        self._stopped = False
        self._pending: dict[str, asyncio.Future] = {}
        self._send_lock = asyncio.Lock()
        self._correlation_counter = 0
        self._seen_message_ids: set[str] = set()
        self._last_message_at: Optional[str] = None

    @property
    def name(self) -> str:
        return "Claw Messenger"

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        # is_reconnect (base contract since v0.18) exists for adapters that
        # buffer a server-side update queue across outages; the claw relay
        # has none, so the flag is accepted and ignored.
        if not self.settings.api_key:
            self._set_fatal_error("config_missing", "CLAW_MESSENGER_API_KEY is not set", retryable=False)
            return False
        if not self.settings.server_url:
            self._set_fatal_error("config_missing", "CLAW_MESSENGER_SERVER_URL is not set", retryable=False)
            return False

        if not self._acquire_platform_lock("claw_messenger", "default", "Claw Messenger websocket"):
            return False

        self._stopped = False
        self._connected_once.clear()
        self._runner_task = asyncio.create_task(self._run_forever())
        try:
            await asyncio.wait_for(self._connected_once.wait(), timeout=30)
            return True
        except asyncio.TimeoutError:
            self._set_fatal_error("connect_timeout", "Timed out connecting to Claw Messenger", retryable=True)
            await self.disconnect()
            return False

    async def disconnect(self) -> None:
        self._stopped = True
        self._mark_disconnected()
        self._reject_pending("Adapter disconnected")
        if self._ws is not None:
            try:
                await self._ws.close(code=1000, reason="Hermes gateway stopping")
            except Exception:
                pass
            self._ws = None
        if self._runner_task and not self._runner_task.done():
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
        self._release_platform_lock()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        text = str(content or "")
        if not text.strip():
            return SendResult(success=True, message_id="")

        if _is_silence_token(text):
            logger.info(
                "[claw_messenger] Suppressing silence token %r for %s",
                text.strip(), chat_id,
            )
            return SendResult(success=True, message_id="")

        target = _normalize_target(chat_id)
        if not target:
            return SendResult(success=False, error="Missing recipient")

        parts = [{"type": "text", "value": text}]
        payload: dict[str, Any] = {
            "type": "send",
            "parts": parts,
        }
        if self.settings.preferred_service:
            payload["service"] = self.settings.preferred_service

        if _is_direct_phone(target):
            payload["to"] = target
        else:
            payload["chatId"] = target

        try:
            response = await self._request(payload, timeout=30)
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

        if response.get("ok"):
            return SendResult(
                success=True,
                message_id=str(response.get("messageId") or ""),
                raw_response=response,
            )
        return SendResult(success=False, error=str(response.get("error") or "Send failed"), raw_response=response)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        target = _normalize_target(chat_id)
        if not target or not _is_direct_phone(target):
            return
        try:
            await self._send_json({"type": "typing.start", "to": target})
        except Exception:
            pass

    async def stop_typing(self, chat_id: str) -> None:
        target = _normalize_target(chat_id)
        if not target or not _is_direct_phone(target):
            return
        try:
            await self._send_json({"type": "typing.stop", "to": target})
        except Exception:
            pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        target = _normalize_target(chat_id)
        is_dm = _is_direct_phone(target)
        return {
            "name": target,
            "type": "dm" if is_dm else "group",
        }

    async def _run_forever(self) -> None:
        attempt = 0
        while not self._stopped:
            try:
                await self._connect_once()
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[claw_messenger] websocket error: %s", exc)
                self._mark_disconnected()
                self._reject_pending("Connection reset")

            if self._stopped:
                break

            attempt += 1
            delay = min(30, 1.5 * attempt)
            await asyncio.sleep(delay)

    async def _connect_once(self) -> None:
        if websockets is None:
            raise RuntimeError("websockets is not installed")

        url = _normalize_server_url(self.settings.server_url, self.settings.api_key)
        safe_url = url.split("?", 1)[0]
        logger.info("[claw_messenger] connecting to %s", safe_url)

        async with websockets.connect(url, ping_interval=None, close_timeout=10) as ws:
            self._ws = ws
            self._mark_connected()
            self._connected_once.set()
            logger.info("[claw_messenger] connected")
            if self._last_message_at:
                await self._send_json({"type": "sync", "since": self._last_message_at})

            try:
                async for raw in ws:
                    await self._handle_ws_raw(raw)
            except websockets.exceptions.ConnectionClosed as exc:
                if exc.code in NO_RECONNECT_CODES:
                    self._set_fatal_error(
                        "connection_closed",
                        f"Claw Messenger closed the connection with code {exc.code}",
                        retryable=False,
                    )
                    self._stopped = True
                raise
            finally:
                if self._ws is ws:
                    self._ws = None
                self._mark_disconnected()
                self._reject_pending("Connection closed")

    async def _handle_ws_raw(self, raw: Any) -> None:
        try:
            data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else str(raw))
        except Exception as exc:
            logger.debug("[claw_messenger] ignoring unparseable websocket message: %s", exc)
            return

        correlation_id = data.get("id")
        if correlation_id and correlation_id in self._pending:
            fut = self._pending.pop(correlation_id)
            if not fut.done():
                fut.set_result(data)
            return

        msg_type = data.get("type")
        if msg_type == "ping":
            await self._send_json({"type": "pong"})
            return
        if msg_type == "pong":
            return
        if msg_type == "message":
            await self._handle_inbound_message(data)
            return
        if msg_type == "sync.done":
            logger.info("[claw_messenger] sync complete")
            return
        if msg_type == "error":
            logger.warning("[claw_messenger] server error: %s", data.get("message") or data.get("error") or "unknown")

    async def _handle_inbound_message(self, data: dict[str, Any]) -> None:
        # Some relays echo messages sent by Hermes/the local Messages app. Those
        # must not re-enter the agent as user prompts.
        direction = str(data.get("direction") or data.get("status") or "").strip().lower()
        if (
            data.get("fromMe") is True
            or data.get("isFromMe") is True
            or data.get("isOutgoing") is True
            or data.get("outgoing") is True
            or direction in {"outgoing", "sent", "from_me", "self"}
        ):
            logger.info("[claw_messenger] dropped outgoing/self message echo")
            return

        from_id = _normalize_target(str(data.get("from") or ""))
        text = str(data.get("text") or "")
        message_id = str(data.get("messageId") or "")
        is_group = data.get("isGroup") is True
        chat_id = str(data.get("chatId") or "")
        attachments = data.get("attachments") or []

        # Swipe-reply / quote metadata. The relay payload contract for
        # replies is not pinned down, so accept the plausible spellings
        # (BlueBubbles-style iMessage bridges use threadOriginatorGuid;
        # associatedMessageGuid also carries tapback targets). When only a
        # GUID arrives, the gateway resolves the quoted text from the
        # transcript via platform_message_id (outbound turns are stamped
        # with theirs by the write-ahead recorder).
        reply_to_id = None
        reply_to_text = None
        for _key in (
            "replyToMessageId", "replyToId", "replyTo", "quotedMessageId",
            "threadOriginatorGuid", "associatedMessageGuid",
        ):
            _val = data.get(_key)
            if isinstance(_val, dict):
                reply_to_id = str(
                    _val.get("messageId") or _val.get("guid") or ""
                ) or None
                reply_to_text = str(_val.get("text") or "") or None
            elif _val:
                reply_to_id = str(_val)
            if reply_to_id:
                break
        if not reply_to_text:
            for _key in ("replyToText", "quotedText"):
                if data.get(_key):
                    reply_to_text = str(data.get(_key))
                    break
        # Discovery: surface payload keys we don't consume yet, so the next
        # real swipe-reply reveals what the relay actually sends.
        _known_keys = {
            "type", "from", "text", "messageId", "isGroup", "chatId",
            "attachments", "direction", "status", "fromMe", "isFromMe",
            "isOutgoing", "outgoing", "timestamp", "chatName", "service",
            "replyToMessageId", "replyToId", "replyTo", "quotedMessageId",
            "threadOriginatorGuid", "associatedMessageGuid", "replyToText",
            "quotedText",
        }
        _extra_keys = sorted(set(data.keys()) - _known_keys)
        if _extra_keys:
            logger.info(
                "[claw_messenger] inbound payload has unconsumed keys: %s",
                _extra_keys,
            )

        media_urls, media_types, attachment_summaries = _attachment_media_fields(attachments)
        if attachments:
            keys = sorted({k for att in attachments if isinstance(att, dict) for k in att.keys()})
            logger.info("[claw_messenger] inbound attachment metadata: count=%d keys=%s", len(attachments), keys)

        if message_id:
            if message_id in self._seen_message_ids:
                return
            self._seen_message_ids.add(message_id)
            if len(self._seen_message_ids) > 1000:
                self._seen_message_ids = set(list(self._seen_message_ids)[-500:])

        self._last_message_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        if not is_group:
            if self.settings.dm_policy == "allowlist" and self.settings.allow_from and from_id not in self.settings.allow_from:
                logger.info("[claw_messenger] dropped unauthorized direct message")
                return
            session_chat_id = from_id
            chat_type = "dm"
            chat_name = from_id
        else:
            if self.settings.group_policy == "disabled":
                return
            if self.settings.group_policy == "allowlist" and self.settings.group_allow_from and chat_id not in self.settings.group_allow_from:
                logger.info("[claw_messenger] dropped unauthorized group message")
                return
            session_chat_id = chat_id
            chat_type = "group"
            chat_name = chat_id

        if attachment_summaries:
            media_note = "[User sent attachment(s): " + ", ".join(attachment_summaries) + "]"
            text = f"{text}\n\n{media_note}".strip() if text else media_note
        if not text:
            return

        message_type = MessageType.TEXT
        if any((m or "").startswith("image/") for m in media_types) or any(_looks_like_image(u) for u in media_urls):
            message_type = MessageType.PHOTO
        elif any((m or "").startswith("audio/") for m in media_types):
            message_type = MessageType.AUDIO

        source = self.build_source(
            chat_id=session_chat_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=from_id,
            user_name=from_id,
            message_id=message_id or None,
        )
        event = MessageEvent(
            text=text,
            message_type=message_type,
            source=source,
            raw_message=data,
            message_id=message_id or None,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=reply_to_id,
            reply_to_text=reply_to_text,
        )

        if not is_group and from_id:
            try:
                await self._send_json({"type": "read", "to": from_id})
            except Exception:
                pass

        await self.handle_message(event)

    async def _request(self, payload: dict[str, Any], timeout: float = 30) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        correlation_id = payload.get("id") or self._next_id()
        payload["id"] = correlation_id
        fut = loop.create_future()
        self._pending[correlation_id] = fut
        try:
            await self._send_json(payload)
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending.pop(correlation_id, None)

    async def _send_json(self, payload: dict[str, Any]) -> None:
        ws = self._ws
        if ws is None:
            raise RuntimeError("WebSocket not connected")
        async with self._send_lock:
            await ws.send(json.dumps(payload))

    def _next_id(self) -> str:
        self._correlation_counter += 1
        return f"hermes-{self._correlation_counter}-{int(time.time() * 1000)}"

    def _reject_pending(self, reason: str) -> None:
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(RuntimeError(reason))
        self._pending.clear()


def check_requirements() -> bool:
    return websockets is not None


def validate_config(config: PlatformConfig) -> bool:
    settings = _settings(config)
    return bool(settings.api_key and settings.server_url)


def is_connected(config: PlatformConfig) -> bool:
    return validate_config(config)


def register(ctx) -> None:
    ctx.register_platform(
        name="claw_messenger",
        label="Claw Messenger",
        adapter_factory=lambda cfg: ClawMessengerAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["CLAW_MESSENGER_API_KEY"],
        install_hint="websockets must be installed in the Hermes environment",
        allowed_users_env="CLAW_MESSENGER_ALLOWED_USERS",
        allow_all_env="CLAW_MESSENGER_ALLOW_ALL_USERS",
        max_message_length=10000,
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are chatting through Claw Messenger over iMessage/SMS. "
            "Keep replies concise and natural. Avoid markdown-heavy formatting."
        ),
    )
