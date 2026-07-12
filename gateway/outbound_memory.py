"""
Out-of-band outbound delivery memory.

Messages delivered to a chat outside a normal agent turn — cron job output,
webhook ``deliver_only`` routes — historically went straight to the platform
adapter and never touched the session transcript. The user saw them as
messages "from the agent", but the agent had no record of them: a reply like
"send" landed in a session that never said anything, and the agent answered
with a blank "what do you need?".

This module closes that gap. The gateway registers a recorder at startup
(see ``GatewayRunner._record_outbound_delivery``); delivery paths call
:func:`record_outbound` after a successful send, and the recorder appends the
message to the target chat's session transcript as an assistant turn. The
next inbound message then lands in a conversation where the agent can see
what was just sent on its behalf.

Everything here is best-effort: recording must never break a delivery.
"""

import logging
import threading
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# Marker prefix used for recorded out-of-band messages. Kept short and
# greppable; the recap builder and tests key off it.
OUT_OF_BAND_PREFIX = "[out-of-band message sent to this chat"

# Follow-up note appended when a write-ahead-recorded delivery fails, so the
# agent knows the user never saw the message above it.
DELIVERY_FAILED_PREFIX = "[delivery status: the out-of-band message above FAILED to deliver"
DELIVERY_UNKNOWN_PREFIX = "[delivery status: confirmation for the out-of-band message above is UNKNOWN"

_lock = threading.Lock()
_recorder: Optional[Callable[[str, str, str, str, Optional[str]], None]] = None
# Write-ahead recorder: an object with
#   begin(platform_name, chat_id, text, origin, thread_id) -> handle | None
#   mark_delivered(handle, platform_message_id) -> None
#   mark_failed(handle, error) -> None
_writeahead_recorder: Optional[Any] = None


def set_outbound_recorder(
    recorder: Optional[Callable[[str, str, str, str, Optional[str]], None]],
) -> None:
    """Register (or clear, with ``None``) the process-wide outbound recorder.

    The recorder receives ``(platform_name, chat_id, text, origin, thread_id)``.
    """
    global _recorder
    with _lock:
        _recorder = recorder


def set_outbound_writeahead_recorder(recorder: Optional[Any]) -> None:
    """Register (or clear) the process-wide WRITE-AHEAD outbound recorder.

    Unlike the post-hoc recorder above, this one is invoked BEFORE the
    platform send (``begin``) and again after the outcome is known
    (``mark_delivered`` / ``mark_failed``), so a message can never reach the
    user's chat without already being a turn in the conversation their next
    reply will hit. When registered, ``record_outbound`` also routes through
    it (begin + mark_delivered).
    """
    global _writeahead_recorder
    with _lock:
        _writeahead_recorder = recorder


def format_outbound_record(text: str, origin: str) -> str:
    """Render the transcript content for a recorded out-of-band delivery."""
    return (
        f"{OUT_OF_BAND_PREFIX} by {origin}, outside a conversation turn. "
        "The user sees it as a message from you — treat it as something you "
        "said, and expect replies to refer to it.]\n"
        f"{text}"
    )


def begin_outbound_record(
    platform_name: str,
    chat_id: str,
    text: str,
    origin: str = "an out-of-band delivery",
    thread_id: Optional[str] = None,
) -> Optional[Any]:
    """Write-ahead: record an out-of-band message BEFORE it is sent.

    Returns an opaque handle to pass to :func:`finish_outbound_record`, or
    None when no write-ahead recorder is registered (caller should fall back
    to post-hoc :func:`record_outbound` on success) or the record failed.
    Never raises.
    """
    with _lock:
        recorder = _writeahead_recorder
    if recorder is None:
        return None
    if not platform_name or not chat_id or not (text or "").strip():
        return None
    try:
        return recorder.begin(str(platform_name), str(chat_id), text, origin, thread_id)
    except Exception:
        logger.warning(
            "outbound-memory: write-ahead record failed for %s:%s",
            platform_name, chat_id, exc_info=True,
        )
        return None


def finish_outbound_record(
    handle: Any,
    success: bool,
    platform_message_id: Optional[str] = None,
    error: Optional[str] = None,
    ambiguous: bool = False,
) -> None:
    """Resolve a write-ahead record with the delivery outcome.

    On success, stamps the recorded turn with the platform message id (when
    the adapter returned one) for reply anchoring. On failure, appends a
    follow-up note so the agent knows the user never saw the message.
    Never raises.
    """
    if handle is None:
        return
    with _lock:
        recorder = _writeahead_recorder
    if recorder is None:
        return
    try:
        if success:
            recorder.mark_delivered(handle, platform_message_id)
        elif ambiguous and hasattr(recorder, "mark_ambiguous"):
            recorder.mark_ambiguous(handle, error or "delivery outcome unknown")
        else:
            recorder.mark_failed(handle, error or "delivery failed")
    except Exception:
        logger.warning(
            "outbound-memory: failed to resolve write-ahead record",
            exc_info=True,
        )


def record_outbound(
    platform_name: str,
    chat_id: str,
    text: str,
    origin: str = "an out-of-band delivery",
    thread_id: Optional[str] = None,
    platform_message_id: Optional[str] = None,
) -> bool:
    """Record a successfully delivered out-of-band message (post-hoc).

    Prefers the write-ahead recorder when registered (begin + mark_delivered
    in one step); otherwise uses the legacy post-hoc recorder callable.
    Returns True if a recorder was registered and accepted the message.
    Never raises: delivery already happened, so failures here are logged
    and swallowed.
    """
    if not platform_name or not chat_id or not (text or "").strip():
        return False
    with _lock:
        wa_recorder = _writeahead_recorder
        recorder = _recorder
    if wa_recorder is not None:
        try:
            handle = wa_recorder.begin(
                str(platform_name), str(chat_id), text, origin, thread_id
            )
            if handle is not None:
                wa_recorder.mark_delivered(handle, platform_message_id)
                return True
        except Exception:
            logger.warning(
                "outbound-memory: failed to record delivery to %s:%s",
                platform_name, chat_id, exc_info=True,
            )
            return False
    if recorder is None:
        return False
    try:
        recorder(str(platform_name), str(chat_id), text, origin, thread_id)
        return True
    except Exception:
        logger.warning(
            "outbound-memory: failed to record delivery to %s:%s",
            platform_name, chat_id, exc_info=True,
        )
        return False
