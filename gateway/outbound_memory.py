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
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# Marker prefix used for recorded out-of-band messages. Kept short and
# greppable; the recap builder and tests key off it.
OUT_OF_BAND_PREFIX = "[out-of-band message sent to this chat"

_lock = threading.Lock()
_recorder: Optional[Callable[[str, str, str, str, Optional[str]], None]] = None


def set_outbound_recorder(
    recorder: Optional[Callable[[str, str, str, str, Optional[str]], None]],
) -> None:
    """Register (or clear, with ``None``) the process-wide outbound recorder.

    The recorder receives ``(platform_name, chat_id, text, origin, thread_id)``.
    """
    global _recorder
    with _lock:
        _recorder = recorder


def format_outbound_record(text: str, origin: str) -> str:
    """Render the transcript content for a recorded out-of-band delivery."""
    return (
        f"{OUT_OF_BAND_PREFIX} by {origin}, outside a conversation turn. "
        "The user sees it as a message from you — treat it as something you "
        "said, and expect replies to refer to it.]\n"
        f"{text}"
    )


def record_outbound(
    platform_name: str,
    chat_id: str,
    text: str,
    origin: str = "an out-of-band delivery",
    thread_id: Optional[str] = None,
) -> bool:
    """Record a successfully delivered out-of-band message.

    Returns True if a recorder was registered and accepted the message.
    Never raises: delivery already happened, so failures here are logged
    and swallowed.
    """
    with _lock:
        recorder = _recorder
    if recorder is None:
        return False
    if not platform_name or not chat_id or not (text or "").strip():
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
