# Conversation as source of truth: every phone message is a respondable turn

**Date:** 2026-07-06
**Context:** the July 4 "send" amnesia and July 6 "polborata" incidents. Both were the same
failure class: something texted Ethan outside the DM agent's transcript, and the agent later
denied knowledge of it. The July 5 outbound-memory patch closed this best-effort; the v0.18
merge silently regressed it for 16 hours (fixed in `3d18d5300d`). Ethan's directive: *"we need
all messages to move through conversation for sure so I can respond to them."*

**Goal:** it must be impossible, by construction, for a message to reach Ethan's phone without
becoming a turn in the conversation his next reply will hit — and replies (including iMessage
swipe-replies) should resolve to the specific message they answer.

## Research findings (full send-path inventory, 2026-07-06)

Every production path that can reach the phone, and how it records today:

| Path | Records? | Mechanism |
| --- | --- | --- |
| Gateway agent replies | yes | normal assistant turn (`run.py:11775`) |
| Cron text, live-adapter branch | yes (since 3d18d5300d) | `record_outbound` (`scheduler.py:1793`) |
| Cron text, standalone branch | yes | `record_outbound` (`scheduler.py:1897`) |
| Cron thread/in-channel seeded briefs | yes | `_seed_cron_*_session` / mirror |
| Webhook deliver_only (codex-notify → notify-ethan.sh, appserver-dispatch.py) | yes | `record_outbound` (`webhook.py:1220`) |
| Alert scripts (critical-alerts, meeting-reminders, gmail-triage) | yes | stdout → cron scheduler paths above |
| `send_message` tool / `hermes send` CLI / MCP `send_message` | **conditional** | `mirror_to_session` — appends only if the target chat already has a session; first-ever message to a new chat is delivered but NOT recorded |
| Cron media attachments (`_send_media_via_adapter`, `scheduler.py:1175`) | **no** | text body records, attachments leave no trace |

Supporting facts:

- Recording is **caller-discipline**: 5 call sites, 3 mechanisms (`record_outbound`,
  `mirror_to_session`, seeds), all record-*after*-send, all best-effort by contract. Nothing
  verifies the invariant; a regression is silent (exactly how July 6 happened — the standalone
  path had a passing test while the live path was dead).
- `BasePlatformAdapter.send()` is the single boundary every outbound message crosses and
  returns `SendResult.message_id`. The claw adapter returns the bridge `messageId`
  (`claw_messenger/adapter.py:302`), but every recording path discards it. The
  `messages.platform_message_id` column exists and is populated only for inbound turns (dedup).
- Inbound swipe-reply metadata is dropped at ingestion: the claw adapter never sets
  `reply_to_message_id`/`reply_to_text` on `MessageEvent` (`adapter.py:481-497`), though the
  dataclass supports both and `run.py:10509` already consumes them. The loss is in the adapter
  (and possibly the bridge payload).
- Session policy: 4h idle / 4am daily reset, 12-message carryover recap. Recorded deliveries
  bump session freshness, so with recording enforced, resets are no longer the amnesia vector.

## Non-goals

- Routing alerts through live agent turns. Deterministic scripts stay the senders (cost,
  latency, reliability, and the freshly-tuned alert-composer contract + golden evals stay
  untouched). "Move through conversation" means the transcript, not the agent loop.
- Local Hermes UI/TUI sessions, github_comment/log webhook sinks — not phone paths.

## Phase 1 — one write-ahead choke point (the "for sure")

Replace scattered record-after-send with a single `record_then_send()` in
`gateway/outbound_memory.py`:

1. Resolve the target session (existing `find_source_for_chat` + `get_or_create_session` —
   already handles expired sessions by creating the successor with a carryover pointer).
2. Append the out-of-band turn **before** sending, with metadata `delivery_status: sending`.
3. Send via the existing per-path mechanics (DeliveryRouter / `_send_to_platform` / webhook).
4. Update the row: `delivered` + `platform_message_id` from `SendResult`, or `failed` + error.

Failure semantics invert to the safe side: today's failure mode is "on phone, not in
transcript" (invisible); after this it's "in transcript, marked failed" (visible, retryable).
If the transcript write itself fails, the send is aborted and the delivery error propagates to
the existing per-path error handling — loud, not silent.

Callers to convert: cron scheduler (both branches — collapses the two guard expressions),
webhook `_deliver_cross_platform`, `send_message` tool (fixes the session-less-chat gap: fall
back from mirror to the choke point, which creates the session).

## Phase 2 — invariant canary (regression-proof, catches unknown paths)

Belt-and-braces at the adapter boundary, since Phase 1 still relies on callers using the choke
point:

- A lightweight post-send hook on `BasePlatformAdapter.send()` success appends to a send-ledger
  (SQLite table in state.db: ts, platform, chat_id, content hash, message_id, and a
  `session_turn`/`recorded` marker threaded via `metadata` by legit callers).
- An in-gateway periodic task (piggyback on an existing maintenance loop) reconciles the ledger
  against transcript rows for DM chats. Any successful send with no matching transcript turn →
  ERROR log + one throttled page via the codex-notify webhook (which is itself recorded).
- One line in the daily briefing: "outbound/transcript reconciliation: N sends, 0 unrecorded."

This is the piece that would have caught the v0.18 regression at 01:00 instead of via a
screenshot at 13:13 — and it covers paths that don't exist yet.

## Phase 3 — close the two known gaps (small, immediate)

- **Cron media:** record a companion turn `[sent N attachment(s): names]` in
  `_send_media_via_adapter` (Phase 1 choke point handles it once media goes through it).
- **send tool / CLI / MCP to a session-less chat:** covered by Phase 1's fallback; until then,
  a two-line fix — when `mirror_to_session` returns False, call `record_outbound`.

## Phase 4 — reply anchoring (respond to a *specific* message)

1. Persist outbound `platform_message_id` on recorded turns (Phase 1 step 4 provides it; write
   it to the existing column — today inbound-only).
2. Inspect the claw-messenger bridge payload for swipe-replies (bridge is Ethan's own
   claw-messenger.onrender.com + Mac relay — extend the relay payload if reply metadata is
   missing; iMessage exposes `associated_message_guid` for replies/tapbacks).
3. Claw adapter: populate `MessageEvent.reply_to_message_id`/`reply_to_text` at
   `adapter.py:481-497`. Downstream plumbing already consumes them.
4. Gateway: when an inbound reply_to matches a recorded outbound row, prepend
   `[replying to your message: "…"]` to the turn — a swipe-reply to a 3-day-old alert then
   resolves even across resets, without any search.

## Phase 5 — reset ergonomics (config-only, defense in depth)

- Raise `carryover_messages` 12 → 25 for the claw_messenger DM (alert bursts are chatty).
- Optionally: recap builder always includes all out-of-band turns newer than the last user
  turn, regardless of the cap (an alert Ethan hasn't answered yet must never fall out of the
  recap window).

## Verification

- Unit: property-style test — every production `_deliver_result`/webhook/send-tool branch ends
  with exactly one transcript write per delivery (kills the July 6 class: the old suite passed
  while the normal path was dead).
- E2E after each phase: post a signed test message to codex-notify → assert transcript row with
  `delivered` + message_id; disable the recorder in a test gateway → assert the canary pages.
- Golden alert-composer eval must be untouched (no composer changes anywhere in this plan).

## Order and effort

1 and 2 are the guarantee and should land together (~a day, mostly in `outbound_memory.py`,
`scheduler.py`, `webhook.py`, plus the reconciler). 3 is minutes if 1 lands first. 4 needs a
bridge-payload research spike before sizing (adapter+gateway side is small). 5 is config.
