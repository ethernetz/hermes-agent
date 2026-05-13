import { describe, expect, it } from 'vitest'

import {
  APPLE_TERMINAL_MIN_VERSION,
  isAppleTerminalWithHyperlinks,
  supportsHyperlinks
} from './supports-hyperlinks.js'

describe('isAppleTerminalWithHyperlinks', () => {
  it('returns true for modern Apple Terminal (build version >= threshold)', () => {
    expect(
      isAppleTerminalWithHyperlinks({
        TERM_PROGRAM: 'Apple_Terminal',
        TERM_PROGRAM_VERSION: '455.1'
      })
    ).toBe(true)
  })

  it('returns true for the exact threshold version', () => {
    expect(
      isAppleTerminalWithHyperlinks({
        TERM_PROGRAM: 'Apple_Terminal',
        TERM_PROGRAM_VERSION: String(APPLE_TERMINAL_MIN_VERSION)
      })
    ).toBe(true)
  })

  it('returns false for Apple Terminal builds below the threshold', () => {
    expect(
      isAppleTerminalWithHyperlinks({
        TERM_PROGRAM: 'Apple_Terminal',
        TERM_PROGRAM_VERSION: String(APPLE_TERMINAL_MIN_VERSION - 1)
      })
    ).toBe(false)
  })

  it('returns false when TERM_PROGRAM_VERSION is missing or unparseable', () => {
    expect(isAppleTerminalWithHyperlinks({ TERM_PROGRAM: 'Apple_Terminal' })).toBe(false)
    expect(
      isAppleTerminalWithHyperlinks({
        TERM_PROGRAM: 'Apple_Terminal',
        TERM_PROGRAM_VERSION: 'not-a-version'
      })
    ).toBe(false)
  })

  it('returns false when TERM_PROGRAM is not Apple_Terminal', () => {
    expect(
      isAppleTerminalWithHyperlinks({
        TERM_PROGRAM: 'iTerm.app',
        TERM_PROGRAM_VERSION: '455.1'
      })
    ).toBe(false)
  })
})

describe('supportsHyperlinks', () => {
  it('returns true when supports-hyperlinks already says yes', () => {
    expect(supportsHyperlinks({ env: {}, stdoutSupported: true })).toBe(true)
  })

  it('returns true for known terminals via TERM_PROGRAM allowlist', () => {
    expect(
      supportsHyperlinks({
        env: { TERM_PROGRAM: 'ghostty' },
        stdoutSupported: false
      })
    ).toBe(true)
  })

  it('returns true for modern Apple Terminal even when supports-hyperlinks rejects it', () => {
    expect(
      supportsHyperlinks({
        env: { TERM_PROGRAM: 'Apple_Terminal', TERM_PROGRAM_VERSION: '455.1' },
        stdoutSupported: false
      })
    ).toBe(true)
  })

  it('returns false for ancient Apple Terminal builds', () => {
    expect(
      supportsHyperlinks({
        env: { TERM_PROGRAM: 'Apple_Terminal', TERM_PROGRAM_VERSION: '300' },
        stdoutSupported: false
      })
    ).toBe(false)
  })

  it('respects LC_TERMINAL inside tmux', () => {
    expect(
      supportsHyperlinks({
        env: { LC_TERMINAL: 'iTerm2', TERM_PROGRAM: 'tmux' },
        stdoutSupported: false
      })
    ).toBe(true)
  })

  it('returns true for kitty via TERM', () => {
    expect(
      supportsHyperlinks({
        env: { TERM: 'xterm-kitty' },
        stdoutSupported: false
      })
    ).toBe(true)
  })

  it('returns false for unknown terminals', () => {
    expect(
      supportsHyperlinks({
        env: { TERM: 'xterm-256color' },
        stdoutSupported: false
      })
    ).toBe(false)
  })
})
