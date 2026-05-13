import supportsHyperlinksLib from 'supports-hyperlinks'

// Additional terminals that support OSC 8 hyperlinks but aren't detected by supports-hyperlinks.
// Checked against both TERM_PROGRAM and LC_TERMINAL (the latter is preserved inside tmux).
export const ADDITIONAL_HYPERLINK_TERMINALS = ['ghostty', 'Hyper', 'kitty', 'alacritty', 'iTerm.app', 'iTerm2']

// Apple Terminal.app added OSC 8 hyperlink support in version ~421 (macOS 12+).
// `supports-hyperlinks` doesn't recognize it, so detect by TERM_PROGRAM_VERSION.
// Version env var is a flat integer like '455' or dotted like '455.1' — accept both.
// User opens links via Cmd+Click in Terminal.app; underlining + accent color from the
// Link component still gives the visual affordance.
export const APPLE_TERMINAL_MIN_VERSION = 421

type EnvLike = Record<string, string | undefined>

type SupportsHyperlinksOptions = {
  env?: EnvLike
  stdoutSupported?: boolean
}

function parseAppleTerminalMajor(version: string | undefined): null | number {
  if (!version) {
    return null
  }

  // Apple ships values like '455' (build) or '455.1' (build.point). The point release
  // is irrelevant for protocol support; the leading integer is the only part we need.
  const match = /^(\d+)/.exec(version.trim())

  if (!match) {
    return null
  }

  const parsed = parseInt(match[1]!, 10)

  return Number.isFinite(parsed) ? parsed : null
}

export function isAppleTerminalWithHyperlinks(env: EnvLike = process.env): boolean {
  if (env['TERM_PROGRAM'] !== 'Apple_Terminal') {
    return false
  }

  const major = parseAppleTerminalMajor(env['TERM_PROGRAM_VERSION'])

  return major !== null && major >= APPLE_TERMINAL_MIN_VERSION
}

/**
 * Returns whether stdout supports OSC 8 hyperlinks.
 * Extends the supports-hyperlinks library with additional terminal detection.
 * @param options Optional overrides for testing (env, stdoutSupported)
 */
export function supportsHyperlinks(options?: SupportsHyperlinksOptions): boolean {
  const stdoutSupported = options?.stdoutSupported ?? supportsHyperlinksLib.stdout

  if (stdoutSupported) {
    return true
  }

  const env = options?.env ?? process.env

  // Check for additional terminals not detected by supports-hyperlinks
  const termProgram = env['TERM_PROGRAM']

  if (termProgram && ADDITIONAL_HYPERLINK_TERMINALS.includes(termProgram)) {
    return true
  }

  // Apple Terminal.app — version-gated so we don't enable on ancient builds that
  // would render the OSC 8 sequence as raw text. Ships with macOS, so this fires
  // for the basic Terminal.app path users hit by default on macOS.
  if (isAppleTerminalWithHyperlinks(env)) {
    return true
  }

  // LC_TERMINAL is set by some terminals (e.g. iTerm2) and preserved inside tmux,
  // where TERM_PROGRAM is overwritten to 'tmux'.
  const lcTerminal = env['LC_TERMINAL']

  if (lcTerminal && ADDITIONAL_HYPERLINK_TERMINALS.includes(lcTerminal)) {
    return true
  }

  // Kitty sets TERM=xterm-kitty
  const term = env['TERM']

  if (term?.includes('kitty')) {
    return true
  }

  return false
}
