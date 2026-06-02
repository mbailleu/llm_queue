#!/usr/bin/env bash
# Statusline helper for anthropic_proxy.
#
# It reports the proxy's own state (tier, in-flight, queue, window, throughput),
# which is the same regardless of which client drives the traffic — so a single
# bar covers Claude Code, opencode, and any other Anthropic/OpenAI client at once.
#
# Claude Code (~/.claude/settings.json):
#   {
#     "statusLine": {
#       "type": "command",
#       "command": "/absolute/path/to/statusline.sh"
#     }
#   }
#
# opencode has no native command-statusline, so surface the proxy line through
# the terminal multiplexer it runs inside:
#
#   tmux (~/.tmux.conf):
#     set -g status-interval 2
#     set -g status-right '#(/absolute/path/to/statusline.sh tmux) | %H:%M'
#
#   zellij — in a compact-bar / status config, run a command pane:
#     command "/absolute/path/to/statusline.sh" { args "ansi"; }
#
#   wezterm (~/.wezterm.lua), poll from update-right-status:
#     local h = io.popen("/absolute/path/to/statusline.sh plain"); ...
#
# First arg picks the format: plain (default), tmux, or ansi.
#   - tmux: emits #[fg=...] color codes for the tmux status line.
#   - ansi: emits raw ANSI escapes (use for zellij / wezterm / bare terminals).
#   - plain: no color (safest when the host re-colors the line itself).
# ANTHROPIC_PROXY_URL overrides the proxy address.
# ANTHROPIC_PROXY_WINDOW picks the throughput window (1m|10m|1h|5h|24h).

set -u

FMT="${1:-plain}"
URL="${ANTHROPIC_PROXY_URL:-http://127.0.0.1:8787}"
WINDOW="${ANTHROPIC_PROXY_WINDOW:-1m}"

case "$FMT" in
  plain|tmux|ansi) ;;
  *) FMT="plain" ;;
esac

OUT="$(curl -fs --max-time 1 "${URL}/_proxy/statusline?fmt=${FMT}&window=${WINDOW}" 2>/dev/null)" || OUT=""

if [ -z "$OUT" ]; then
  case "$FMT" in
    tmux) echo "#[fg=red]proxy?#[default]" ;;
    ansi) printf '\x1b[31mproxy?\x1b[0m\n' ;;
    *)    echo "proxy?" ;;
  esac
  exit 0
fi

echo "$OUT"
