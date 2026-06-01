#!/usr/bin/env bash
# Statusline helper for anthropic_proxy.
#
# Usage from Claude Code (~/.claude/settings.json):
#   {
#     "statusLine": {
#       "type": "command",
#       "command": "/absolute/path/to/statusline.sh"
#     }
#   }
#
# Usage from tmux (~/.tmux.conf):
#   set -g status-interval 2
#   set -g status-right '#(/absolute/path/to/statusline.sh tmux) | %H:%M'
#
# First arg picks the format: plain (default), tmux, or ansi.
# ANTHROPIC_PROXY_URL overrides the proxy address.

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
