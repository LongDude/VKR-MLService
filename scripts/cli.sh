#!/usr/bin/env sh
set -eu

CATEGORY="${1:-}"

case "$CATEGORY" in
  tasks|worker|data|ml) ;;
  *)
    echo "Usage: scripts/cli.sh {tasks|worker|data|ml} [args...]" >&2
    exit 2
    ;;
esac
shift

exec docker exec "${ML_CONTAINER:-vkr-ml-api}" python -m "cli.$CATEGORY" "$@"
