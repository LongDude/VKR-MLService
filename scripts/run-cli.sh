#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
CATEGORY="${1:-}"

case "$CATEGORY" in
  tasks|worker|data|ml) ;;
  *)
    echo "Usage: scripts/run-cli.sh {tasks|worker|data|ml} [args...]" >&2
    exit 2
    ;;
esac
shift

for CANDIDATE in \
  "$ROOT/.venv/bin/python" \
  "$ROOT/.venv-lin/bin/python" \
  "$ROOT/.venv/Scripts/python.exe" \
  "$ROOT/.win-dev/Scripts/python.exe"
do
  if [ -x "$CANDIDATE" ]; then
    PYTHON="$CANDIDATE"
    break
  fi
done

if [ -z "${PYTHON:-}" ]; then
  echo "Python virtual environment not found. Initialize .venv or .venv-lin first." >&2
  exit 1
fi

export PYTHONPATH="$ROOT/src"
cd "$ROOT"
exec "$PYTHON" -m "cli.$CATEGORY" "$@"
