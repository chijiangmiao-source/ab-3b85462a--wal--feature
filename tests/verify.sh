#!/bin/sh
# One-shot acceptance run executed by the Compose `verify` service.
# Works both in the container (workdir /srv) and from a local checkout.
# Exit code is the acceptance verdict (0 = pass, non-zero = fail).
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export PYTHONPATH="$ROOT/app:$ROOT/tests${PYTHONPATH:+:$PYTHONPATH}"
TARGET_URL="${TARGET_URL:-http://127.0.0.1:${PORT:-8080}}"
export TARGET_URL

echo "== [1/2] unit / engine tests =="
python3 -m unittest discover -s tests -p 'test_*.py' -v

echo
echo "== [2/2] HTTP smoke against ${TARGET_URL} =="
python3 tests/http_smoke.py

echo
echo "VERIFY ACCEPTED"
