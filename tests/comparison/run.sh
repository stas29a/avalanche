#!/usr/bin/env bash
set -euo pipefail

comparison_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
comparison_python="$comparison_dir/.venv/bin/python"

if [[ ! -x "$comparison_python" ]]; then
    python3 -m venv "$comparison_dir/.venv"
    "$comparison_python" -m pip install -r "$comparison_dir/requirements.txt"
fi

exec "$comparison_python" "$comparison_dir/run.py" "$@"
