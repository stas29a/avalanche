#!/usr/bin/env bash
set -euo pipefail

comparison_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
comparison_python="$comparison_dir/.venv/bin/python"
if [[ ! -x "$comparison_python" ]]; then
    comparison_python="python3"
fi

exec "$comparison_python" "$comparison_dir/run_local.py" "$@"
