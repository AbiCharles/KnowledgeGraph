"""CLI entry point: `python -m pipeline`."""
from __future__ import annotations
import sys

from .run import run_pipeline


def main() -> int:
    failed = False
    for result in run_pipeline():
        if result.status == "running":
            print(f"[ {result.stage} ] {result.name} ... ", end="", flush=True)
            continue

        marker = "PASS" if result.status == "pass" else "FAIL"
        print(f"{marker} ({result.duration_ms} ms)")

        for line in result.logs:
            print(f"      {line}")

        if result.status == "fail":
            print(f"      ERROR: {result.error}", file=sys.stderr)
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
