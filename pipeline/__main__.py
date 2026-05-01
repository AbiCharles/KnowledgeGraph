"""CLI entry point: `python -m pipeline`."""
from __future__ import annotations
import sys

from .run import run_pipeline
from .use_case_registry import get_active


def main() -> int:
    failed = False
    use_case = get_active()
    print(f"Active use case: {use_case.manifest.name} ({use_case.slug})\n")
    for result in run_pipeline(use_case):
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
