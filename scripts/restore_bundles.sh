#!/usr/bin/env bash
# Restore use_cases/ from a backup tarball produced by backup_bundles.sh.
# Aborts if the destination is non-empty unless --force is passed, so an
# operator can't accidentally clobber a working set of bundles.
#
# Usage:
#   scripts/restore_bundles.sh backups/use_cases-20260502T023000Z.tar.gz
#   scripts/restore_bundles.sh backups/use_cases-20260502T023000Z.tar.gz --force

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
USE_CASES_DIR="${REPO_ROOT}/use_cases"

ARCHIVE="${1:-}"
FORCE="${2:-}"

if [ -z "${ARCHIVE}" ] || [ ! -f "${ARCHIVE}" ]; then
  echo "Usage: $0 <archive.tar.gz> [--force]" >&2
  exit 1
fi

# Refuse to clobber a populated use_cases/ unless explicitly forced.
if [ -d "${USE_CASES_DIR}" ] && [ -n "$(ls -A "${USE_CASES_DIR}" 2>/dev/null)" ]; then
  if [ "${FORCE}" != "--force" ]; then
    echo "ERROR: ${USE_CASES_DIR} is non-empty. Pass --force to overwrite." >&2
    echo "Hint: rename the existing dir first if you want a side-by-side restore." >&2
    exit 2
  fi
  echo "WARN: --force given; clearing ${USE_CASES_DIR} before restore."
  rm -rf "${USE_CASES_DIR:?}"/*  "${USE_CASES_DIR:?}"/.[!.]* 2>/dev/null || true
fi

mkdir -p "${USE_CASES_DIR}"
# Extract from REPO_ROOT — tarball was created with `-C REPO_ROOT use_cases/`
# so the use_cases/ prefix is already in the archive.
tar -xzf "${ARCHIVE}" -C "${REPO_ROOT}"

echo "OK restored from ${ARCHIVE} into ${USE_CASES_DIR}"
echo "Next: restart uvicorn so the registry re-discovers bundles."
