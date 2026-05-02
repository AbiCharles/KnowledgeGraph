#!/usr/bin/env bash
# Back up the use_cases/ directory (bundles + .versions/ + LLM usage tracker)
# to a timestamped tarball. Designed to run from cron — does ONE thing,
# exits cleanly, prints the path of the new artefact for log scrapers.
#
# Usage:
#   scripts/backup_bundles.sh                       # writes to ./backups/
#   scripts/backup_bundles.sh /var/lib/kf/backups   # custom destination
#
# Cron example (daily 02:30 UTC, retain 14 days):
#   30 2 * * * /opt/kf/scripts/backup_bundles.sh /var/lib/kf/backups \
#               > /var/log/kf-backup.log 2>&1
#   # Then a separate cleaner:
#   0 3 * * * find /var/lib/kf/backups -name 'use_cases-*.tar.gz' \
#               -mtime +14 -delete

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
USE_CASES_DIR="${REPO_ROOT}/use_cases"
DEST="${1:-${REPO_ROOT}/backups}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${DEST}/use_cases-${STAMP}.tar.gz"

if [ ! -d "${USE_CASES_DIR}" ]; then
  echo "ERROR: ${USE_CASES_DIR} does not exist" >&2
  exit 1
fi

mkdir -p "${DEST}"

# Exclude the lock files + transient staging directories — they're not part
# of any restorable state and would just bloat the archive.
tar \
  --exclude="*.lock" \
  --exclude="*.staging" \
  --exclude="*.staging/*" \
  -czf "${OUT}" \
  -C "${REPO_ROOT}" \
  use_cases/

# Sanity check the archive — tar -t just lists contents; failure means a
# corrupt write and we should NOT swallow it silently.
if ! tar -tzf "${OUT}" > /dev/null; then
  echo "ERROR: backup archive ${OUT} failed integrity check" >&2
  rm -f "${OUT}"
  exit 2
fi

SIZE=$(du -h "${OUT}" | cut -f1)
echo "OK ${OUT} (${SIZE})"
