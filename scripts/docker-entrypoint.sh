#!/bin/sh
# Seed-on-boot for the use_cases bundle directory.
#
# /app/use_cases is a persistent volume (Fly) or named volume (compose). A
# fresh volume mounts EMPTY and shadows the demo bundles baked into the image,
# so without this the dashboard would start with no use cases. We keep a
# read-only copy of the seeded bundles at /app/use_cases_seed and restore any
# that are missing from the volume on each boot.
#
# Only bundles that don't already exist in the volume are copied, so anything
# the operator uploaded at runtime is left untouched.
set -e

SEED=/app/use_cases_seed
DEST=/app/use_cases

if [ -d "$SEED" ]; then
  for d in "$SEED"/*/; do
    [ -d "$d" ] || continue
    name=$(basename "$d")
    if [ ! -e "$DEST/$name" ]; then
      echo "[entrypoint] seeding bundle: $name"
      cp -a "$d" "$DEST/$name"
    fi
  done
fi

exec "$@"
