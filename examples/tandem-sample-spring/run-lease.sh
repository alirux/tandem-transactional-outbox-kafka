#!/usr/bin/env bash
# Runs the Tandem Spring sample under LEASE coordination (requires Docker), demonstrating the
# Admin API's relay-control endpoints that only mean anything under LEASE: GET /relay/buckets,
# GET /relay/workers, POST /relay/buckets/{bucket}/release. See run.sh for the default (SINGLE)
# demo, which covers the write-side tiers and the coordination-agnostic Admin API endpoints.
#
# Usage (from any directory):
#   examples/tandem-sample-spring/run-lease.sh
#   ./run-lease.sh            (when inside examples/tandem-sample-spring/)
set -euo pipefail

# The project root is the nearest directory above this script that holds settings.gradle.kts.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
until [[ -f "$ROOT/settings.gradle.kts" ]]; do
    [[ "$ROOT" != / ]] || { echo "Cannot find settings.gradle.kts above this script" >&2; exit 1; }
    ROOT="$(dirname "$ROOT")"
done

cd "$ROOT"
./gradlew :tandem-sample-spring:run --console=plain --args="--spring.profiles.active=lease"
