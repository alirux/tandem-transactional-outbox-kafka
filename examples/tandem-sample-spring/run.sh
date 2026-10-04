#!/usr/bin/env bash
# Runs the Tandem Spring sample application (requires Docker).
#
# Usage (from any directory):
#   examples/tandem-sample-spring/run.sh
#   ./run.sh            (when inside examples/tandem-sample-spring/)
#
# The script navigates to the project root automatically so that Gradle and
# Testcontainers can locate the schema files regardless of where you call it from.
set -euo pipefail

# The project root is the nearest directory above this script that holds settings.gradle.kts.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
until [[ -f "$ROOT/settings.gradle.kts" ]]; do
    [[ "$ROOT" != / ]] || { echo "Cannot find settings.gradle.kts above this script" >&2; exit 1; }
    ROOT="$(dirname "$ROOT")"
done

cd "$ROOT"
./gradlew :tandem-sample-spring:run --console=plain
