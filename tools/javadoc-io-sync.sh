#!/usr/bin/env bash
#
# Makes javadoc.io serve the latest release of every published Tandem module.
#
# Why: javadoc.io does not always notice a new release on Maven Central by itself,
# so its "latest" link can stay several versions behind. Its versions page has two
# buttons, "Sync from Maven" and "Upload selected"; this script presses them for
# the latest release of each module, then waits until the docs are generated.
#
# The module list comes from the latest tandem-bom on Maven Central, so a new module
# is picked up without editing this file. Independently versioned modules are not in
# the BOM and are listed in INDEPENDENT_MODULES below.
#
# Usage:
#   tools/javadoc-io-sync.sh                      # every module, its latest release
#   tools/javadoc-io-sync.sh tandem-core          # only the named modules
#   DRY_RUN=1 tools/javadoc-io-sync.sh            # report the status, change nothing
#   TIMEOUT=1800 tools/javadoc-io-sync.sh         # wait longer for generation (seconds)
#
# Exits non-zero if any module's latest release is not available when it finishes.
# Requires curl.

set -euo pipefail

GROUP="com.codingful"
CENTRAL="https://repo1.maven.org/maven2/${GROUP//.//}"
JAVADOC_IO="https://javadoc.io"
INDEPENDENT_MODULES=(tandem-rabbitmq)
TIMEOUT="${TIMEOUT:-900}"
POLL=20
SYNC_POLLS=12
DRY_RUN="${DRY_RUN:-0}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

latest_release() {
  curl -fs -m 30 "$CENTRAL/$1/maven-metadata.xml" | sed -n 's:.*<release>\(.*\)</release>.*:\1:p'
}

modules_from_bom() {
  local bom
  bom="$(latest_release tandem-bom)"
  curl -fsS -m 30 "$CENTRAL/tandem-bom/$bom/tandem-bom-$bom.pom" \
    | sed -n 's:.*<artifactId>\(.*\)</artifactId>.*:\1:p' | grep -vx tandem-bom
}

# Fetches a module's versions page into $work/<module>.html, keeping its session cookie.
fetch_page() {
  curl -fsS -m 30 -b "$work/$1.jar" -c "$work/$1.jar" "$JAVADOC_IO/versions/$GROUP/$1" \
    | tr -s '[:space:]' ' ' > "$work/$1.html"
}

csrf_token() {
  sed -n 's:.*name="csrfToken" value="\([^"]*\)".*:\1:p' "$work/$1.html" | head -1
}

# The status javadoc.io shows for a version (UPLOADED, DISCOVERED, ...); empty when it
# does not list that version at all.
version_status() {
  { grep -oE "<td>${2//./\\.}</td> <td> <span[^>]*title=\"[A-Z_]+\"" "$work/$1.html" || true; } \
    | sed -n 's:.*title="\([A-Z_]*\)".*:\1:p' | head -1
}

post_form() {
  local module="$1" action="$2"; shift 2
  curl -fsS -m 60 -o /dev/null -b "$work/$module.jar" -c "$work/$module.jar" \
    --data-urlencode "csrfToken=$(csrf_token "$module")" "$@" \
    "$JAVADOC_IO/versions/$GROUP/$module/$action"
}

if [ "$#" -gt 0 ]; then
  modules=("$@")
else
  modules=()
  while read -r m; do modules+=("$m"); done < <(modules_from_bom)
  modules+=("${INDEPENDENT_MODULES[@]}")
fi

pending=()
failed=()

for module in "${modules[@]}"; do
  version="$(latest_release "$module" || true)"
  if [ -z "$version" ]; then
    echo "$module: not found on Maven Central, skipped"
    continue
  fi

  fetch_page "$module"
  status="$(version_status "$module" "$version")"

  if [ "$status" = "UPLOADED" ]; then
    echo "$module $version: already available"
    continue
  fi
  if [ "$DRY_RUN" = "1" ]; then
    echo "$module $version: ${status:-not listed by javadoc.io}"
    continue
  fi

  if [ -z "$status" ]; then
    # The sync runs in the background: the new version shows up a few seconds later.
    post_form "$module" sync
    for _ in $(seq 1 "$SYNC_POLLS"); do
      sleep 5
      fetch_page "$module"
      status="$(version_status "$module" "$version")"
      [ -z "$status" ] || break
    done
    if [ -z "$status" ]; then
      echo "$module $version: still not listed after syncing from Maven"
      failed+=("$module")
      continue
    fi
  fi

  if [ "$status" = "DISCOVERED" ]; then
    post_form "$module" upload --data-urlencode "versionId=$version"
    echo "$module $version: upload started"
  else
    echo "$module $version: $status"
  fi
  pending+=("$module:$version")
done

deadline=$(( $(date +%s) + TIMEOUT ))
while [ "${#pending[@]}" -gt 0 ]; do
  sleep "$POLL"
  still=()
  for entry in "${pending[@]}"; do
    module="${entry%%:*}"; version="${entry#*:}"
    fetch_page "$module"
    status="$(version_status "$module" "$version")"
    if [ "$status" = "UPLOADED" ]; then
      echo "$module $version: available"
    else
      still+=("$entry")
    fi
  done
  pending=("${still[@]+"${still[@]}"}")
  if [ "${#pending[@]}" -gt 0 ] && [ "$(date +%s)" -ge "$deadline" ]; then
    for entry in "${pending[@]}"; do
      echo "${entry%%:*} ${entry#*:}: not available after ${TIMEOUT}s"
      failed+=("${entry%%:*}")
    done
    break
  fi
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo "Not available: ${failed[*]}"
  exit 1
fi
