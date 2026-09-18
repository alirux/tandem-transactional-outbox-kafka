#!/usr/bin/env bash
#
# Records where this repository ranks in GitHub's repository search, appended to
# tools/search-rank.csv, at most one row per query per day. The schedule that runs
# it is weekly; see .github/workflows/search-rank.yml.
#
# Why: GitHub weights the repository name heavily, so renames, description and
# topic changes move the ranking. GitHub keeps no history of that, and traffic
# Insights only go back 14 days, so the curve has to be recorded as it happens.
#
# Usage:
#   tools/track-search-rank.sh          # append today's rows, skipping any already recorded
#   FORCE=1 tools/track-search-rank.sh  # append even if today is already recorded
#   REPO=owner/name tools/track-search-rank.sh
#
# Requires the GitHub CLI, authenticated (gh auth login).

set -euo pipefail

REPO="${REPO:-alirux/tandem-transactional-outbox-kafka}"
CSV="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/tools/search-rank.csv"
SCANNED=100

# The queries to track. Add one per line; the ranking only means something when
# the same string is measured over time, so edit these sparingly.
QUERIES=(
  "transactional outbox kafka"
  "transactional outbox spring"
  "outbox pattern java"
  "outbox kafka postgresql"
  "transactional outbox"
)

today="$(date -u +%F)"

read -r stars forks < <(gh api "repos/$REPO" --jq '"\(.stargazers_count) \(.forks_count)"')

[ -f "$CSV" ] || echo "date,query,position,stars,forks" > "$CSV"

for query in "${QUERIES[@]}"; do
  if [ -z "${FORCE:-}" ] && grep -qF "$today,\"$query\"," "$CSV"; then
    echo "Already recorded, skipping query:$query"
    continue
  fi

  position="$(gh api -X GET search/repositories \
    -f q="$query" -f per_page="$SCANNED" \
    --jq "[.items[].full_name] | index(\"$REPO\") | if . == null then \"\" else . + 1 end")"

  echo "$today,\"$query\",$position,$stars,$forks" >> "$CSV"
  echo "Recorded query:$query, position:${position:-beyond $SCANNED}"

  sleep 2 # The search API allows 30 requests per minute.
done

echo
echo "Wrote $CSV"
