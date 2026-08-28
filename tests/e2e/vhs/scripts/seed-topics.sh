#!/usr/bin/env bash
# Seeds 25 extra topics (bulk-01..bulk-25) so the Topics table overflows the
# content region and the "Showing N of M" indicator + PgUp/PgDn paging can be
# exercised against a real cluster. Idempotent (--if-not-exists); creations run
# in parallel (8 workers) to keep the e2e tape short.
set -euo pipefail

if ! docker exec streampulse-kafka true 2>/dev/null; then
  echo "streampulse-kafka container not running" >&2
  exit 1
fi

docker exec streampulse-kafka bash -c '
  seq -w 1 25 | xargs -P 8 -I{} /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9093 \
    --create --if-not-exists --topic "bulk-{}" \
    --partitions 1 --replication-factor 1 >/dev/null
'

echo "seeded 25 bulk topics"