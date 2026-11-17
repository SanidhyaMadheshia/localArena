#!/usr/bin/env bash
# Drill 011d: kafka-two-az-isolate
# Proves: losing 2 of 3 AZs stops Kafka writes (KRaft quorum and minISR=2 both need 2 brokers)
#         and it is the expected, safe failure: no split brain, no lost acknowledged data.
# Expected: producer stalls during isolation; after restore recovery completes. See 011c.
# RISK: same as kafka-az-isolate.sh, two AZs at once.
DRILL_NAME=kafka-two-az-isolate AZ_COUNT=2 exec "$(dirname "${BASH_SOURCE[0]}")/kafka-az-isolate.sh"
