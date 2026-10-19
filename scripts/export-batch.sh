#!/usr/bin/env bash
# Execute on edge node, then copy destination to dashboard host.
set -euo pipefail
if [[ $# -lt 2 || $# -gt 3 ]]; then echo 'usage: export-batch.sh HDFS_BATCH DESTINATION [SPARK_EVIDENCE_JSON]' >&2; exit 2; fi
[[ ! -e "$2" ]] || { echo 'Destination must be new' >&2; exit 1; }
export JAVA_HOME=${JAVA_HOME:-/usr/local/jdk1.8.0_212}
export PATH=$JAVA_HOME/bin:$PATH
partial="$2.partial.$$"
trap 'rm -rf -- "$partial"' EXIT
hdfs dfs -get "$1" "$partial"
# Copy all completed result directories, including future result contracts.
for directory in "$partial"/*/; do
  [[ -f "$directory/_SUCCESS" ]] || { echo "Incomplete Spark output: $directory" >&2; exit 1; }
done
shopt -s nullglob
metadata=("$partial"/metadata/part-*)
[[ ${#metadata[@]} == 1 ]] || { echo 'Expected exactly one completed metadata part' >&2; exit 1; }
cat "${metadata[0]}" > "$partial/metadata.json"
if [[ $# == 3 ]]; then cp -- "$3" "$partial/spark-evidence.json"; fi
mv -- "$partial" "$2"
