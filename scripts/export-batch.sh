#!/usr/bin/env bash
# Execute on edge node, then copy destination to dashboard host.
set -euo pipefail
if [[ $# != 2 ]]; then echo 'usage: export-batch.sh HDFS_BATCH DESTINATION' >&2; exit 2; fi
[[ ! -e "$2" ]] || { echo 'Destination must be new' >&2; exit 1; }
export JAVA_HOME=${JAVA_HOME:-/usr/local/jdk1.8.0_212}
export PATH=$JAVA_HOME/bin:$PATH
hdfs dfs -get "$1" "$2"
if [[ -d "$2/metadata" ]]; then
  cat "$2"/metadata/part-* > "$2/metadata.json"
fi
