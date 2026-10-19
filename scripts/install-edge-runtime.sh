#!/usr/bin/env bash
# Host-side additive deployment. Existing Hadoop/Java/Kafka configuration stays intact.
set -euo pipefail
edge=${1:-h1}
uv sync --python 3.11.16 --extra spark --extra dev --frozen
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
python_home=$(uv python find 3.11.16)
python_home=$(dirname "$(dirname "$(readlink -f "$python_home")")")
tar -C "$(dirname "$python_home")" -czf "$stage/python.tgz" "$(basename "$python_home")"
tar -C .venv/lib/python3.11/site-packages -czf "$stage/runtime.tgz" pyspark py4j kafka
scp -q "$stage/python.tgz" "$stage/runtime.tgz" "$edge:~/opt/"
ssh "$edge" 'mkdir -p ~/opt/retailrocket-runtime; tar --warning=no-timestamp -xzf ~/opt/python.tgz -C ~/opt; tar --warning=no-timestamp -xzf ~/opt/runtime.tgz -C ~/opt/retailrocket-runtime'
mkdir -p "$stage/retailrocket-jars"
while read -r group artifact version; do
  file="$artifact-$version.jar"
  curl --fail --location --retry 3 "https://repo.maven.apache.org/maven2/${group//.//}/$artifact/$version/$file" -o "$stage/retailrocket-jars/$file"
done <<'JARS'
org.apache.spark spark-sql-kafka-0-10_2.12 3.5.6
org.apache.spark spark-token-provider-kafka-0-10_2.12 3.5.6
org.apache.kafka kafka-clients 3.4.1
com.google.code.findbugs jsr305 3.0.0
org.apache.commons commons-pool2 2.11.1
JARS
(cd "$stage/retailrocket-jars"; sha256sum -c "$OLDPWD/scripts/connector-sha256.txt")
scp -qr "$stage/retailrocket-jars" "$edge:~/opt/"
