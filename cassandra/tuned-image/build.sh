#!/usr/bin/env bash
# Build 0track/cassandra-accord:tuned (see Dockerfile).  Needs git, a JDK 21 and docker.
# Accord is built at the commit that modules/accord of the Cassandra tree points to; only the
# classes of Apply are taken from that build, so the rest of the image's Accord jar is untouched.
set -euo pipefail
DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ACCORD_COMMIT=${ACCORD_COMMIT:-446b250496ba386bd594ad9e4d2f86ce778f58ef}
BASE=${BASE:-0track/cassandra-accord:latest}
JAR=cassandra-accord-6.0-alpha3-SNAPSHOT.jar
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT

git init -q "$W/accord"
git -C "$W/accord" fetch -q --depth 1 https://github.com/belliottsmith/cassandra-accord.git "$ACCORD_COMMIT"
git -C "$W/accord" checkout -q FETCH_HEAD
git -C "$W/accord" apply "$DIR/accord-applylink-slice-keys.patch"
(cd "$W/accord" && ./gradlew --no-daemon -q :accord-core:jar -x test)

mkdir "$W/new"; (cd "$W/new" && jar xf "$W/accord/accord-core/build/libs/accord-core-1.0-SNAPSHOT.jar")
docker run --rm --entrypoint cat "$BASE" "/opt/cassandra/lib/$JAR" > "$DIR/accord-patched.jar"
(cd "$W/new" && jar uf "$DIR/accord-patched.jar" accord/messages/Apply.class 'accord/messages/Apply$ApplyLink.class')
docker build -t 0track/cassandra-accord:tuned "$DIR"
rm -f "$DIR/accord-patched.jar"
