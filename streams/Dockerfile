# syntax=docker/dockerfile:1
# Build context: repository root (needs schemas/ and streams/).
FROM gradle:9.8.1-jdk21 AS build
WORKDIR /src
COPY schemas ./schemas
COPY streams ./streams
WORKDIR /src/streams
RUN --mount=type=cache,target=/home/gradle/.gradle gradle installDist --no-daemon -q -x test

# glibc base: RocksDB's JNI library needs libstdc++ (Alpine/musl fails with UnsatisfiedLinkError).
FROM eclipse-temurin:21-jre
# pebble (Ubuntu's service manager, a Go binary) is not used and carried stdlib CVEs (Experiment 005).
RUN rm -f /usr/bin/pebble && useradd -u 10001 -r -m streams && mkdir -p /var/lib/streams && chown streams /var/lib/streams
COPY --from=build /src/streams/build/install/localarena-streams /app
USER streams
ENV STATE_DIR=/var/lib/streams JAVA_OPTS="-Xmx256m -XX:+ExitOnOutOfMemoryError"
EXPOSE 7070
ENTRYPOINT ["/app/bin/localarena-streams"]
