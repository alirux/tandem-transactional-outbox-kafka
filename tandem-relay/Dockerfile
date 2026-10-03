# The tandem-relay container image (LLD-relay §7.2). It packages the jar Gradle built on the pinned Tandem
# release and compiles nothing, so the image and the jar attached to a release are the same bits. The build
# context is this directory; .dockerignore lets the pinned jar in and nothing else.
#
#   ./gradlew :tandem-relay:pinnedBootJar
#   docker build $(./gradlew -q :tandem-relay:imageBuildArgs) --tag tandem-relay:local tandem-relay
#
# The two build arguments state what the labels claim: the image's version and the Tandem release inside
# it. imageBuildArgs prints the values the jar was built with, and the first stage refuses a jar whose
# build-info disagrees, so a label cannot name another release than the one the image contains.

# Extraction runs on the build platform: the layers are the same on every architecture, and running a JVM
# under emulation for it would only slow a multi-architecture build down.
FROM --platform=$BUILDPLATFORM eclipse-temurin:25-jre AS extract
ARG RELAY_VERSION
ARG TANDEM_VERSION
WORKDIR /build
RUN if [ -z "$RELAY_VERSION" ] || [ -z "$TANDEM_VERSION" ]; then \
        echo "RELAY_VERSION and TANDEM_VERSION are required: pass \$(./gradlew -q :tandem-relay:imageBuildArgs)" >&2; \
        exit 1; \
    fi
COPY build/libs/tandem-relay-${RELAY_VERSION}.jar application.jar
# Boot's tools jar mode splits the jar into layers that cache independently: the libraries change far less
# often than the application. The second extraction unpacks only the application layer as plain files,
# which is where the jar keeps its licence, its third-party notices and its build-info.
RUN java -Djarmode=tools -jar application.jar extract --layers --destination layers \
    && java -Djarmode=tools -jar application.jar extract --launcher --layers application --destination unpacked
RUN grep -qxF "build.version=${RELAY_VERSION}" unpacked/application/META-INF/build-info.properties \
    && grep -qxF "build.tandem.library=${TANDEM_VERSION}" unpacked/application/META-INF/build-info.properties \
    || { echo "The jar's build-info does not say RELAY_VERSION=${RELAY_VERSION} and TANDEM_VERSION=${TANDEM_VERSION}" >&2; exit 1; }

FROM eclipse-temurin:25-jre
ARG RELAY_VERSION
ARG TANDEM_VERSION
LABEL org.opencontainers.image.title="tandem-relay" \
      org.opencontainers.image.description="The Tandem transactional outbox relay, PostgreSQL to Kafka, with the optional Admin API" \
      org.opencontainers.image.source="https://github.com/alirux/tandem-transactional-outbox-kafka" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${RELAY_VERSION}" \
      com.codingful.tandem.library.version="${TANDEM_VERSION}"

# A user of its own with a fixed, numeric id: Kubernetes' runAsNonRoot can verify a number, not a name.
RUN groupadd --system --gid 10001 tandem \
    && useradd --system --uid 10001 --gid tandem --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin tandem

WORKDIR /app
COPY --from=extract /build/layers/dependencies/ ./
COPY --from=extract /build/layers/spring-boot-loader/ ./
COPY --from=extract /build/layers/snapshot-dependencies/ ./
COPY --from=extract /build/layers/application/ ./
# The image redistributes the whole runtime, so the licence and the notices travel with it.
COPY --from=extract /build/unpacked/application/META-INF/LICENSE /build/unpacked/application/META-INF/THIRD-PARTY-NOTICES.md /licenses/

USER 10001:10001
# 8080 serves the Admin API when it is enabled, 8081 the probes and the Prometheus scrape (LLD-relay §4.1).
EXPOSE 8080 8081
# The heap follows the container's memory limit. Replace or extend it with the same variable.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
# Exec form: the JVM is the container's first process and receives SIGTERM itself, which is what lets a
# stop drain the relay instead of ending in a kill. No HEALTHCHECK: orchestrators use the probe paths.
ENTRYPOINT ["java", "-jar", "application.jar"]
