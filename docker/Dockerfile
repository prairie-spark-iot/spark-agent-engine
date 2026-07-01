# syntax=docker/dockerfile:1
# Generic Java JRE runtime image — copy your pre-built JAR as app.jar before build.

FROM eclipse-temurin:25-jre

RUN groupadd --system --gid 1000 appuser && \
    useradd --system --gid appuser --uid 1000 \
        --shell /sbin/nologin --no-create-home --home-dir /app appuser

WORKDIR /app
COPY *.jar app.jar
RUN chown appuser:appuser app.jar

USER appuser
EXPOSE 8080

# ── JDK 25 defaults (override via docker-compose environment if needed) ──
ENV JAVA_TOOL_OPTIONS="\
    -Xms2g -Xmx2g \
    -XX:+UseZGC \
    -XX:+AlwaysPreTouch \
    -XX:+UseCompactObjectHeaders \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=/tmp/heapdump.hprof \
    "
ENV TZ=Asia/Shanghai

ENTRYPOINT ["java", "-jar", "app.jar"]