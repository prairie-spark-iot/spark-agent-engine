# syntax=docker/dockerfile:1

FROM eclipse-temurin:25-jre

RUN groupadd --system --gid 1000 app && \
    useradd --system \
    --gid app \
    --uid 1000 \
    --shell /sbin/nologin \
    --no-create-home \
    --home-dir /app app

WORKDIR /app

ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar

RUN chown app:app /app/app.jar

USER app

EXPOSE 8080

ENV TZ=Asia/Shanghai

ENV JAVA_TOOL_OPTIONS="\
-XX:+UseZGC \
-XX:+ZGenerational \
-XX:InitialRAMPercentage=20 \
-XX:MaxRAMPercentage=70 \
-XX:+AlwaysPreTouch \
-XX:+UseCompactObjectHeaders \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=/tmp/heapdump.hprof \
-XX:+ExitOnOutOfMemoryError \
-Dfile.encoding=UTF-8 \
-Djava.security.egd=file:/dev/urandom \
-Dsun.net.inetaddr.ttl=60 \
-Dsun.net.inetaddr.negative.ttl=10 \
"

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
CMD wget -qO- http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", "-jar", "/app/app.jar"]