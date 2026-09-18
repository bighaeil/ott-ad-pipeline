# --------------------------------------------------------------------------
# ad-decision 멀티스테이지 빌드
# gradle wrapper 대신 gradle 이미지를 쓴다 (wrapper jar 를 저장소에 넣지 않기 위해).
# --------------------------------------------------------------------------
FROM gradle:8.12-jdk21 AS build
WORKDIR /src

# 의존성 해석만 먼저 수행해 레이어 캐시를 만든다.
COPY settings.gradle.kts build.gradle.kts ./
RUN gradle --no-daemon -q dependencies --configuration runtimeClasspath > /dev/null 2>&1 || true

COPY src ./src
RUN gradle --no-daemon bootJar -x test

# --------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S app && adduser -S app -G app \
 && mkdir -p /data && chown -R app:app /data

COPY --from=build /src/build/libs/ad-decision.jar /app/ad-decision.jar
USER app

ENV JAVA_OPTS="-Xmx256m -XX:MaxMetaspaceSize=160m"
EXPOSE 8090

ENTRYPOINT ["/bin/sh", "-c", "exec java $JAVA_OPTS -jar /app/ad-decision.jar"]
