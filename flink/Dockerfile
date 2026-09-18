# ===========================================================================
# Flink 1.20 + SQL 커넥터 JAR
#
# 버전 조합은 임의로 정한 게 아니라 Maven Central 에서 "Flink 1.20 용으로
# 실제 배포된 것"을 확인해서 고정했다. 올릴 때는 반드시 접미사(-1.20)를 확인할 것.
#
#   flink-sql-connector-kafka  3.4.0-1.20   (1.20 용 최신. 3.3.0-1.20 도 있음)
#   flink-connector-jdbc       3.3.0-1.20   (core + postgres dialect 가 한 jar 에 들어 있음)
#   postgresql (JDBC 드라이버)  42.7.4       (jdbc 커넥터는 드라이버를 번들하지 않는다)
#
# [Redis 관련 기록]
#   Flink 1.20 용 Redis "SQL" 커넥터는 존재하지 않는다.
#     - Apache 공식 없음
#     - Apache Bahir flink-connector-redis 1.1.0 : Flink 1.1 시절, 프로젝트 자체가 은퇴
#     - io.github.jeff-zou:flink-connector-redis 1.4.3 : pom 의 flink.version = 1.15.1
#     - com.redis:redis-flink-connector 0.0.9 : flink-core 1.19 기반 DataStream 싱크,
#                                               Table/SQL 팩토리 없음
#   따라서 SQL 로 Redis 에 직접 쓰는 길이 없다. 임의로 버전을 올려 끼우지 않고,
#   Flink 는 분단위 집계를 Kafka(agg.minute)로 내보내고 redis-writer 가 Redis 에 적재한다.
#   자세한 내용은 README "버전 관련 기록" 참조.
# ===========================================================================
FROM flink:1.20-scala_2.12-java17

ARG MAVEN=https://repo1.maven.org/maven2
ARG KAFKA_CONNECTOR=3.4.0-1.20
ARG JDBC_CONNECTOR=3.3.0-1.20
ARG PG_DRIVER=42.7.4

USER root

RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fsSL -o flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
      ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar; \
    curl -fsSL -o flink-connector-jdbc-${JDBC_CONNECTOR}.jar \
      ${MAVEN}/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR}/flink-connector-jdbc-${JDBC_CONNECTOR}.jar; \
    curl -fsSL -o postgresql-${PG_DRIVER}.jar \
      ${MAVEN}/org/postgresql/postgresql/${PG_DRIVER}/postgresql-${PG_DRIVER}.jar; \
    chown flink:flink flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
                      flink-connector-jdbc-${JDBC_CONNECTOR}.jar \
                      postgresql-${PG_DRIVER}.jar

# --- 단계 5: MinIO(S3) Parquet 적재용 ---------------------------------------
# Parquet 포맷과 S3 파일시스템은 배포판 버전과 정확히 같아야 한다.
# 베이스 이미지 태그가 1.20 (마이너 미지정)이라 버전을 하드코딩하지 않고
# flink-dist jar 이름에서 뽑아 쓴다.
#
# flink-s3-fs-hadoop 은 lib/ 가 아니라 plugins/ 에 넣어야 한다.
# lib/ 에 두면 Flink 의 셰이딩된 hadoop 과 클래스가 충돌한다. (공식 문서 지침)
ARG HADOOP_CLIENT=3.3.4

RUN set -eux; \
    FV="$(ls /opt/flink/lib/flink-dist-*.jar | sed 's|.*/flink-dist-\(.*\)\.jar|\1|')"; \
    echo "flink version = ${FV}"; \
    curl -fsSL -o /opt/flink/lib/flink-sql-parquet-${FV}.jar \
      ${MAVEN}/org/apache/flink/flink-sql-parquet/${FV}/flink-sql-parquet-${FV}.jar; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-${FV}.jar /opt/flink/plugins/s3-fs-hadoop/; \
    chown -R flink:flink /opt/flink/lib /opt/flink/plugins

# flink-sql-parquet 은 parquet-hadoop 을 쓰는데 Hadoop 자체는 번들하지 않는다.
# 넣지 않으면 INSERT 시점에 ClassNotFoundException: org.apache.hadoop.conf.Configuration.
#
# 풀 Hadoop 배포판 대신 shaded 한 client-api/runtime 두 개만 넣는다.
# (Spark 이미지가 쓰는 것과 같은 조합. 서드파티 의존성이 relocate 되어 있어
#  Flink 자체 jar 와 충돌하지 않는다.)
# s3a 구현은 여기 없고 플러그인(flink-s3-fs-hadoop) 에만 있으므로 겹치지 않는다.
RUN set -eux; \
    curl -fsSL -o /opt/flink/lib/hadoop-client-api-${HADOOP_CLIENT}.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-client-api/${HADOOP_CLIENT}/hadoop-client-api-${HADOOP_CLIENT}.jar; \
    curl -fsSL -o /opt/flink/lib/hadoop-client-runtime-${HADOOP_CLIENT}.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/${HADOOP_CLIENT}/hadoop-client-runtime-${HADOOP_CLIENT}.jar; \
    chown flink:flink /opt/flink/lib/hadoop-client-*.jar; \
    ls -l /opt/flink/lib /opt/flink/plugins/s3-fs-hadoop

USER flink
