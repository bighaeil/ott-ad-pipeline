# ===========================================================================
# Spark 3.5.3 + S3A + PostgreSQL JDBC
#
# 버전 조합 근거 (임의로 고르면 반드시 깨지는 부분이다)
#   apache/spark:3.5.3 이미지에 들어 있는 hadoop-client-api 는 3.3.4 다.
#   -> hadoop-aws 는 반드시 같은 3.3.4 여야 한다.
#   -> hadoop-aws 3.3.4 는 AWS SDK v1 의 aws-java-sdk-bundle 1.12.262 로 빌드됐다.
#      (hadoop 3.4 부터 SDK v2 인 bundle-2.x 로 바뀐다. 섞으면 NoClassDefFoundError)
#
# aws-java-sdk-bundle 이 280MB 라 저장소에 두지 않고 이미지 레이어로만 받는다.
# ===========================================================================
FROM apache/spark:3.5.3

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ARG HADOOP_AWS=3.3.4
ARG AWS_SDK=1.12.262
ARG PG_DRIVER=42.7.4

RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fsSL -O ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS}/hadoop-aws-${HADOOP_AWS}.jar; \
    curl -fsSL -O ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK}/aws-java-sdk-bundle-${AWS_SDK}.jar; \
    curl -fsSL -O ${MAVEN}/org/postgresql/postgresql/${PG_DRIVER}/postgresql-${PG_DRIVER}.jar; \
    ls -l hadoop-aws-*.jar aws-java-sdk-bundle-*.jar postgresql-*.jar

# 배치 잡은 compose 에서 볼륨으로 마운트한다 (/opt/spark-jobs).
WORKDIR /opt/spark
