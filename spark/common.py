"""Spark 배치 잡 공통 설정."""
import os

from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "events")

PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/adplatform")
PG_USER = os.getenv("PG_USER", "ads")
PG_PASSWORD = os.getenv("PG_PASSWORD", "ads")

EVENTS_PATH = f"s3a://{BUCKET}/"


def build_spark(app_name):
    return (
        SparkSession.builder.appName(app_name)
        # --- S3A (MinIO) ---
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        # MinIO 는 가상호스트 스타일(bucket.host)을 안 쓰므로 path style 로 강제한다.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # S3 는 etag/version 기반 변경 감지를 하는데 MinIO 와 궁합이 나쁘다.
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
        # --- 로컬 축소 설정 ---
        # 운영은 executor 여러 대. 로컬은 local[*] 단일 프로세스.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")        # 운영: 200~1000
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def jdbc_execute(spark, sql):
    """JDBC 로 임의 DDL/DML 실행. Spark DataFrame API 로는 ALTER TABLE 을 못 한다."""
    jvm = spark._jvm
    jvm.Class.forName("org.postgresql.Driver")
    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD)
    try:
        st = conn.createStatement()
        st.execute(sql)
        st.close()
    finally:
        conn.close()


def read_jdbc(spark, table):
    return (spark.read.format("jdbc")
            .option("url", PG_URL).option("dbtable", table)
            .option("user", PG_USER).option("password", PG_PASSWORD)
            .option("driver", "org.postgresql.Driver").load())


def write_jdbc(df, table, mode="overwrite", truncate=True):
    w = (df.write.format("jdbc")
         .option("url", PG_URL).option("dbtable", table)
         .option("user", PG_USER).option("password", PG_PASSWORD)
         .option("driver", "org.postgresql.Driver"))
    if truncate:
        w = w.option("truncate", "true")
    w.mode(mode).save()


def hr(title):
    print("\n" + "=" * 78)
    print("  " + title)
    print("=" * 78, flush=True)
