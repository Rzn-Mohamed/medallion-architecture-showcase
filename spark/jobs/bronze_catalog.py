import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType, ShortType, ByteType, LongType,
    DoubleType, FloatType, StringType, BooleanType,
    DateType, TimestampType, DecimalType, BinaryType,
)

# Config
BRONZE_BASE    = os.getenv("BRONZE_BASE",        "hdfs://namenode:9000/warehouse/bronze")
HIVE_METASTORE = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("BronzeCatalog")

# SparkSession with Hive support
spark = (
    SparkSession.builder
    .appName("BronzeCatalog")
    .config("spark.sql.warehouse.dir", BRONZE_BASE)
    .config("hive.metastore.uris", HIVE_METASTORE)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .enableHiveSupport()
    .getOrCreate()
)

hadoop_conf = spark._jsc.hadoopConfiguration()


# Maps Spark types to native Hive DDL types (STORED AS PARQUET so HiveServer2/DBeaver can read tables)
def spark_to_hive_type(dtype):
    if isinstance(dtype, (IntegerType, ShortType, ByteType)):
        return "INT"
    if isinstance(dtype, LongType):
        return "BIGINT"
    if isinstance(dtype, DoubleType):
        return "DOUBLE"
    if isinstance(dtype, FloatType):
        return "FLOAT"
    if isinstance(dtype, BooleanType):
        return "BOOLEAN"
    if isinstance(dtype, DateType):
        return "DATE"
    if isinstance(dtype, TimestampType):
        return "TIMESTAMP"
    if isinstance(dtype, DecimalType):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if isinstance(dtype, BinaryType):
        return "BINARY"
    return "STRING"


def build_hive_cols(schema):
    """Return a Hive DDL column-list string from a Spark StructType."""
    return ",\n    ".join(
        f"`{f.name}` {spark_to_hive_type(f.dataType)}"
        for f in schema.fields
    )


# Ensure the bronze database exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze LOCATION '{BRONZE_BASE}'")
logger.info(f"Database 'bronze' ready at {BRONZE_BASE}")

# List all NiFi-written subdirectories under /warehouse/bronze/
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jvm.java.net.URI.create(BRONZE_BASE),
    hadoop_conf,
)

bronze_path = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_BASE)

try:
    statuses = fs.listStatus(bronze_path)
except Exception as exc:
    logger.error(f"Cannot list HDFS path {BRONZE_BASE}: {exc}")
    spark.stop()
    sys.exit(1)

directories = [s for s in statuses if s.isDirectory()]
logger.info(f"Found {len(directories)} directory(ies) under {BRONZE_BASE}")

if not directories:
    logger.warning("No Bronze directories found — NiFi may not have run yet. Exiting.")
    spark.stop()
    sys.exit(0)

# Register each directory as a native Hive external table
registered = 0
skipped    = 0
errors     = 0

for status in directories:
    folder_name = status.getPath().getName()
    table_path  = f"{BRONZE_BASE}/{folder_name}/"

    try:
        dir_path     = spark._jvm.org.apache.hadoop.fs.Path(table_path)
        dir_statuses = fs.listStatus(dir_path)

        # Detect layout: data/ subdir = Iceberg; otherwise flat NiFi layout
        data_subdir = spark._jvm.org.apache.hadoop.fs.Path(f"{table_path}data/")
        if fs.exists(data_subdir) and fs.isDirectory(data_subdir):
            data_statuses = fs.listStatus(data_subdir)
            parquet_files = [
                s for s in data_statuses
                if not s.isDirectory() and s.getPath().getName().endswith(".parquet")
            ]
            location      = f"{table_path}data/"
            layout        = "Iceberg(data/)"
            tblprop_extra = ""
        else:
            parquet_files = [
                s for s in dir_statuses
                if not s.isDirectory() and s.getPath().getName().endswith(".parquet")
            ]
            location      = table_path
            layout        = "flat"
            tblprop_extra = ""

            # Rename metadata/ → _metadata/ so Hive's FileInputFormat skips it
            meta_path        = spark._jvm.org.apache.hadoop.fs.Path(f"{table_path}metadata/")
            hidden_meta_path = spark._jvm.org.apache.hadoop.fs.Path(f"{table_path}_metadata/")
            if fs.exists(meta_path) and fs.isDirectory(meta_path):
                fs.rename(meta_path, hidden_meta_path)
                logger.info(f"[{folder_name}] Renamed metadata/ → _metadata/ (hidden from Hive)")

        if not parquet_files:
            logger.warning(f"[{folder_name}] No Parquet files found (layout={layout}) — skipping")
            skipped += 1
            continue

        # Infer schema via glob path (skips metadata/ automatically)
        glob_path = location.rstrip("/") + "/*.parquet"
        try:
            schema    = spark.read.parquet(glob_path).schema
            hive_cols = build_hive_cols(schema)
        except Exception as schema_err:
            logger.warning(f"[{folder_name}] Schema inference failed: {schema_err} — skipping")
            skipped += 1
            continue

        tblprops_clause = (
            f"TBLPROPERTIES ({tblprop_extra})" if tblprop_extra else ""
        )

        # DROP first to keep schema and location up to date on each run
        spark.sql(f"DROP TABLE IF EXISTS bronze.`{folder_name}`")

        spark.sql(f"""
            CREATE EXTERNAL TABLE bronze.`{folder_name}` (
                {hive_cols}
            )
            STORED AS PARQUET
            LOCATION '{location}'
            {tblprops_clause}
        """)

        spark.sql(f"REFRESH TABLE bronze.`{folder_name}`")

        logger.info(
            f"[{folder_name}] Registered → bronze.{folder_name} "
            f"({len(parquet_files)} Parquet file(s), layout={layout})"
        )
        registered += 1

    except Exception as exc:
        logger.error(f"[{folder_name}] Failed to register: {exc}", exc_info=True)
        errors += 1


# Summary
logger.info(
    f"BronzeCatalog complete — "
    f"registered={registered}, skipped={skipped}, errors={errors}"
)

if errors > 0:
    logger.warning(f"{errors} table(s) failed to register. Check logs above.")

spark.stop()
