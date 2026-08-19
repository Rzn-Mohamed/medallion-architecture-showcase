import logging
import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StringType, IntegerType, ShortType, ByteType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    DecimalType, BinaryType,
)
from silver_config import SILVER_GROUPS

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SilverJob")

BRONZE_BASE       = os.getenv("BRONZE_BASE",        "hdfs://namenode:9000/warehouse/bronze")
SILVER_BASE       = os.getenv("SILVER_BASE",        "hdfs://namenode:9000/warehouse/silver")
HIVE_METASTORE    = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
HDFS_REPLICATION  = 1
WRITE_PARTITIONS  = 4   # coalesce target for output Parquet files

spark = (
    SparkSession.builder
    .appName("SilverJob")
    .config("spark.sql.warehouse.dir", SILVER_BASE)
    .config("hive.metastore.uris", HIVE_METASTORE)
    .enableHiveSupport()
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("dfs.replication", str(HDFS_REPLICATION))


def table_exists(table_name):
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI.create(SILVER_BASE),
            hadoop_conf
        )
        path = spark._jvm.org.apache.hadoop.fs.Path(f"{SILVER_BASE}/{table_name}/")
        return fs.exists(path) and fs.listStatus(path).length > 0
    except Exception as e:
        logger.warning(f"[{table_name}] HDFS check failed: {e}")
        return False


def apply_renames(df, renames):
    if not renames:
        return df
    current_cols = set(df.columns)
    for old_name, new_name in renames.items():
        if old_name in current_cols:
            if new_name in current_cols:
                df = df.withColumn(new_name, F.coalesce(F.col(new_name), F.col(old_name)))
                df = df.drop(old_name)
                logger.info(f"  Merged column: {old_name} into existing {new_name}")
            else:
                df = df.withColumnRenamed(old_name, new_name)
                logger.info(f"  Renamed column: {old_name} -> {new_name}")
    return df


def normalize_gender(df):
    """Normalise GENDER encoding variants to 'Masculin' or 'Feminin'."""
    return df.withColumn(
        "GENDER",
        F.when(F.col("GENDER") == "Masculin", "Masculin")
         .when(F.col("GENDER").contains("minin"), "Feminin")
         .otherwise(None)
    )


# Maps Spark types to native Hive DDL types (STORED AS PARQUET so HiveServer2/DBeaver can query Silver tables)
def _spark_to_hive_type(dtype):
    if isinstance(dtype, (IntegerType, ShortType, ByteType)):
        return "INT"
    if isinstance(dtype, LongType):      return "BIGINT"
    if isinstance(dtype, DoubleType):    return "DOUBLE"
    if isinstance(dtype, FloatType):     return "FLOAT"
    if isinstance(dtype, BooleanType):   return "BOOLEAN"
    if isinstance(dtype, DateType):      return "DATE"
    if isinstance(dtype, TimestampType): return "TIMESTAMP"
    if isinstance(dtype, DecimalType):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if isinstance(dtype, BinaryType):    return "BINARY"
    return "STRING"


def _build_hive_cols(schema):
    """Return a Hive DDL column-list string from a Spark StructType."""
    return ",\n        ".join(
        f"`{f.name}` {_spark_to_hive_type(f.dataType)}"
        for f in schema.fields
    )


def cast_and_clean(df, config):
    # Step 1: Apply renames first
    df = apply_renames(df, config.get("renames", {}))

    # Step 2: Trim strings and cast columns in a single pass
    cast_map   = config["casts"]
    upper_cols = set(config.get("uppercase", []))

    select_exprs = []
    for field in df.schema.fields:
        col_expr = F.col(field.name)

        if isinstance(field.dataType, StringType):
            col_expr = F.trim(col_expr)

        if field.name in cast_map:
            dtype = cast_map[field.name]
            if isinstance(dtype, tuple):
                col_expr = F.to_date(col_expr, dtype[1])
            else:
                col_expr = col_expr.cast(dtype)

        if field.name in upper_cols:
            col_expr = F.upper(col_expr)

        select_exprs.append(col_expr.alias(field.name))

    df = df.select(*select_exprs)

    # Step 3: Normalize GENDER if flagged in config
    if config.get("normalize_gender", False) and "GENDER" in df.columns:
        df = normalize_gender(df)
        logger.info("  Applied GENDER normalization")

    # Step 4: Build and apply all filter conditions
    filter_conditions = []

    date_floor = config.get("date_floor", "2000-01-01")
    date_cols  = [c for c, d in cast_map.items() if isinstance(d, tuple)]

    for col_name in date_cols:
        if col_name in df.columns:
            filter_conditions.append(
                F.col(col_name).isNull() | (
                    (F.col(col_name) >= F.lit(date_floor)) &
                    (F.col(col_name) <= F.current_date())
                )
            )

    for col_name in config.get("non_negative", []):
        if col_name in df.columns:
            filter_conditions.append(
                F.col(col_name).isNull() | (F.col(col_name) >= 0)
            )

    for col_name in config["not_null"]:
        if col_name in df.columns:
            filter_conditions.append(F.col(col_name).isNotNull())

    if filter_conditions:
        combined = filter_conditions[0]
        for cond in filter_conditions[1:]:
            combined = combined & cond
        df = df.filter(combined)

    # Step 5: Dedup
    dedup_cols = config.get("dedup")
    if dedup_cols:
        df = df.dropDuplicates(dedup_cols)

    df = df.withColumn("_silver_ts", F.current_timestamp())
    return df


def run(table_name, force=False):
    if not force and table_exists(table_name):
        logger.info(f"[{table_name}] Already exists in Silver, skipping")
        return

    config = SILVER_GROUPS[table_name]
    paths  = [f"{BRONZE_BASE}/{src}/" for src in config["sources"]]

    logger.info(f"[{table_name}] Reading {len(paths)} source(s) from Bronze")
    logger.info(f"[{table_name}] date_floor = {config.get('date_floor', '2000-01-01')}")

    if config.get("allow_missing", False):
        dfs = []
        for p in paths:
            try:
                # Use glob to skip NiFi metadata/ subdirs alongside Parquet files
                glob_path = p.rstrip("/") + "/*.parquet"
                dfs.append(spark.read.parquet(glob_path))
            except Exception as e:
                logger.warning(f"[{table_name}] Source not found, skipping: {p} ({e})")
        if not dfs:
            logger.error(f"[{table_name}] No sources found, aborting")
            return
        from functools import reduce
        df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    else:
        # Glob pattern skips NiFi metadata/ subdirs alongside the Parquet files
        glob_paths = [p.rstrip("/") + "/*.parquet" for p in paths]
        df = spark.read.parquet(*glob_paths)

    df = cast_and_clean(df, config)

    out_path = f"{SILVER_BASE}/{table_name}/"
    # coalesce avoids a full shuffle; cache so the write doesn't re-read Bronze
    df.cache()
    df.coalesce(WRITE_PARTITIONS).write.mode("overwrite").parquet(out_path)
    row_count = spark.read.parquet(out_path).count()
    logger.info(f"[{table_name}] {row_count:,} rows written to {out_path}")
    df.unpersist()

    # Register in Hive Metastore (DROP + CREATE keeps schema current after each overwrite)
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS silver LOCATION '{SILVER_BASE}'")
        hive_cols = _build_hive_cols(df.schema)
        spark.sql(f"DROP TABLE IF EXISTS silver.`{table_name}`")
        spark.sql(f"""
            CREATE EXTERNAL TABLE silver.`{table_name}` (
                {hive_cols}
            )
            STORED AS PARQUET
            LOCATION '{out_path}'
        """)
        spark.sql(f"REFRESH TABLE silver.`{table_name}`")
        logger.info(f"[{table_name}] Registered in Hive Metastore as silver.{table_name}")
    except Exception as hive_err:
        # Hive registration failure is non-fatal — the Parquet file is already written
        logger.warning(f"[{table_name}] Hive registration failed (data still written): {hive_err}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Silver Layer Job")
    parser.add_argument("--tables", nargs="+", default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    tables_to_run = args.tables if args.tables else list(SILVER_GROUPS.keys())
    total         = len(tables_to_run)

    logger.info(f"Processing {total} table(s), force={args.force}")
    job_start = time.time()

    for idx, table in enumerate(tables_to_run, 1):
        if table not in SILVER_GROUPS:
            logger.error(f"[{idx}/{total}] {table} not in silver_config, skipping")
            continue
        try:
            t0 = time.time()
            logger.info(f"[{idx}/{total}] Starting {table}")
            run(table, force=args.force)
            elapsed = time.time() - t0
            logger.info(f"[{idx}/{total}] Finished {table} in {elapsed:.1f}s")
        except Exception as e:
            logger.error(f"[{idx}/{total}] Failed {table}: {e}", exc_info=True)

    total_elapsed = time.time() - job_start
    spark.stop()
    logger.info(f"SilverJob complete in {total_elapsed:.1f}s")