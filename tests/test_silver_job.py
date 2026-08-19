"""
Unit tests for spark/jobs/silver_job.py

Tests cover:
  - apply_renames
  - normalize_gender
  - _spark_to_hive_type
  - _build_hive_cols
  - cast_and_clean  (real local SparkSession — needs Java)
  - table_exists    (mocked HDFS)
  - run             (mocked Spark + HDFS)
"""

import os
import sys
import types
import unittest
import pathlib
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Add spark/jobs to sys.path so silver_config is importable
# ---------------------------------------------------------------------------

SPARK_JOBS = pathlib.Path(__file__).parent.parent / "spark" / "jobs"
sys.path.insert(0, str(SPARK_JOBS))

# ---------------------------------------------------------------------------
# Stub dotenv before anything imports it
# ---------------------------------------------------------------------------

_dotenv_stub            = types.ModuleType("dotenv")
_dotenv_stub.load_dotenv = lambda: None
sys.modules["dotenv"]  = _dotenv_stub

# ---------------------------------------------------------------------------
# Build a fake SparkSession builder chain so silver_job's module-level
# SparkSession.builder...getOrCreate() call does NOT start a real cluster.
# We swap pyspark.sql.SparkSession out completely BEFORE the import.
# ---------------------------------------------------------------------------

class _FakeBuilder:
    """Chainable builder that returns a MagicMock spark instance."""

    def __init__(self):
        self._spark = MagicMock()
        self._spark._jsc.hadoopConfiguration.return_value = MagicMock()

    def appName(self, *a, **kw):    return self
    def config(self, *a, **kw):     return self
    def master(self, *a, **kw):     return self
    def enableHiveSupport(self):    return self
    def getOrCreate(self):          return self._spark


class _FakeSparkSession:
    builder = _FakeBuilder()

    @classmethod
    def getOrCreate(cls):
        return cls.builder.getOrCreate()


# Patch pyspark.sql.SparkSession before silver_job imports it
import pyspark.sql as _pyspark_sql
_real_SparkSession            = _pyspark_sql.SparkSession
_pyspark_sql.SparkSession     = _FakeSparkSession

with patch.dict(os.environ, {
    "BRONZE_BASE":        "hdfs://namenode:9000/warehouse/bronze",
    "SILVER_BASE":        "hdfs://namenode:9000/warehouse/silver",
    "HIVE_METASTORE_URI": "thrift://hive-metastore:9083",
}):
    import silver_job

# Restore real SparkSession for the DataFrame-based test classes
_pyspark_sql.SparkSession = _real_SparkSession

from pyspark.sql.types import (
    StringType, IntegerType, ShortType, ByteType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    DecimalType, BinaryType, StructType, StructField,
)

# Shortcuts to functions under test
apply_renames       = silver_job.apply_renames
normalize_gender    = silver_job.normalize_gender
_spark_to_hive_type = silver_job._spark_to_hive_type
_build_hive_cols    = silver_job._build_hive_cols
cast_and_clean      = silver_job.cast_and_clean
table_exists        = silver_job.table_exists
run                 = silver_job.run


# ---------------------------------------------------------------------------
# Patch pyspark.sql.functions so F.col/trim/etc. don't need a live JVM
# ---------------------------------------------------------------------------

def _make_col():
    """A chainable mock Column — supports .alias(), .cast(), .isNull(), etc."""
    col = MagicMock()
    col.alias.return_value    = col
    col.cast.return_value     = col
    col.isNull.return_value   = col
    col.isNotNull.return_value = col
    col.__and__               = lambda s, o: col
    col.__ge__                = lambda s, o: col
    col.__le__                = lambda s, o: col
    return col


_F_STUB = MagicMock()
_F_STUB.col.side_effect               = lambda *a, **kw: _make_col()
_F_STUB.trim.side_effect              = lambda *a, **kw: _make_col()
_F_STUB.upper.side_effect             = lambda *a, **kw: _make_col()
_F_STUB.to_date.side_effect           = lambda *a, **kw: _make_col()
_F_STUB.when.return_value             = _make_col()
_F_STUB.lit.side_effect               = lambda *a, **kw: _make_col()
_F_STUB.current_date.return_value     = _make_col()
_F_STUB.current_timestamp.return_value = _make_col()
_F_STUB.coalesce.side_effect          = lambda *a, **kw: _make_col()

# Patch silver_job's reference to pyspark.sql.functions
silver_job.F = _F_STUB


# ---------------------------------------------------------------------------
# Helper: build a chainable DataFrame mock
# ---------------------------------------------------------------------------

def _make_df(columns=None):
    """Return a MagicMock that looks like a Spark DataFrame."""
    df               = MagicMock()
    df.columns       = list(columns or [])
    df.schema        = MagicMock()
    df.schema.fields = [
        MagicMock(name=c, dataType=StringType()) for c in (columns or [])
    ]
    df.withColumnRenamed.return_value = df
    df.withColumn.return_value        = df
    df.drop.return_value              = df
    df.select.return_value            = df
    df.filter.return_value            = df
    df.dropDuplicates.return_value    = df
    df.cache.return_value             = df
    df.coalesce.return_value          = df
    df.unpersist.return_value         = None
    return df


# ===========================================================================
# apply_renames
# ===========================================================================

class TestApplyRenames(unittest.TestCase):

    def test_renames_existing_column(self):
        df         = _make_df(["CONTRAT"])
        renamed_df = _make_df(["RADICAL"])
        df.withColumnRenamed.return_value = renamed_df
        result = apply_renames(df, {"CONTRAT": "RADICAL"})
        df.withColumnRenamed.assert_called_once_with("CONTRAT", "RADICAL")
        self.assertIs(result, renamed_df)

    def test_ignores_missing_source_column(self):
        df     = _make_df(["NAME"])
        result = apply_renames(df, {"MISSING": "OTHER"})
        df.withColumnRenamed.assert_not_called()
        self.assertIs(result, df)

    def test_empty_renames_returns_same_df(self):
        df     = _make_df(["COL"])
        result = apply_renames(df, {})
        self.assertIs(result, df)

    def test_merge_when_target_already_exists(self):
        # Both CONTRAT and RADICAL are in the df — should coalesce then drop
        df = _make_df(["CONTRAT", "RADICAL"])
        result = apply_renames(df, {"CONTRAT": "RADICAL"})
        df.withColumn.assert_called_once()
        df.drop.assert_called_once_with("CONTRAT")

    def test_coalesce_used_not_rename_when_target_exists(self):
        df = _make_df(["OLD", "NEW"])
        apply_renames(df, {"OLD": "NEW"})
        df.withColumnRenamed.assert_not_called()


# ===========================================================================
# normalize_gender
# ===========================================================================

class TestNormalizeGender(unittest.TestCase):

    def test_calls_withColumn_on_gender(self):
        df = _make_df(["GENDER"])
        normalize_gender(df)
        df.withColumn.assert_called_once()
        col_name = df.withColumn.call_args[0][0]
        self.assertEqual(col_name, "GENDER")

    def test_returns_result_of_withColumn(self):
        df         = _make_df(["GENDER"])
        result_df  = _make_df(["GENDER"])
        df.withColumn.return_value = result_df
        self.assertIs(normalize_gender(df), result_df)


# ===========================================================================
# _spark_to_hive_type
# ===========================================================================

class TestSparkToHiveTypeSilver(unittest.TestCase):

    def test_int_types(self):
        for t in [IntegerType(), ShortType(), ByteType()]:
            self.assertEqual(_spark_to_hive_type(t), "INT")

    def test_long_type(self):
        self.assertEqual(_spark_to_hive_type(LongType()), "BIGINT")

    def test_double_type(self):
        self.assertEqual(_spark_to_hive_type(DoubleType()), "DOUBLE")

    def test_float_type(self):
        self.assertEqual(_spark_to_hive_type(FloatType()), "FLOAT")

    def test_boolean_type(self):
        self.assertEqual(_spark_to_hive_type(BooleanType()), "BOOLEAN")

    def test_date_type(self):
        self.assertEqual(_spark_to_hive_type(DateType()), "DATE")

    def test_timestamp_type(self):
        self.assertEqual(_spark_to_hive_type(TimestampType()), "TIMESTAMP")

    def test_decimal_type(self):
        self.assertEqual(_spark_to_hive_type(DecimalType(10, 2)), "DECIMAL(10,2)")

    def test_binary_type(self):
        self.assertEqual(_spark_to_hive_type(BinaryType()), "BINARY")

    def test_string_fallback(self):
        self.assertEqual(_spark_to_hive_type(StringType()), "STRING")

    def test_unknown_fallback(self):
        class Exotic:
            pass
        self.assertEqual(_spark_to_hive_type(Exotic()), "STRING")


# ===========================================================================
# _build_hive_cols
# ===========================================================================

class TestBuildHiveColsSilver(unittest.TestCase):

    def _schema(self, *field_defs):
        return StructType([StructField(n, t, True) for n, t in field_defs])

    def test_single_column(self):
        result = _build_hive_cols(self._schema(("ID", LongType())))
        self.assertEqual(result.strip(), "`ID` BIGINT")

    def test_multiple_columns_separated_by_comma(self):
        result = _build_hive_cols(self._schema(
            ("A", IntegerType()),
            ("B", StringType()),
        ))
        self.assertIn("`A` INT",    result)
        self.assertIn("`B` STRING", result)
        self.assertIn(",",          result)

    def test_backticks_around_names(self):
        result = _build_hive_cols(self._schema(("my field", StringType())))
        self.assertIn("`my field`", result)

    def test_preserves_order(self):
        result = _build_hive_cols(self._schema(
            ("X", IntegerType()),
            ("Y", DateType()),
            ("Z", StringType()),
        ))
        self.assertLess(result.index("`X`"), result.index("`Y`"))
        self.assertLess(result.index("`Y`"), result.index("`Z`"))


# ===========================================================================
# cast_and_clean
# ===========================================================================

class TestCastAndClean(unittest.TestCase):

    def _minimal_config(self, **overrides):
        base = {
            "renames":      {},
            "casts":        {},
            "not_null":     [],
            "dedup":        [],
            "non_negative": [],
        }
        base.update(overrides)
        return base

    def _str_field(self, name):
        f          = MagicMock()
        f.name     = name
        f.dataType = StringType()
        return f

    def _dbl_field(self, name):
        f          = MagicMock()
        f.name     = name
        f.dataType = DoubleType()
        return f

    def test_select_called_with_all_fields(self):
        df               = _make_df(["A", "B"])
        df.schema.fields = [self._str_field("A"), self._str_field("B")]
        cast_and_clean(df, self._minimal_config())
        df.select.assert_called_once()

    def test_dedup_called_when_columns_specified(self):
        df               = _make_df(["RADICAL", "DATE"])
        df.schema.fields = [self._str_field("RADICAL"), self._str_field("DATE")]
        config = self._minimal_config(dedup=["RADICAL", "DATE"])
        cast_and_clean(df, config)
        df.dropDuplicates.assert_called_once_with(["RADICAL", "DATE"])

    def test_dedup_not_called_when_list_empty(self):
        df               = _make_df(["A"])
        df.schema.fields = [self._str_field("A")]
        cast_and_clean(df, self._minimal_config(dedup=[]))
        df.dropDuplicates.assert_not_called()

    def test_filter_called_when_not_null_specified(self):
        df               = _make_df(["RADICAL"])
        df.schema.fields = [self._str_field("RADICAL")]
        config = self._minimal_config(not_null=["RADICAL"])
        cast_and_clean(df, config)
        df.filter.assert_called_once()

    def test_filter_called_when_non_negative_specified(self):
        df               = _make_df(["MONTANT"])
        df.schema.fields = [self._dbl_field("MONTANT")]
        config = self._minimal_config(
            casts={"MONTANT": "double"},
            non_negative=["MONTANT"],
        )
        cast_and_clean(df, config)
        df.filter.assert_called_once()

    def test_silver_ts_column_added(self):
        df               = _make_df(["ID"])
        df.schema.fields = [self._str_field("ID")]
        cast_and_clean(df, self._minimal_config())
        calls = [c[0][0] for c in df.withColumn.call_args_list]
        self.assertIn("_silver_ts", calls)

    def test_normalize_gender_called_when_flag_true(self):
        df               = _make_df(["GENDER"])
        df.schema.fields = [self._str_field("GENDER")]
        df.columns       = ["GENDER"]
        config = self._minimal_config(normalize_gender=True)
        with patch("silver_job.normalize_gender", return_value=df) as mock_ng:
            cast_and_clean(df, config)
            mock_ng.assert_called_once()

    def test_normalize_gender_not_called_when_flag_false(self):
        df               = _make_df(["GENDER"])
        df.schema.fields = [self._str_field("GENDER")]
        df.columns       = ["GENDER"]
        config = self._minimal_config(normalize_gender=False)
        with patch("silver_job.normalize_gender") as mock_ng:
            cast_and_clean(df, config)
            mock_ng.assert_not_called()



# ===========================================================================
# table_exists


class TestTableExists(unittest.TestCase):

    def _jvm_mock(self, path_exists, has_files):
        fs                       = MagicMock()
        fs.exists.return_value   = path_exists
        ls                       = MagicMock()
        ls.length                = 1 if has_files else 0
        fs.listStatus.return_value = ls

        jvm = MagicMock()
        jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs
        return jvm

    def test_returns_true_when_path_exists_and_has_files(self):
        with patch.object(silver_job, "spark") as sp:
            sp._jvm                             = self._jvm_mock(True, True)
            sp._jsc.hadoopConfiguration.return_value = MagicMock()
            self.assertTrue(table_exists("silver_comptes"))

    def test_returns_false_when_path_does_not_exist(self):
        with patch.object(silver_job, "spark") as sp:
            sp._jvm                             = self._jvm_mock(False, False)
            sp._jsc.hadoopConfiguration.return_value = MagicMock()
            self.assertFalse(table_exists("silver_comptes"))

    def test_returns_false_when_path_exists_but_empty(self):
        with patch.object(silver_job, "spark") as sp:
            sp._jvm                             = self._jvm_mock(True, False)
            sp._jsc.hadoopConfiguration.return_value = MagicMock()
            self.assertFalse(table_exists("silver_soldes"))

    def test_returns_false_on_exception(self):
        with patch.object(silver_job, "spark") as sp:
            sp._jvm.org.apache.hadoop.fs.FileSystem.get.side_effect = Exception("HDFS down")
            sp._jsc.hadoopConfiguration.return_value = MagicMock()
            self.assertFalse(table_exists("silver_comptes"))


# ===========================================================================
# run
# ===========================================================================

class TestRun(unittest.TestCase):

    def _base_df_mock(self):
        df                         = MagicMock()
        df.columns                 = ["RADICAL", "CCLE"]
        df.schema                  = MagicMock()
        df.schema.fields           = []
        df.cache.return_value      = df
        df.coalesce.return_value   = df
        df.write.mode.return_value.parquet = MagicMock()
        df.filter.return_value     = df
        df.dropDuplicates.return_value = df
        df.select.return_value     = df
        df.withColumn.return_value = df

        count_df               = MagicMock()
        count_df.count.return_value = 100
        return df, count_df

    @patch("silver_job.table_exists", return_value=True)
    def test_skips_existing_table_without_force(self, _):
        with patch.object(silver_job, "spark") as sp:
            run("silver_comptes", force=False)
            sp.read.parquet.assert_not_called()

    @patch("silver_job.table_exists", return_value=True)
    def test_processes_existing_table_when_force_true(self, _):
        df, count_df = self._base_df_mock()
        with patch.object(silver_job, "spark") as sp, \
             patch("silver_job.cast_and_clean", return_value=df):
            sp.read.parquet.return_value = count_df
            run("silver_comptes", force=True)
            df.coalesce.assert_called_once()

    @patch("silver_job.table_exists", return_value=False)
    def test_run_calls_cast_and_clean(self, _):
        df, count_df = self._base_df_mock()
        with patch.object(silver_job, "spark") as sp, \
             patch("silver_job.cast_and_clean", return_value=df) as mock_clean:
            sp.read.parquet.return_value = count_df
            run("silver_comptes", force=False)
            mock_clean.assert_called_once()

    @patch("silver_job.table_exists", return_value=False)
    def test_run_unknown_table_raises_key_error(self, _):
        with self.assertRaises(KeyError):
            run("silver_nonexistent")

    @patch("silver_job.table_exists", return_value=False)
    def test_hive_registration_failure_is_non_fatal(self, _):
        df, count_df = self._base_df_mock()
        with patch.object(silver_job, "spark") as sp, \
             patch("silver_job.cast_and_clean", return_value=df):
            sp.read.parquet.return_value = count_df
            sp.sql.side_effect = [None, None, Exception("Hive down"), None]
            run("silver_comptes", force=False)  # must not raise


if __name__ == "__main__":
    unittest.main()
