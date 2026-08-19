"""
Unit tests for the pure-Python helpers in spark/jobs/bronze_catalog.py.

spark_to_hive_type and build_hive_cols are copied directly here so we
can test them in isolation — no Spark session or HDFS connection needed.
"""

import unittest

from pyspark.sql.types import (
    IntegerType, ShortType, ByteType, LongType,
    DoubleType, FloatType, StringType, BooleanType,
    DateType, TimestampType, DecimalType, BinaryType,
    StructType, StructField,
)


# ---------------------------------------------------------------------------
# Functions under test (copied verbatim from bronze_catalog.py)
# ---------------------------------------------------------------------------

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
    return ",\n    ".join(
        f"`{f.name}` {spark_to_hive_type(f.dataType)}"
        for f in schema.fields
    )


# ===========================================================================
# spark_to_hive_type
# ===========================================================================

class TestSparkToHiveType(unittest.TestCase):

    def test_integer_type(self):
        self.assertEqual(spark_to_hive_type(IntegerType()), "INT")

    def test_short_type(self):
        self.assertEqual(spark_to_hive_type(ShortType()), "INT")

    def test_byte_type(self):
        self.assertEqual(spark_to_hive_type(ByteType()), "INT")

    def test_long_type(self):
        self.assertEqual(spark_to_hive_type(LongType()), "BIGINT")

    def test_double_type(self):
        self.assertEqual(spark_to_hive_type(DoubleType()), "DOUBLE")

    def test_float_type(self):
        self.assertEqual(spark_to_hive_type(FloatType()), "FLOAT")

    def test_boolean_type(self):
        self.assertEqual(spark_to_hive_type(BooleanType()), "BOOLEAN")

    def test_date_type(self):
        self.assertEqual(spark_to_hive_type(DateType()), "DATE")

    def test_timestamp_type(self):
        self.assertEqual(spark_to_hive_type(TimestampType()), "TIMESTAMP")

    def test_decimal_type_with_precision_scale(self):
        self.assertEqual(spark_to_hive_type(DecimalType(18, 4)), "DECIMAL(18,4)")

    def test_decimal_type_zero_scale(self):
        self.assertEqual(spark_to_hive_type(DecimalType(10, 0)), "DECIMAL(10,0)")

    def test_binary_type(self):
        self.assertEqual(spark_to_hive_type(BinaryType()), "BINARY")

    def test_string_type_returns_string(self):
        self.assertEqual(spark_to_hive_type(StringType()), "STRING")

    def test_unknown_type_falls_back_to_string(self):
        class UnknownType:
            pass
        self.assertEqual(spark_to_hive_type(UnknownType()), "STRING")


# ===========================================================================
# build_hive_cols
# ===========================================================================

class TestBuildHiveCols(unittest.TestCase):

    def _schema(self, *field_defs):
        return StructType([StructField(n, t, True) for n, t in field_defs])

    def test_single_string_column(self):
        result = build_hive_cols(self._schema(("NAME", StringType())))
        self.assertEqual(result.strip(), "`NAME` STRING")

    def test_multiple_columns_joined_by_comma(self):
        result = build_hive_cols(self._schema(
            ("ID",    LongType()),
            ("LABEL", StringType()),
        ))
        parts = [p.strip() for p in result.split(",")]
        self.assertIn("`ID` BIGINT",    parts)
        self.assertIn("`LABEL` STRING", parts)

    def test_column_names_wrapped_in_backticks(self):
        result = build_hive_cols(self._schema(("my col", StringType())))
        self.assertIn("`my col`", result)

    def test_decimal_column_in_output(self):
        result = build_hive_cols(self._schema(("AMOUNT", DecimalType(15, 2))))
        self.assertIn("DECIMAL(15,2)", result)

    def test_date_column_in_output(self):
        result = build_hive_cols(self._schema(("DATE_CHARG", DateType())))
        self.assertIn("`DATE_CHARG` DATE", result)

    def test_preserves_column_order(self):
        result = build_hive_cols(self._schema(
            ("A", IntegerType()),
            ("B", StringType()),
            ("C", DateType()),
        ))
        self.assertLess(result.index("`A`"), result.index("`B`"))
        self.assertLess(result.index("`B`"), result.index("`C`"))

    def test_mixed_numeric_types(self):
        result = build_hive_cols(self._schema(
            ("INT_COL",    IntegerType()),
            ("DOUBLE_COL", DoubleType()),
            ("FLOAT_COL",  FloatType()),
            ("LONG_COL",   LongType()),
        ))
        self.assertIn("`INT_COL` INT",       result)
        self.assertIn("`DOUBLE_COL` DOUBLE", result)
        self.assertIn("`FLOAT_COL` FLOAT",   result)
        self.assertIn("`LONG_COL` BIGINT",   result)


if __name__ == "__main__":
    unittest.main()
