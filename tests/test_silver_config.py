"""
Unit tests for spark/jobs/silver_config.py

silver_config.py is a pure data module (no Spark, no I/O).
We validate:
  - All required keys present on every group
  - Source lists are non-empty
  - Cast values have supported types
  - Dedup columns are a subset of cast columns
  - not_null columns are a subset of cast columns
  - non_negative columns are a subset of cast columns
  - allow_missing is a bool
  - date_floor is a valid ISO date string
"""

import sys
import pathlib
import unittest
from datetime import date

# Ensure spark/jobs is importable
SPARK_JOBS = pathlib.Path(__file__).parent.parent / "spark" / "jobs"
sys.path.insert(0, str(SPARK_JOBS))

from silver_config import SILVER_GROUPS

REQUIRED_KEYS = {"sources", "allow_missing", "renames", "date_floor", "casts", "not_null", "dedup", "non_negative"}

VALID_SIMPLE_TYPES = {"int", "string", "double", "float", "boolean", "long"}


class TestSilverConfigStructure(unittest.TestCase):

    def test_silver_groups_is_non_empty(self):
        self.assertGreater(len(SILVER_GROUPS), 0)

    def test_all_required_keys_present(self):
        for name, cfg in SILVER_GROUPS.items():
            for key in REQUIRED_KEYS:
                self.assertIn(key, cfg, f"[{name}] missing key '{key}'")

    def test_sources_lists_are_non_empty(self):
        for name, cfg in SILVER_GROUPS.items():
            self.assertGreater(len(cfg["sources"]), 0, f"[{name}] sources list is empty")

    def test_sources_are_strings(self):
        for name, cfg in SILVER_GROUPS.items():
            for src in cfg["sources"]:
                self.assertIsInstance(src, str, f"[{name}] source {src!r} is not a string")

    def test_allow_missing_is_boolean(self):
        for name, cfg in SILVER_GROUPS.items():
            self.assertIsInstance(cfg["allow_missing"], bool, f"[{name}] allow_missing must be bool")

    def test_renames_is_dict(self):
        for name, cfg in SILVER_GROUPS.items():
            self.assertIsInstance(cfg["renames"], dict, f"[{name}] renames must be dict")

    def test_date_floor_is_valid_iso_date(self):
        for name, cfg in SILVER_GROUPS.items():
            try:
                d = date.fromisoformat(cfg["date_floor"])
                self.assertIsInstance(d, date)
            except ValueError:
                self.fail(f"[{name}] date_floor '{cfg['date_floor']}' is not a valid ISO date")

    def test_casts_values_are_valid_types(self):
        for name, cfg in SILVER_GROUPS.items():
            for col, dtype in cfg["casts"].items():
                if isinstance(dtype, tuple):
                    self.assertEqual(len(dtype), 2, f"[{name}][{col}] tuple cast must be (type, format)")
                    self.assertEqual(dtype[0], "date", f"[{name}][{col}] only 'date' tuple casts supported")
                    self.assertIsInstance(dtype[1], str, f"[{name}][{col}] date format must be string")
                else:
                    self.assertIn(
                        dtype, VALID_SIMPLE_TYPES,
                        f"[{name}][{col}] invalid cast type '{dtype}'"
                    )

    def test_not_null_columns_exist_in_casts(self):
        for name, cfg in SILVER_GROUPS.items():
            cast_cols = set(cfg["casts"].keys())
            for col in cfg["not_null"]:
                self.assertIn(col, cast_cols, f"[{name}] not_null column '{col}' not in casts")

    def test_dedup_columns_exist_in_casts(self):
        for name, cfg in SILVER_GROUPS.items():
            cast_cols = set(cfg["casts"].keys())
            for col in cfg["dedup"]:
                self.assertIn(col, cast_cols, f"[{name}] dedup column '{col}' not in casts")

    def test_non_negative_columns_exist_in_casts(self):
        for name, cfg in SILVER_GROUPS.items():
            cast_cols = set(cfg["casts"].keys())
            for col in cfg["non_negative"]:
                self.assertIn(col, cast_cols, f"[{name}] non_negative column '{col}' not in casts")

    def test_list_fields_are_lists(self):
        for name, cfg in SILVER_GROUPS.items():
            for field in ("not_null", "dedup", "non_negative", "sources"):
                self.assertIsInstance(cfg[field], list, f"[{name}][{field}] must be a list")

    def test_no_duplicate_sources(self):
        for name, cfg in SILVER_GROUPS.items():
            sources = cfg["sources"]
            self.assertEqual(len(sources), len(set(sources)), f"[{name}] duplicate sources found")

    def test_no_duplicate_not_null_columns(self):
        for name, cfg in SILVER_GROUPS.items():
            cols = cfg["not_null"]
            self.assertEqual(len(cols), len(set(cols)), f"[{name}] duplicate not_null columns")

    def test_no_duplicate_dedup_columns(self):
        for name, cfg in SILVER_GROUPS.items():
            cols = cfg["dedup"]
            self.assertEqual(len(cols), len(set(cols)), f"[{name}] duplicate dedup columns")


class TestSilverConfigSpecificGroups(unittest.TestCase):
    """Spot-checks on key business-critical tables."""

    def test_silver_comptes_has_all_sources(self):
        sources = SILVER_GROUPS["silver_comptes"]["sources"]
        self.assertIn("ATT_PROD_EPARGNE_COMPTE2024", sources)
        self.assertIn("ATT_HISSAB_COMPTE2023",        sources)

    def test_silver_comptes_not_null_columns(self):
        not_null = SILVER_GROUPS["silver_comptes"]["not_null"]
        self.assertIn("RADICAL", not_null)
        self.assertIn("CCLE",    not_null)

    def test_silver_perimetre_date_floor_is_1900(self):
        # Birthdays go back to 1900
        df = SILVER_GROUPS["silver_perimetre"]["date_floor"]
        self.assertEqual(df, "1900-01-01")

    def test_silver_perimetre_has_normalize_gender_flag(self):
        self.assertTrue(SILVER_GROUPS["silver_perimetre"].get("normalize_gender", False))

    def test_silver_gab_allow_missing_is_true(self):
        self.assertTrue(SILVER_GROUPS["silver_gab"]["allow_missing"])

    def test_silver_tpe_allow_missing_is_true(self):
        self.assertTrue(SILVER_GROUPS["silver_tpe"]["allow_missing"])

    def test_silver_vignette_renames_contrat_to_radical(self):
        renames = SILVER_GROUPS["silver_vignette"]["renames"]
        self.assertEqual(renames.get("CONTRAT"), "RADICAL")

    def test_silver_virement_renames_contrat_to_radical(self):
        renames = SILVER_GROUPS["silver_virement"]["renames"]
        self.assertEqual(renames.get("CONTRAT"), "RADICAL")

    def test_silver_soldes_non_negative_soldeverif(self):
        self.assertIn("SOLDEVERIF", SILVER_GROUPS["silver_soldes"]["non_negative"])

    def test_silver_depot_non_negative_montant_depot(self):
        self.assertIn("MONTANT_DEPOT", SILVER_GROUPS["silver_depot"]["non_negative"])

    def test_silver_retrait_dedup_columns(self):
        dedup = SILVER_GROUPS["silver_retrait"]["dedup"]
        for col in ("RADICAL", "NUM_CARTE", "DATE_OP", "MONTANT"):
            self.assertIn(col, dedup)

    def test_silver_carte_not_null_includes_numero_carte(self):
        self.assertIn("NUMERO_CARTE", SILVER_GROUPS["silver_carte"]["not_null"])

    def test_silver_digital_single_source(self):
        self.assertEqual(len(SILVER_GROUPS["silver_digital"]["sources"]), 1)

    def test_silver_perimetre_single_source(self):
        self.assertEqual(len(SILVER_GROUPS["silver_perimetre"]["sources"]), 1)

    def test_total_group_count(self):
        # 17 silver groups defined in silver_config.py
        self.assertEqual(len(SILVER_GROUPS), 17)


if __name__ == "__main__":
    unittest.main()
