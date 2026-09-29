import csv
import json
import shutil
import sqlite3
import unittest
import uuid
import zipfile
from pathlib import Path

import sync_ingredient_profiles as profiles


class IngredientProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(f".test-ingredient-profiles-{uuid.uuid4().hex}")
        self.tmpdir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_amount_and_unit_normalization(self):
        self.assertEqual(profiles.normalize_amount("125.000000"), "125")
        self.assertEqual(profiles.normalize_amount("0.040000"), "0.04")
        self.assertEqual(profiles.normalize_unit("ug"), "MCG")
        self.assertEqual(profiles.normalize_unit("GM"), "G")

    def test_profile_uses_all_ingredients_and_ignores_row_order(self):
        root = self.tmpdir
        nhi_path = root / "nhi.csv"
        with nhi_path.open("w", encoding="utf-8-sig", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=["許可證字號"])
            writer.writeheader()
            writer.writerow({"許可證字號": "衛部藥輸字第028405號"})

        header = ["許可證字號", "處方標示", "成分名稱", "成分代碼", "含量描述", "含量", "含量單位"]
        rows = [
            ["衛部藥輸字第028405號", "Each Capsule contains:", "B", "2", "", "80.000", "MG"],
            ["衛部藥輸字第028405號", "Each Capsule contains:", "A", "1", "", "125.000", "MG"],
        ]
        hashes = []
        for index, ordered_rows in enumerate((rows, list(reversed(rows)))):
            zip_path = root / f"tfda-{index}.zip"
            csv_text = root / f"tfda-{index}.csv"
            with csv_text.open("w", encoding="utf-8-sig", newline="") as target:
                writer = csv.writer(target)
                writer.writerow(header)
                writer.writerows(ordered_rows)
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.write(csv_text, arcname="43_2.csv")
            result, report = profiles.build_profiles(str(nhi_path), str(zip_path))
            self.assertEqual(report["ready_profile_count"], 1)
            row = result["衛部藥輸字第028405號"]
            self.assertEqual(row["component_count"], 2)
            self.assertEqual(len(json.loads(row["components_json"])), 2)
            hashes.append(row["profile_hash"])
        self.assertEqual(hashes[0], hashes[1])

    def test_equivalent_mass_units_have_the_same_signature(self):
        mg = {"code": "1", "name": "A", "amount": "125", "unit": "MG"}
        gram = {"code": "1", "name": "A", "amount": "0.125", "unit": "G"}
        self.assertEqual(profiles.comparison_signature(mg), profiles.comparison_signature(gram))

    def test_missing_structured_amount_is_partial(self):
        component, comparable = profiles.component_from_row({
            "成分名稱": "APREPITANT",
            "成分代碼": "9200095800",
            "含量描述": "equivalent to 125 mg",
            "含量": "",
            "含量單位": "MG",
            "處方標示": "Each capsule contains",
        })
        self.assertFalse(comparable)
        self.assertEqual(component["description"], "EQUIVALENT TO 125 MG")
        self.assertTrue(profiles.profile_component_signature(component).startswith("PARTIAL|"))

    def test_partial_profile_builds_without_numeric_conversion(self):
        nhi_path = self.tmpdir / "nhi.csv"
        with nhi_path.open("w", encoding="utf-8-sig", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=["許可證字號"])
            writer.writeheader()
            writer.writerow({"許可證字號": "衛部藥輸字第000001號"})

        csv_path = self.tmpdir / "tfda.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as target:
            writer = csv.writer(target)
            writer.writerow(["許可證字號", "處方標示", "成分名稱", "成分代碼", "含量描述", "含量", "含量單位"])
            writer.writerow(["衛部藥輸字第000001號", "Each capsule contains", "A", "1", "equivalent to 1 mg", "", "MG"])
        zip_path = self.tmpdir / "tfda.zip"
        with zipfile.ZipFile(zip_path, "w") as archive:
            archive.write(csv_path, arcname="43_2.csv")

        result, report = profiles.build_profiles(str(nhi_path), str(zip_path))
        self.assertEqual(result["衛部藥輸字第000001號"]["profile_status"], "partial")
        self.assertEqual(report["partial_profile_count"], 1)

    def test_diff_detects_only_changed_profiles(self):
        old = {
            "A": {"license_no": "A", "profile_hash": "1", "components_json": "[]", "component_count": 0, "profile_status": "partial", "source_hash": "old"},
            "B": {"license_no": "B", "profile_hash": "2", "components_json": "[]", "component_count": 0, "profile_status": "partial", "source_hash": "old"},
        }
        new = {
            "A": {**old["A"], "source_hash": "new"},
            "C": {"license_no": "C", "profile_hash": "3", "components_json": "[]", "component_count": 0, "profile_status": "partial", "source_hash": "new"},
        }
        diff = profiles.compare_profiles(new, old)
        self.assertEqual(diff.updated, [])
        self.assertEqual([row["license_no"] for row in diff.inserted], ["C"])
        self.assertEqual(diff.deleted, ["B"])

    def test_generated_incremental_sql_applies_profile_changes(self):
        inserted = {
            "license_no": "A",
            "profile_hash": "hash-a",
            "components_json": "[]",
            "component_count": 0,
            "profile_status": "partial",
        }
        sql_path = self.tmpdir / "profiles.sql"
        profiles.generate_sql(
            profiles.ProfileDiff(inserted=[inserted], updated=[], deleted=[]),
            "dataset-hash",
            1,
            str(sql_path),
        )
        db = sqlite3.connect(":memory:")
        db.executescript(profiles.TABLE_SCHEMA)
        db.executescript("""
          CREATE TABLE sync_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, sync_time TEXT DEFAULT CURRENT_TIMESTAMP, status TEXT, total_records INTEGER);
          CREATE TABLE sync_state (
            source TEXT PRIMARY KEY, data_hash TEXT, last_checked_at TEXT, last_changed_at TEXT,
            last_status TEXT, total_records INTEGER, pending_hash TEXT, pending_reason TEXT,
            next_retry_at TEXT, updated_at TEXT
          );
        """)
        db.executescript(sql_path.read_text(encoding="utf-8"))
        self.assertEqual(
            db.execute("SELECT license_no, profile_hash FROM drug_ingredient_profiles").fetchone(),
            ("A", "hash-a"),
        )
        self.assertEqual(
            db.execute("SELECT source, data_hash FROM sync_state").fetchone(),
            ("tfda_ingredients", "dataset-hash"),
        )


if __name__ == "__main__":
    unittest.main()
