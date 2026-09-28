import os
import sqlite3
import unittest

import upload_d1


def row(code, name="Drug", price="10"):
    return {column: "" for column in upload_d1.COLUMNS} | {
        "藥品代號": code,
        "藥品英文名稱": name,
        "支付價": price,
    }


class IncrementalUploadTests(unittest.TestCase):
    def test_hash_is_independent_of_input_order(self):
        first = {"B": row("B"), "A": row("A")}
        second = {"A": row("A"), "B": row("B")}
        self.assertEqual(upload_d1.dataset_hash(first), upload_d1.dataset_hash(second))

    def test_compare_detects_insert_update_and_delete(self):
        incoming = {"A": row("A", "Changed"), "C": row("C")}
        existing = {"A": row("A", "Old"), "B": row("B")}
        diff = upload_d1.compare_rows(incoming, existing)
        self.assertEqual([item["藥品代號"] for item in diff.inserted], ["C"])
        self.assertEqual([item["藥品代號"] for item in diff.updated], ["A"])
        self.assertEqual(diff.deleted, ["B"])
        self.assertEqual(diff.changed_count, 3)

    def test_budget_blocks_projected_usage_above_operating_threshold(self):
        allowed, reason = upload_d1.budget_decision(
            {"rows_read": 100, "rows_written": upload_d1.WRITE_PAUSE_THRESHOLD - 5},
            predicted_read=10,
            predicted_write=10,
        )
        self.assertFalse(allowed)
        self.assertIn("寫入", reason)

    def test_unknown_usage_allows_only_small_change(self):
        self.assertTrue(upload_d1.budget_decision(None, 100, 10)[0])
        self.assertFalse(
            upload_d1.budget_decision(None, 100, upload_d1.UNKNOWN_USAGE_WRITE_CAP + 1)[0]
        )

    def test_data_quality_blocks_large_delete(self):
        diff = upload_d1.Diff(inserted=[], updated=[], deleted=[str(i) for i in range(6)])
        allowed, reason = upload_d1.data_quality_decision(diff, 100, 94)
        self.assertFalse(allowed)
        self.assertIn("刪除", reason)

    def test_incremental_sql_applies_expected_state(self):
        existing = {"A": row("A", "Old"), "B": row("B")}
        incoming = {"A": row("A", "New"), "C": row("C")}
        diff = upload_d1.compare_rows(incoming, existing)

        sql_path = "test_incremental_output.sql"
        try:
            upload_d1.generate_incremental_sql(diff, "abc123", 2, sql_path)
            with open(sql_path, encoding="utf-8") as source:
                sql = source.read()
        finally:
            if os.path.exists(sql_path):
                os.remove(sql_path)

        self.assertNotIn("BEGIN TRANSACTION", sql)
        self.assertNotIn("COMMIT;", sql)

        db = sqlite3.connect(":memory:")
        column_sql = ", ".join(f'"{column}" TEXT' for column in upload_d1.COLUMNS)
        db.execute(f"CREATE TABLE nhi_drugs (id INTEGER PRIMARY KEY AUTOINCREMENT, {column_sql}, updated_at TEXT)")
        db.execute("CREATE TABLE sync_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, sync_time TEXT DEFAULT CURRENT_TIMESTAMP, status TEXT, total_records INTEGER)")
        db.execute(upload_d1.SYNC_STATE_SCHEMA)
        placeholders = ",".join("?" for _ in upload_d1.COLUMNS)
        quoted_columns = ",".join(f'"{column}"' for column in upload_d1.COLUMNS)
        db.executemany(
            f"INSERT INTO nhi_drugs ({quoted_columns}) VALUES ({placeholders})",
            [[item[column] for column in upload_d1.COLUMNS] for item in existing.values()],
        )
        db.commit()
        db.executescript(sql)

        actual = dict(db.execute("SELECT 藥品代號, 藥品英文名稱 FROM nhi_drugs ORDER BY 藥品代號"))
        self.assertEqual(actual, {"A": "New", "C": "Drug"})
        state = db.execute("SELECT data_hash, last_status, total_records FROM sync_state WHERE source='nhi'").fetchone()
        self.assertEqual(state, ("abc123", "success", 2))


if __name__ == "__main__":
    unittest.main()
