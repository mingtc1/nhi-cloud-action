-- Compact TFDA ingredient profiles used only to refine same-group Level 1 candidates.
-- One row per license keeps the initial load and subsequent updates within D1 Free limits.
CREATE TABLE IF NOT EXISTS drug_ingredient_profiles (
  license_no TEXT PRIMARY KEY,
  profile_hash TEXT NOT NULL,
  components_json TEXT NOT NULL,
  component_count INTEGER NOT NULL,
  profile_status TEXT NOT NULL,
  updated_at TEXT DEFAULT (datetime('now'))
);
