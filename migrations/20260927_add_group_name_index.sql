-- One-time production migration for Level 1-3 substitute queries and group options.
-- Safe to rerun: the index is created only when it does not already exist.
CREATE INDEX IF NOT EXISTS idx_group_name ON nhi_drugs(分類分組名稱);
