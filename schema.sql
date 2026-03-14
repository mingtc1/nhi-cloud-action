-- NHI Drug Database Schema for Cloudflare D1
-- This schema is applied via: wrangler d1 execute nhi-drugs --remote --file=./schema.sql

DROP TABLE IF EXISTS nhi_drugs;

CREATE TABLE nhi_drugs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    異動 TEXT,
    藥品代號 TEXT,
    藥品英文名稱 TEXT,
    藥品中文名稱 TEXT,
    成分 TEXT,
    規格量 TEXT,
    規格單位 TEXT,
    單複方 TEXT,
    支付價 TEXT,
    有效起日 TEXT,
    有效迄日 TEXT,
    藥商 TEXT,
    製造廠名稱 TEXT,
    劑型 TEXT,
    藥品分類 TEXT,
    分類分組名稱 TEXT,
    ATC代碼 TEXT,
    給付規定章節 TEXT,
    藥品代碼超連結 TEXT,
    給付規定章節連結 TEXT,
    許可證字號 TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

-- Performance indexes
CREATE INDEX idx_drug_code ON nhi_drugs(藥品代號);
CREATE INDEX idx_license ON nhi_drugs(許可證字號);
CREATE INDEX idx_atc ON nhi_drugs(ATC代碼);
