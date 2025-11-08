-- -------------------------
-- Table: subscribers
-- -------------------------
CREATE TABLE IF NOT EXISTS subscribers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL UNIQUE, -- Telegram user ID
    username TEXT DEFAULT NULL,       -- optionnel
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- -------------------------
-- Table: signals
-- -------------------------
CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pair TEXT NOT NULL,
  direction TEXT NOT NULL,
  reason TEXT,
  ts_enter TEXT NOT NULL,
  ts_send TEXT,
  ts_result TEXT,
  result TEXT,
  confidence REAL,
  payload_json TEXT
);

CREATE TABLE IF NOT EXISTS metadata (
  k TEXT PRIMARY KEY,
  v TEXT
);
