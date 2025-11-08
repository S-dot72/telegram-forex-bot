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
