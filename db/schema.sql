PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS collected_evidence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  record_id TEXT NOT NULL UNIQUE,
  company_name TEXT NOT NULL,
  uscc_or_entity_id TEXT,
  source_type TEXT NOT NULL,
  source_platform TEXT NOT NULL,
  source_url TEXT NOT NULL UNIQUE,
  source_title TEXT,
  published_at TEXT,
  captured_at TEXT NOT NULL,
  city TEXT,
  job_title TEXT,
  evidence_quote TEXT,
  evidence_summary TEXT,
  screenshot_path TEXT,
  collector TEXT,
  verification_status TEXT NOT NULL DEFAULT 'pending',
  risk_level TEXT NOT NULL DEFAULT 'medium',
  boycott_recommended INTEGER NOT NULL DEFAULT 0,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS verified_evidence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  collected_id INTEGER NOT NULL UNIQUE,
  record_id TEXT NOT NULL UNIQUE,
  company_name TEXT NOT NULL,
  verification_status TEXT NOT NULL,
  risk_level TEXT NOT NULL,
  boycott_recommended INTEGER NOT NULL DEFAULT 0,
  verifier TEXT NOT NULL,
  verification_note TEXT,
  verified_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (collected_id) REFERENCES collected_evidence(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS company_products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_name TEXT NOT NULL,
  product_name TEXT NOT NULL,
  product_category TEXT,
  product_url TEXT,
  confidence TEXT NOT NULL DEFAULT 'unverified',
  source_note TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(company_name, product_name)
);

CREATE TABLE IF NOT EXISTS pending_product_submissions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_name TEXT NOT NULL,
  product_name TEXT NOT NULL,
  product_category TEXT,
  product_url TEXT,
  source_note TEXT,
  submitted_by TEXT NOT NULL DEFAULT 'community-user',
  review_status TEXT NOT NULL DEFAULT 'pending',
  reviewed_by TEXT,
  reviewed_note TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_collected_company ON collected_evidence(company_name);
CREATE INDEX IF NOT EXISTS idx_collected_status ON collected_evidence(verification_status);
CREATE INDEX IF NOT EXISTS idx_verified_company ON verified_evidence(company_name);
CREATE INDEX IF NOT EXISTS idx_products_company ON company_products(company_name);
CREATE INDEX IF NOT EXISTS idx_pending_products_company ON pending_product_submissions(company_name);
CREATE INDEX IF NOT EXISTS idx_pending_products_status ON pending_product_submissions(review_status);
