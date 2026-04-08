PRAGMA foreign_keys = ON;

CREATE TABLE accounts (
    account_id INTEGER PRIMARY KEY,
    account_name TEXT NOT NULL,
    plan_tier TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE sessions (
    session_id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL,
    user_identifier TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    device_type TEXT NOT NULL,
    country TEXT NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

CREATE TABLE page_views (
    page_view_id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    page_path TEXT NOT NULL,
    referrer TEXT,
    viewed_at TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE conversions (
    conversion_id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    conversion_type TEXT NOT NULL,
    revenue_cents INTEGER NOT NULL,
    converted_at TEXT NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE INDEX idx_sessions_account_id ON sessions(account_id);
CREATE INDEX idx_sessions_started_at ON sessions(started_at);
CREATE INDEX idx_page_views_session_id ON page_views(session_id);
CREATE INDEX idx_page_views_viewed_at ON page_views(viewed_at);
CREATE INDEX idx_conversions_session_id ON conversions(session_id);

