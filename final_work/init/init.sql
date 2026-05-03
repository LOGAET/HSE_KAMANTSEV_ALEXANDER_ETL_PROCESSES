CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS staging.user_sessions (
    session_id      VARCHAR(50) PRIMARY KEY,
    user_id         VARCHAR(50) NOT NULL,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    pages_visited   TEXT[],
    device          VARCHAR(20),
    actions         TEXT[],
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.support_tickets (
    ticket_id       VARCHAR(50) PRIMARY KEY,
    user_id         VARCHAR(50),
    status          VARCHAR(20),
    issue_type      VARCHAR(50),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.event_logs (
    event_id        VARCHAR(50),
    event_timestamp TIMESTAMP NOT NULL,
    event_type      VARCHAR(50),
    details         TEXT,
    loaded_at       TIMESTAMP DEFAULT NOW()
) PARTITION BY RANGE (event_timestamp);

CREATE TABLE IF NOT EXISTS staging.event_logs_2024
    PARTITION OF staging.event_logs
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS staging.event_logs_2025
    PARTITION OF staging.event_logs
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS staging.event_logs_2026
    PARTITION OF staging.event_logs
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS staging.user_recommendations (
    user_id             VARCHAR(50) PRIMARY KEY,
    recommended_products TEXT[],
    last_updated        TIMESTAMP,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.moderation_queue (
    review_id           VARCHAR(50) PRIMARY KEY,
    user_id             VARCHAR(50),
    product_id          VARCHAR(50),
    review_text         TEXT,
    rating              INT CHECK (rating BETWEEN 1 AND 5),
    moderation_status   VARCHAR(20),
    flags               TEXT[],
    submitted_at        TIMESTAMP,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.user_activity (
    user_id             VARCHAR(50),
    activity_date       DATE,
    total_sessions      INT,
    avg_session_min     NUMERIC(10,2),
    total_pages         INT,
    most_used_device    VARCHAR(20),
    PRIMARY KEY (user_id, activity_date)
);

CREATE TABLE IF NOT EXISTS analytics.support_stats (
    status              VARCHAR(20),
    issue_type          VARCHAR(50),
    week                DATE,
    ticket_count        INT,
    avg_resolution_hours NUMERIC(10,2),
    open_tickets        INT,
    PRIMARY KEY (status, issue_type, week)
);
