ALTER TABLE trade_reports
    ADD COLUMN IF NOT EXISTS execution_time timestamptz;

CREATE TABLE IF NOT EXISTS feed_watermarks (
    source text PRIMARY KEY CHECK (source IN ('Internal', 'BrokerA')),
    event_time timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS missing_report_breaks (
    trade_ref text NOT NULL,
    missing_source text NOT NULL CHECK (missing_source IN ('Internal', 'BrokerA')),
    status text NOT NULL CHECK (status IN ('Open', 'AutoResolved')),
    opened_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at timestamptz,
    PRIMARY KEY (trade_ref, missing_source),
    CHECK ((status = 'AutoResolved') = (resolved_at IS NOT NULL))
);
