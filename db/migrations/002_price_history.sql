ALTER TABLE trade_reports
    ADD COLUMN IF NOT EXISTS price numeric(18,8) CHECK (price > 0);

CREATE TABLE IF NOT EXISTS price_breaks (
    break_id uuid PRIMARY KEY,
    trade_ref text NOT NULL,
    internal_price numeric(18,8) NOT NULL CHECK (internal_price > 0),
    broker_price numeric(18,8) NOT NULL CHECK (broker_price > 0),
    status text NOT NULL CHECK (status IN ('Open', 'AutoResolved')),
    opened_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at timestamptz,
    CHECK (internal_price <> broker_price),
    CHECK ((status = 'AutoResolved') = (resolved_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS price_breaks_trade_ref_idx ON price_breaks (trade_ref);
