CREATE TABLE IF NOT EXISTS trade_reports (
    report_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trade_ref text NOT NULL,
    source text NOT NULL CHECK (source IN ('Internal', 'BrokerA')),
    version integer NOT NULL CHECK (version > 0),
    quantity bigint NOT NULL CHECK (quantity > 0),
    status_after_report text NOT NULL CHECK (
        status_after_report IN ('WaitingForInternal', 'WaitingForBroker', 'Mismatched', 'Matched')
    ),
    recorded_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (trade_ref, source, version)
);

CREATE TABLE IF NOT EXISTS quantity_breaks (
    break_id uuid PRIMARY KEY,
    trade_ref text NOT NULL,
    internal_quantity bigint NOT NULL CHECK (internal_quantity > 0),
    broker_quantity bigint NOT NULL CHECK (broker_quantity > 0),
    status text NOT NULL CHECK (status IN ('Open', 'AutoResolved')),
    opened_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at timestamptz,
    CHECK (internal_quantity <> broker_quantity),
    CHECK ((status = 'AutoResolved') = (resolved_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS quantity_breaks_trade_ref_idx ON quantity_breaks (trade_ref);
