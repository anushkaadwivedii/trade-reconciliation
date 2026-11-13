using Npgsql;

namespace TradeReconciliation.KafkaDemo;

internal sealed partial class PostgresTradeStore
{
    // Reports and watermarks share this transaction lock so neither can miss the other's update.
    private static async Task LockFeedProgressAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(829316055)", connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> SaveWatermarkAsync(
        TradeSource source, DateTimeOffset watermark, TimeSpan gracePeriod, CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await LockFeedProgressAsync(connection, transaction, cancellationToken);

        await using var advance = new NpgsqlCommand("""
            INSERT INTO feed_watermarks (source, event_time) VALUES ($1, $2)
            ON CONFLICT (source) DO UPDATE SET event_time = EXCLUDED.event_time
            WHERE feed_watermarks.event_time < EXCLUDED.event_time
            """, connection, transaction)
        {
            Parameters = { new() { Value = source.ToString() }, new() { Value = watermark } }
        };

        var opened = 0;
        if (await advance.ExecuteNonQueryAsync(cancellationToken) > 0)
        {
            opened = await OpenEligibleBreaksAsync(connection, transaction, source, gracePeriod, null, cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return opened;
    }

    private static async Task UpdateMissingBreakAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        TradeReport report,
        TimeSpan gracePeriod,
        CancellationToken cancellationToken)
    {
        await using var resolve = new NpgsqlCommand("""
            UPDATE missing_report_breaks SET status = 'AutoResolved', resolved_at = CURRENT_TIMESTAMP
            WHERE trade_ref = $1 AND missing_source = $2 AND status = 'Open'
            """, connection, transaction)
        {
            Parameters = { new() { Value = report.TradeRef }, new() { Value = report.Source.ToString() } }
        };
        await resolve.ExecuteNonQueryAsync(cancellationToken);

        var otherSource = report.Source == TradeSource.Internal ? TradeSource.BrokerA : TradeSource.Internal;
        await OpenEligibleBreaksAsync(connection, transaction, otherSource, gracePeriod, report.TradeRef, cancellationToken);
    }

    private static async Task<int> OpenEligibleBreaksAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        TradeSource missingSource,
        TimeSpan gracePeriod,
        string? tradeRef,
        CancellationToken cancellationToken)
    {
        const string candidatesSql = """
            SELECT DISTINCT ON (report.trade_ref) report.trade_ref, report.execution_time, watermark.event_time
            FROM trade_reports report
            JOIN feed_watermarks watermark ON watermark.source = $1
            WHERE report.source <> $1 AND report.execution_time IS NOT NULL
              AND ($2::text IS NULL OR report.trade_ref = $2)
              AND NOT EXISTS (
                  SELECT 1 FROM trade_reports other
                  WHERE other.trade_ref = report.trade_ref AND other.source = $1
              )
              AND NOT EXISTS (
                  SELECT 1 FROM missing_report_breaks existing
                  WHERE existing.trade_ref = report.trade_ref AND existing.missing_source = $1
              )
            ORDER BY report.trade_ref, report.report_id
            """;

        await using var candidates = new NpgsqlCommand(candidatesSql, connection, transaction)
        {
            Parameters =
            {
                new() { Value = missingSource.ToString() },
                new() { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)tradeRef ?? DBNull.Value }
            }
        };

        var presentSource = missingSource == TradeSource.Internal ? TradeSource.BrokerA : TradeSource.Internal;
        var tracker = new MissingReportTracker(gracePeriod);
        var opened = new List<MissingReportBreak>();
        await using (var rows = await candidates.ExecuteReaderAsync(cancellationToken))
        {
            while (await rows.ReadAsync(cancellationToken))
            {
                tracker.AdvanceWatermark(missingSource, rows.GetFieldValue<DateTimeOffset>(2));
                var change = tracker.Observe(rows.GetString(0), presentSource, rows.GetFieldValue<DateTimeOffset>(1));
                if (change is not null)
                {
                    opened.Add(change);
                }
            }
        }

        foreach (var change in opened)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO missing_report_breaks (trade_ref, missing_source, status)
                VALUES ($1, $2, 'Open') ON CONFLICT (trade_ref, missing_source) DO NOTHING
                """, connection, transaction)
            {
                Parameters = { new() { Value = change.TradeRef }, new() { Value = change.MissingSource.ToString() } }
            };
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        return opened.Count;
    }
}
