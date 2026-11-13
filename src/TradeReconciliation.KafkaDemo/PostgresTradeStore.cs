using Npgsql;
using NpgsqlTypes;

namespace TradeReconciliation.KafkaDemo;

internal sealed class PostgresTradeStore(NpgsqlDataSource database)
{
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // Serialize setup if two demo processes start against an empty database.
        await using var schemaLock = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(829316054)", connection, transaction);
        await schemaLock.ExecuteNonQueryAsync(cancellationToken);

        foreach (var resource in new[] { "TradeHistory.sql", "PriceHistory.sql" })
        {
            await using var schema = typeof(PostgresTradeStore).Assembly.GetManifestResourceStream(resource)
                ?? throw new InvalidOperationException($"Missing database schema {resource}.");
            using var reader = new StreamReader(schema);
            var sql = await reader.ReadToEndAsync(cancellationToken);
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task SaveAsync(
        TradeReport report,
        ReconciliationResult result,
        CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await WriteAsync(connection, transaction, report, result, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<ReconciliationResult?> ApplyAndSaveAsync(
        TradeReport report,
        CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // Serialize updates for the same trade, including during a consumer rebalance.
        await using var tradeLock = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", connection, transaction)
        {
            Parameters = { new() { Value = report.TradeRef } }
        };
        await tradeLock.ExecuteNonQueryAsync(cancellationToken);

        var reconciler = new TradeReconciler();
        await using var history = new NpgsqlCommand("""
            SELECT source, version, quantity, price
            FROM trade_reports
            WHERE trade_ref = $1
            ORDER BY report_id
            """, connection, transaction)
        {
            Parameters = { new() { Value = report.TradeRef } }
        };

        await using (var rows = await history.ExecuteReaderAsync(cancellationToken))
        {
            while (await rows.ReadAsync(cancellationToken))
            {
                var source = Enum.Parse<TradeSource>(rows.GetString(0));
                var version = rows.GetInt32(1);
                var quantity = rows.GetInt64(2);
                decimal? price = rows.IsDBNull(3) ? null : rows.GetDecimal(3);
                if (source == report.Source && version == report.Version)
                {
                    if (quantity != report.Quantity || price != report.Price)
                    {
                        throw new FormatException("Report repeats a saved version with a different quantity or price.");
                    }

                    // PostgreSQL already committed this report and its break changes.
                    return null;
                }

                reconciler.Apply(new TradeReport(report.TradeRef, source, version, quantity, price));
            }
        }

        var result = reconciler.Apply(report);
        await WriteAsync(connection, transaction, report, result, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    private static async Task WriteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        TradeReport report,
        ReconciliationResult result,
        CancellationToken cancellationToken)
    {
        const string insertReport = """
            INSERT INTO trade_reports (trade_ref, source, version, quantity, status_after_report, price)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (trade_ref, source, version) DO NOTHING
            """;

        await using var reportCommand = new NpgsqlCommand(insertReport, connection, transaction)
        {
            Parameters =
            {
                new() { Value = report.TradeRef },
                new() { Value = report.Source.ToString() },
                new() { Value = report.Version },
                new() { Value = report.Quantity },
                new() { Value = result.Status.ToString() },
                new() { NpgsqlDbType = NpgsqlDbType.Numeric, Value = (object?)report.Price ?? DBNull.Value }
            }
        };

        if (await reportCommand.ExecuteNonQueryAsync(cancellationToken) == 0)
        {
            return;
        }

        foreach (var quantityBreak in result.Breaks)
        {
            await SaveBreakAsync(connection, transaction, report.TradeRef, quantityBreak, cancellationToken);
        }

        foreach (var priceBreak in result.PriceBreaks)
        {
            await SavePriceBreakAsync(connection, transaction, report.TradeRef, priceBreak, cancellationToken);
        }
    }

    private static async Task SaveBreakAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string tradeRef,
        QuantityBreak quantityBreak,
        CancellationToken cancellationToken)
    {
        const string upsertBreak = """
            INSERT INTO quantity_breaks (break_id, trade_ref, internal_quantity, broker_quantity, status, resolved_at)
            VALUES ($1, $2, $3, $4, $5, CASE WHEN $5 = 'AutoResolved' THEN CURRENT_TIMESTAMP END)
            ON CONFLICT (break_id) DO UPDATE
            SET status = EXCLUDED.status, resolved_at = EXCLUDED.resolved_at
            WHERE quantity_breaks.status = 'Open' AND EXCLUDED.status = 'AutoResolved'
            """;

        await using var command = new NpgsqlCommand(upsertBreak, connection, transaction)
        {
            Parameters =
            {
                new() { Value = quantityBreak.Id },
                new() { Value = tradeRef },
                new() { Value = quantityBreak.InternalQuantity },
                new() { Value = quantityBreak.BrokerQuantity },
                new() { Value = quantityBreak.Status.ToString() }
            }
        };
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task SavePriceBreakAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string tradeRef,
        PriceBreak priceBreak,
        CancellationToken cancellationToken)
    {
        const string upsertBreak = """
            INSERT INTO price_breaks (break_id, trade_ref, internal_price, broker_price, status, resolved_at)
            VALUES ($1, $2, $3, $4, $5, CASE WHEN $5 = 'AutoResolved' THEN CURRENT_TIMESTAMP END)
            ON CONFLICT (break_id) DO UPDATE
            SET status = EXCLUDED.status, resolved_at = EXCLUDED.resolved_at
            WHERE price_breaks.status = 'Open' AND EXCLUDED.status = 'AutoResolved'
            """;

        await using var command = new NpgsqlCommand(upsertBreak, connection, transaction)
        {
            Parameters =
            {
                new() { Value = priceBreak.Id },
                new() { Value = tradeRef },
                new() { Value = priceBreak.InternalPrice },
                new() { Value = priceBreak.BrokerPrice },
                new() { Value = priceBreak.Status.ToString() }
            }
        };
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
