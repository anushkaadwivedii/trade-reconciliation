using Npgsql;

namespace TradeReconciliation.KafkaDemo;

internal sealed class PostgresTradeStore(NpgsqlDataSource database)
{
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var schema = typeof(PostgresTradeStore).Assembly
            .GetManifestResourceStream("TradeHistory.sql")
            ?? throw new InvalidOperationException("The trade history schema is missing.");
        using var reader = new StreamReader(schema);
        var sql = await reader.ReadToEndAsync(cancellationToken);

        await using var connection = await database.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // Serialize setup if two demo processes start against an empty database.
        await using var schemaLock = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(829316054)", connection, transaction);
        await schemaLock.ExecuteNonQueryAsync(cancellationToken);

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task SaveAsync(
        TradeReport report,
        ReconciliationResult result,
        CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        const string insertReport = """
            INSERT INTO trade_reports (trade_ref, source, version, quantity, status_after_report)
            VALUES ($1, $2, $3, $4, $5)
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
                new() { Value = result.Status.ToString() }
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

        await transaction.CommitAsync(cancellationToken);
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
}
