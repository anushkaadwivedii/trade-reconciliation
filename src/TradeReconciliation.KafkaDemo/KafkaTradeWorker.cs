using System.Text;
using Confluent.Kafka;

namespace TradeReconciliation.KafkaDemo;

internal static class KafkaTradeWorker
{
    public static async Task RunAsync(
        string bootstrapServers,
        string topic,
        string groupId,
        PostgresTradeStore store,
        CancellationToken cancellationToken)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();

        consumer.Subscribe(topic);
        Console.WriteLine($"Listening on {topic} as {groupId}. Press Ctrl+C to stop.");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var record = consumer.Consume(cancellationToken);
                var report = ReadReport(record.Message);
                var result = await store.ApplyAndSaveAsync(report, cancellationToken);

                // Advance only after PostgreSQL commits or confirms an identical report.
                consumer.Commit(record);
                var status = result?.Status.ToString() ?? "Already saved";
                Console.WriteLine($"{report.TradeRef} {report.Source} v{report.Version}: {status}");
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    internal static TradeReport ReadReport(Message<string, string> message)
    {
        if (message.Headers is null
            || !message.Headers.TryGetLastBytes("source", out var value)
            || value is null)
        {
            throw new FormatException("Kafka report is missing its source header.");
        }

        var source = Encoding.UTF8.GetString(value) switch
        {
            nameof(TradeSource.Internal) => TradeSource.Internal,
            nameof(TradeSource.BrokerA) => TradeSource.BrokerA,
            _ => throw new FormatException("Kafka report has an unknown source header.")
        };

        if (message.Value is null)
        {
            throw new FormatException("Kafka report is missing its payload.");
        }

        var report = TradeReportNormalizer.Normalize(source, message.Value);
        if (report.TradeRef != message.Key)
        {
            throw new FormatException("Kafka key does not match the report's trade reference.");
        }

        return report;
    }
}
