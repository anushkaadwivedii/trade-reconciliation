using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace TradeReconciliation.KafkaDemo;

internal static class KafkaTradeWorker
{
    internal const string DeadLetterTopic = "trade-reports.deadletter";

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

        using var rejectedReports = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            EnableIdempotence = true,
            MessageTimeoutMs = 10_000
        }).Build();

        consumer.Subscribe(topic);
        Console.WriteLine($"Listening on {topic} as {groupId}. Press Ctrl+C to stop.");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var record = consumer.Consume(cancellationToken);
                string outcome;
                try
                {
                    var report = ReadReport(record.Message);
                    var result = await store.ApplyAndSaveAsync(report, cancellationToken);
                    var status = result?.Status.ToString() ?? "Already saved";
                    outcome = $"{report.TradeRef} {report.Source} v{report.Version}: {status}";
                }
                catch (FormatException exception)
                {
                    var rejectionId = await PublishRejectionAsync(
                        rejectedReports, record, exception.Message, cancellationToken);
                    outcome = $"Rejected {rejectionId}: {exception.Message}";
                }

                // Advance after PostgreSQL succeeds or Kafka acknowledges the rejection.
                consumer.Commit(record);
                Console.WriteLine(outcome);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private static async Task<string> PublishRejectionAsync(
        IProducer<string, string> producer,
        ConsumeResult<string, string> record,
        string reason,
        CancellationToken cancellationToken)
    {
        var rejectionId = FormattableString.Invariant(
            $"{record.Topic}:{record.Partition.Value}:{record.Offset.Value}");

        var headers = record.Message.Headers?.Select(header => new
        {
            name = header.Key,
            value = header.GetValueBytes()
        });

        var rejection = JsonSerializer.Serialize(new
        {
            topic = record.Topic,
            partition = record.Partition.Value,
            offset = record.Offset.Value,
            key = record.Message.Key,
            payload = record.Message.Value,
            headers,
            reason
        });

        await producer.ProduceAsync(DeadLetterTopic, new Message<string, string>
        {
            Key = rejectionId,
            Value = rejection
        }, cancellationToken);

        return rejectionId;
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
