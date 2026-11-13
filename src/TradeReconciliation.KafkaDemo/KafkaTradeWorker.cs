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
        TimeSpan gracePeriod,
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
                    var message = record.Message;
                    if (ReadKind(message.Headers) == "watermark")
                    {
                        var source = ReadSource(message.Headers);
                        var watermark = ReadWatermark(message, source);
                        var opened = await store.SaveWatermarkAsync(source, watermark, gracePeriod, cancellationToken);
                        outcome = $"Watermark {source} {watermark:O}: opened {opened} missing-report breaks";
                    }
                    else
                    {
                        var report = ReadReport(message);
                        var result = await store.ApplyAndSaveAsync(report, gracePeriod, cancellationToken);
                        var status = result?.Status.ToString() ?? "Already saved";
                        outcome = $"{report.TradeRef} {report.Source} v{report.Version}: {status}";
                    }
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

    private static string ReadKind(Headers? headers)
    {
        if (headers is null || !headers.TryGetLastBytes("kind", out var value))
        {
            return "report";
        }

        return value is null ? throw new FormatException("Kafka message kind is null.")
            : Encoding.UTF8.GetString(value) switch
            {
                "report" => "report",
                "watermark" => "watermark",
                _ => throw new FormatException("Kafka message has an unknown kind header.")
            };
    }

    private static DateTimeOffset ReadWatermark(Message<string, string> message, TradeSource source)
    {
        if (message.Key != $"watermark:{source}" || message.Value is null)
        {
            throw new FormatException("Watermark requires a payload and a key of watermark:<source>.");
        }

        try
        {
            using var document = JsonDocument.Parse(message.Value, new JsonDocumentOptions
            {
                AllowDuplicateProperties = false
            });
            var root = document.RootElement;
            if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("eventTimeUtc", out var value))
            {
                return TradeReportNormalizer.ParseUtcTimestamp(value, "eventTimeUtc");
            }
        }
        catch (JsonException exception)
        {
            throw new FormatException("Watermark must be valid JSON with unique property names.", exception);
        }

        throw new FormatException("Watermark must contain eventTimeUtc.");
    }

    private static TradeSource ReadSource(Headers? headers)
    {
        if (headers is null
            || !headers.TryGetLastBytes("source", out var value)
            || value is null)
        {
            throw new FormatException("Kafka report is missing its source header.");
        }

        return Encoding.UTF8.GetString(value) switch
        {
            nameof(TradeSource.Internal) => TradeSource.Internal,
            nameof(TradeSource.BrokerA) => TradeSource.BrokerA,
            _ => throw new FormatException("Kafka report has an unknown source header.")
        };
    }

    internal static TradeReport ReadReport(Message<string, string> message)
    {
        var source = ReadSource(message.Headers);
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
