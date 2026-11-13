using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;
using TradeReconciliation.KafkaDemo;
using TradeReconciliation;

const string topic = "trade-reports.demo";
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
Console.CancelKeyPress += (_, args) =>
{
    args.Cancel = true;
    timeout.Cancel();
};

try
{
    var connectionString = Environment.GetEnvironmentVariable("TRADE_DB_CONNECTION");
    await using var database = string.IsNullOrWhiteSpace(connectionString)
        ? null
        : NpgsqlDataSource.Create(connectionString);
    var store = database is null ? null : new PostgresTradeStore(database);
    if (store is not null)
    {
        await store.InitializeAsync(timeout.Token);
    }

    await EnsureTopicAsync(bootstrapServers, topic, timeout.Token);

    var tradeRef = $"DEMO-{Guid.NewGuid():N}";
    var reports = new (TradeSource Source, string Payload)[]
    {
        (TradeSource.Internal, $$"""{"tradeRef":"{{tradeRef}}","version":1,"quantity":100}"""),
        (TradeSource.BrokerA, $$"""{"execution_id":"{{tradeRef}}","revision":1,"qty":90}"""),
        (TradeSource.BrokerA, $$"""{"execution_id":"{{tradeRef}}","revision":2,"qty":100}""")
    };

    using var producer = new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        EnableIdempotence = true,
        MessageTimeoutMs = 10_000
    }).Build();

    TopicPartitionOffset? firstOffset = null;
    foreach (var (source, payload) in reports)
    {
        var delivery = await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = tradeRef,
            Value = payload,
            Headers = new Headers { { "source", Encoding.UTF8.GetBytes(source.ToString()) } }
        }, timeout.Token);
        firstOffset ??= delivery.TopicPartitionOffset;
    }

    using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = bootstrapServers,
        GroupId = "trade-reconciliation-demo",
        EnableAutoCommit = false
    }).Build();

    // Start at this run's first record. The reconciler is rebuilt for each demo.
    consumer.Assign(firstOffset ?? throw new InvalidOperationException("No reports were published."));
    var reconciler = new TradeReconciler();
    var processed = 0;

    while (processed < reports.Length)
    {
        var message = consumer.Consume(timeout.Token).Message;
        if (message.Key != tradeRef)
        {
            continue;
        }

        var source = ReadSource(message.Headers);
        var report = TradeReportNormalizer.Normalize(source, message.Value);
        if (report.TradeRef != message.Key)
        {
            throw new FormatException("Kafka key does not match the report's trade reference.");
        }

        var result = reconciler.Apply(report);
        if (store is not null)
        {
            await store.SaveAsync(report, result, timeout.Token);
        }

        Console.WriteLine($"{report.Source} v{report.Version}: {report.Quantity} shares -> {result.Status}");
        foreach (var quantityBreak in result.Breaks)
        {
            Console.WriteLine(
                $"  Break {quantityBreak.Id}: {quantityBreak.InternalQuantity} vs {quantityBreak.BrokerQuantity} ({quantityBreak.Status})");
        }

        processed++;
    }

    consumer.Close();
    if (store is not null)
    {
        Console.WriteLine($"Saved reports and breaks for {tradeRef}.");
    }

    return 0;
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("Demo stopped or timed out. Check that Kafka and any configured database are running.");
    return 1;
}
catch (KafkaException exception)
{
    Console.Error.WriteLine($"Kafka demo failed: {exception.Error.Reason}");
    return 1;
}
catch (NpgsqlException exception)
{
    Console.Error.WriteLine($"PostgreSQL write failed: {exception.Message}");
    return 1;
}
catch (FormatException exception)
{
    Console.Error.WriteLine($"Kafka report rejected: {exception.Message}");
    return 1;
}

static TradeSource ReadSource(Headers? headers)
{
    if (headers is null || !headers.TryGetLastBytes("source", out var value) || value is null)
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

static async Task EnsureTopicAsync(string bootstrapServers, string topic, CancellationToken cancellationToken)
{
    using var admin = new AdminClientBuilder(new AdminClientConfig
    {
        BootstrapServers = bootstrapServers
    }).Build();

    try
    {
        // The request timeout bounds the call before the admin client is disposed.
        await admin.CreateTopicsAsync(
            [new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
            new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) });
    }
    catch (CreateTopicsException exception)
        when (exception.Results.All(result => result.Error.Code == ErrorCode.TopicAlreadyExists))
    {
        // Reuse the demo topic on subsequent runs.
    }

    cancellationToken.ThrowIfCancellationRequested();
}
