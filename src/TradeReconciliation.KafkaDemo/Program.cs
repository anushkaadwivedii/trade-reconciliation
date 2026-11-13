using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;
using TradeReconciliation.KafkaDemo;
using TradeReconciliation;

var mode = args switch
{
    [] => "demo",
    ["--worker"] => "worker",
    ["--publish-only"] => "publish",
    _ => null
};
if (mode is null)
{
    Console.Error.WriteLine("Usage: TradeReconciliation.KafkaDemo [--worker | --publish-only]");
    return 2;
}

var topic = mode == "demo" ? "trade-reports.demo" : "trade-reports.live";
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
using var timeout = new CancellationTokenSource();
if (mode != "worker")
{
    timeout.CancelAfter(TimeSpan.FromSeconds(30));
}
Console.CancelKeyPress += (_, args) =>
{
    args.Cancel = true;
    timeout.Cancel();
};

try
{
    var connectionString = Environment.GetEnvironmentVariable("TRADE_DB_CONNECTION");
    if (mode == "worker" && string.IsNullOrWhiteSpace(connectionString))
    {
        Console.Error.WriteLine("Set TRADE_DB_CONNECTION to run the worker.");
        return 2;
    }

    await using var database = mode == "publish" || string.IsNullOrWhiteSpace(connectionString)
        ? null
        : NpgsqlDataSource.Create(connectionString);
    var store = database is null ? null : new PostgresTradeStore(database);
    if (store is not null)
    {
        await store.InitializeAsync(timeout.Token);
    }

    await EnsureTopicAsync(bootstrapServers, topic, timeout.Token);

    if (mode == "worker")
    {
        var groupId = Environment.GetEnvironmentVariable("KAFKA_CONSUMER_GROUP") ?? "trade-reconciliation-worker";
        await EnsureTopicAsync(bootstrapServers, KafkaTradeWorker.DeadLetterTopic, timeout.Token);
        await KafkaTradeWorker.RunAsync(bootstrapServers, topic, groupId, store!, timeout.Token);
        return 0;
    }

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

    if (mode == "publish")
    {
        Console.WriteLine($"Published three reports for {tradeRef}.");
        return 0;
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

        var report = KafkaTradeWorker.ReadReport(message);
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
catch (OperationCanceledException) when (mode == "worker" && timeout.IsCancellationRequested)
{
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
