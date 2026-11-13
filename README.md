# Trade Reconciliation

Trade Reconciliation is a C# portfolio project that compares a firm's trade reports with a broker's reports and tracks differences through corrections.

The current checkpoint is an in-memory reconciler for one internal source and one broker. Reports for the same trade are matched by trade reference. The reconciler keeps the highest version received from each source and compares quantities when both sides have reported. It keeps each quantity break in history when a later report resolves it.

## Run the tests

Install the .NET 10 SDK, then run this from the repository root:

```sh
dotnet test tests/TradeReconciliation.Tests/TradeReconciliation.Tests.csproj
```

Start with `BrokerCorrectionResolvesTheOriginalQuantityBreak` in the test project. It walks through an internal report of 100 shares, a broker report of 90, and a broker correction to 100.

## Feed formats

The normalizer turns both JSON formats into the same `TradeReport` record. The caller identifies the source. The field names below define the two simulated feed formats:

| Feed | Example |
| --- | --- |
| Internal | `{"tradeRef":"TRADE-001","version":1,"quantity":100}` |
| Broker A | `{"execution_id":"TRADE-001","revision":1,"qty":90}` |

Each report needs a nonblank trade reference, a positive 32-bit version, and a positive whole-share quantity that fits in a signed 64-bit integer. Extra fields are allowed. Malformed reports, duplicate JSON properties, and missing or invalid required fields raise a `FormatException` with a reason.

## Run the Kafka demo

With Docker and the Compose plugin available, run these commands from the repository root:

```sh
docker compose up -d --wait kafka
dotnet run --project src/TradeReconciliation.KafkaDemo
docker compose stop kafka
```

The demo can also use an existing Kafka broker. Set `KAFKA_BOOTSTRAP_SERVERS` if its address differs from `localhost:9092`. The broker must allow topic creation, or already have the `trade-reports.demo` topic.

The producer puts all three raw reports on one demo topic, keyed by trade reference. A message header identifies the feed format. Kafka preserves the order of the reports within the partition. The consumer starts at the first offset published by this run, skips other runs' trade keys, and passes each report through the existing normalizer and reconciler. The output shows `WaitingForBroker`, `Mismatched`, and `Matched`, with the same break ID printed first as `Open` and then as `AutoResolved`.

The demo reads its own three reports. Each run starts with fresh reconciliation state and a new trade reference. Consumer offset commits and restart recovery are planned.

## Save and query the demo in PostgreSQL

Start Kafka and PostgreSQL together, then run the demo with the local Compose connection string:

```sh
docker compose up -d --wait
TRADE_DB_CONNECTION='Host=localhost;Port=5432;Database=trades;Username=trade_demo;Password=trade_demo_local' \
  dotnet run --project src/TradeReconciliation.KafkaDemo
```

The Compose credentials are only for this local demo. For an existing database, provide its connection string through `TRADE_DB_CONNECTION`. The demo needs permission to create its tables. It applies `db/migrations/001_trade_history.sql` on startup and uses parameterized SQL through Npgsql's ADO.NET API to save each report and its break changes in one transaction. The report log keeps each version, and a resolved break keeps the original 100-versus-90 quantities and its opening and resolution times.

After the demo exits, open a SQL prompt and replace `DEMO-...` with the reference it printed:

```sh
docker compose exec postgres psql -U trade_demo -d trades
```

```sql
SELECT source, version, quantity, status_after_report
FROM trade_reports
WHERE trade_ref = 'DEMO-...'
ORDER BY report_id;

SELECT break_id, internal_quantity, broker_quantity, status, opened_at, resolved_at
FROM quantity_breaks
WHERE trade_ref = 'DEMO-...';
```

Run `docker compose down` to stop both services. The named PostgreSQL volume keeps the records for the next startup.

## Project status

The local demo sends two JSON feed formats through Kafka, reconciles quantities, and can store reports and breaks in PostgreSQL. Persistent consumer recovery, additional trade rules, and monitoring are planned.
