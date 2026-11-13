# Trade Reconciliation

Trade Reconciliation is a C# portfolio project that compares a firm's trade reports with a broker's reports and tracks differences through corrections.

The current checkpoint is an in-memory reconciler for one internal source and one broker. Reports for the same trade are matched by trade reference. The reconciler keeps the highest version received from each source and compares quantities and available prices when both sides have reported. It keeps each quantity break in history when a later report resolves it.

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
| Internal | `{"tradeRef":"TRADE-001","version":1,"quantity":100,"price":10.01}` |
| Broker A | `{"execution_id":"TRADE-001","revision":1,"qty":90,"px":10.02}` |

Each report needs a nonblank trade reference, a positive 32-bit version, and a positive whole-share quantity that fits in a signed 64-bit integer. Prices are optional for compatibility with earlier reports. If supplied, a price must be a positive JSON number in plain decimal notation with up to 10 whole digits and 8 fractional digits. C# compares prices exactly using `decimal`, assuming both feeds use the same currency. When either price is missing, the reconciler skips price comparison and keeps any existing price break open. Extra fields are allowed. Malformed reports, duplicate JSON properties, and missing or invalid required fields raise a `FormatException` with a reason.

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

The Compose credentials are only for this local demo. For an existing database, provide its connection string through `TRADE_DB_CONNECTION`. The demo needs permission to create its tables. It applies the SQL files in `db/migrations/` on startup and uses parameterized SQL through Npgsql's ADO.NET API to save each report and its break changes in one transaction. The report log keeps each version, and a resolved break keeps the original 100-versus-90 quantities and its opening and resolution times.

After the demo exits, open a SQL prompt and replace `DEMO-...` with the reference it printed:

```sh
docker compose exec postgres psql -U trade_demo -d trades
```

```sql
SELECT source, version, quantity, price, status_after_report
FROM trade_reports
WHERE trade_ref = 'DEMO-...'
ORDER BY report_id;

SELECT break_id, internal_quantity, broker_quantity, status, opened_at, resolved_at
FROM quantity_breaks
WHERE trade_ref = 'DEMO-...';

SELECT break_id, internal_price, broker_price, status, opened_at, resolved_at
FROM price_breaks
WHERE trade_ref = 'DEMO-...';
```

Run `docker compose down` to stop both services. The named PostgreSQL volume keeps the records for the next startup.

## Run the continuous worker

With Kafka and PostgreSQL running, start the worker in one terminal:

```sh
TRADE_DB_CONNECTION='Host=localhost;Port=5432;Database=trades;Username=trade_demo;Password=trade_demo_local' \
  dotnet run --project src/TradeReconciliation.KafkaDemo -- --worker
```

Publish the three sample reports from a second terminal:

```sh
dotnet run --project src/TradeReconciliation.KafkaDemo -- --publish-only
```

The worker listens on `trade-reports.live`, separate from the one-shot demo topic. It uses the `trade-reconciliation-worker` consumer group by default. Keep the group and database the same when restarting so the worker resumes from the saved Kafka offset. Set `KAFKA_CONSUMER_GROUP` for a separate group.

For each report, the worker rebuilds that trade's state from its PostgreSQL history in arrival order, applies the new report, and saves it with its break changes in one transaction. Then it commits the Kafka offset. If the same source and version arrive again with the same quantity and price, the worker advances the offset without adding another row or changing the break. This is at-least-once delivery with idempotent database writes.

Replaying one trade's history for each new report keeps the recovery logic small for this checkpoint. A more efficient current-state projection is future work.

### Inspect rejected reports

Invalid reports go to `trade-reports.deadletter`. Each entry includes the original Kafka topic, partition, offset, key, JSON text, headers (with binary values encoded as Base64), and the reason for rejection. The worker commits the input offset only after Kafka acknowledges the rejected entry. Kafka or PostgreSQL failures stop processing with the current offset uncommitted.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic trade-reports.deadletter --from-beginning \
  --property print.key=true
```

A crash after Kafka saves a rejection but before the input offset is committed can publish the same rejection again. The dead-letter key (`topic:partition:offset`) identifies the original Kafka record so consumers can recognize repeated entries.

## Missing-report grace period

The domain library includes a `MissingReportTracker` for the two feeds. It takes a configurable grace period, each trade's execution time, and explicit event-time watermarks. A watermark is a feed's statement about how far it has progressed. A trade that only one side has reported becomes a missing-report break when the **other** feed's watermark moves strictly past the execution time plus the grace period.

For example, with a 30-second grace period and an internal trade at 14:00:00, a broker watermark of 14:00:30 leaves the trade waiting. A broker watermark of 14:00:31 opens a break. A later broker report for the same trade resolves it. A break is identified by its trade reference and missing source. Repeated reports and older watermarks do not emit duplicate changes.

The continuous worker accepts optional execution timestamps in `execTimeUtc` for internal reports and `executed_at` for Broker A. Timestamps use UTC with a trailing `Z` and at most six fractional digits. Older reports can omit the timestamp. A report without a timestamp can resolve an existing break, but a trade needs at least one timestamped report before the worker can open a missing-report break.

Set `MISSING_REPORT_GRACE_SECONDS` before starting the worker to change the default of 30 seconds. Keep this setting consistent across worker instances and restarts for the same database. Watermarks are explicit feed messages rather than inferred from the latest report. They share the single-partition `trade-reports.live` topic with the reports so the demo applies them in stream order.

To try it on a fresh demo database and topic, start the worker, then publish these three records. Each line contains the Kafka headers, key, and JSON payload, separated by tabs:

```sh
printf '%s\t%s\t%s\n' \
  'source:Internal' 'WATERMARK-DEMO-1' '{"tradeRef":"WATERMARK-DEMO-1","version":1,"quantity":100,"execTimeUtc":"2026-01-05T14:00:00Z"}' \
  'source:BrokerA,kind:watermark' 'watermark:BrokerA' '{"eventTimeUtc":"2026-01-05T14:00:31Z"}' \
  'source:BrokerA' 'WATERMARK-DEMO-1' '{"execution_id":"WATERMARK-DEMO-1","revision":1,"qty":100,"executed_at":"2026-01-05T14:00:00Z"}' |
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic trade-reports.live \
  --property parse.headers=true --property parse.key=true
```

```sql
SELECT source, event_time FROM feed_watermarks;
SELECT trade_ref, missing_source, status, opened_at, resolved_at
FROM missing_report_breaks WHERE trade_ref = 'WATERMARK-DEMO-1';
```

PostgreSQL stores each feed's latest watermark and updates the missing-report break in the same transaction as the input that caused the change. The worker acknowledges the Kafka offset afterward. Missing-report breaks have their own history table. The report log's `status_after_report` still describes the quantity and price reconciliation result at arrival.

## Project status

The project normalizes and reconciles two JSON feeds, publishes reports through Kafka, and stores trade and mismatch history in PostgreSQL. The continuous worker resumes from Kafka's saved position and uses PostgreSQL to rebuild trade state after restart. Rejected reports are available in a separate Kafka topic.
