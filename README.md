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

## Project status

The local tests cover both JSON formats, validation, and a complete report-to-correction example. Kafka messaging, PostgreSQL storage, and monitoring are planned.
