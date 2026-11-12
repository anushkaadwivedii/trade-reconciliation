# Trade Reconciliation

Trade Reconciliation is a C# portfolio project that compares a firm's trade reports with a broker's reports and tracks differences through corrections.

The current checkpoint is an in-memory reconciler for one internal source and one broker. Reports for the same trade are matched by trade reference. The reconciler keeps the highest version received from each source and compares quantities when both sides have reported. It keeps each quantity break in history when a later report resolves it.

## Run the tests

Install the .NET 10 SDK, then run this from the repository root:

```sh
dotnet test tests/TradeReconciliation.Tests/TradeReconciliation.Tests.csproj
```

Start with `BrokerCorrectionResolvesTheOriginalQuantityBreak` in the test project. It walks through an internal report of 100 shares, a broker report of 90, and a broker correction to 100.

## Project status

This checkpoint covers quantity reconciliation with reports supplied directly to the reconciler. Source adapters, Kafka messaging, PostgreSQL storage, and monitoring are planned.
