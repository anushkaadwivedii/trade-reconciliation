using Xunit;

namespace TradeReconciliation.Tests;

public sealed class TradeReconcilerTests
{
    [Fact]
    public void BrokerCorrectionResolvesTheOriginalQuantityBreak()
    {
        const string tradeRef = "TRADE-001";
        var reconciler = new TradeReconciler();

        var waiting = reconciler.Apply(
            new TradeReport(tradeRef, TradeSource.Internal, Version: 1, Quantity: 100));

        Assert.Equal(ReconciliationStatus.WaitingForBroker, waiting.Status);
        Assert.Empty(waiting.Breaks);

        var mismatched = reconciler.Apply(
            new TradeReport(tradeRef, TradeSource.BrokerA, Version: 1, Quantity: 90));

        Assert.Equal(ReconciliationStatus.Mismatched, mismatched.Status);
        var originalBreak = Assert.Single(mismatched.Breaks);
        Assert.Equal(BreakStatus.Open, originalBreak.Status);
        Assert.Equal(100L, originalBreak.InternalQuantity);
        Assert.Equal(90L, originalBreak.BrokerQuantity);

        var matched = reconciler.Apply(
            new TradeReport(tradeRef, TradeSource.BrokerA, Version: 2, Quantity: 100));

        Assert.Equal(ReconciliationStatus.Matched, matched.Status);
        var resolvedBreak = Assert.Single(matched.Breaks);
        Assert.Equal(originalBreak with { Status = BreakStatus.AutoResolved }, resolvedBreak);
    }

    [Fact]
    public void BrokerReportWaitsForTheInternalReportForTheSameTrade()
    {
        var reconciler = new TradeReconciler();
        reconciler.Apply(new TradeReport("TRADE-OTHER", TradeSource.Internal, 1, 100));

        var waiting = reconciler.Apply(new TradeReport("TRADE-001", TradeSource.BrokerA, 1, 100));

        Assert.Equal(ReconciliationStatus.WaitingForInternal, waiting.Status);
        Assert.Empty(waiting.Breaks);

        var matched = reconciler.Apply(new TradeReport("TRADE-001", TradeSource.Internal, 1, 100));

        Assert.Equal(ReconciliationStatus.Matched, matched.Status);
        Assert.Empty(matched.Breaks);
    }

    [Fact]
    public void ReplayedOlderReportsCannotUndoACorrection()
    {
        const string tradeRef = "TRADE-001";
        var reconciler = new TradeReconciler();
        var originalBrokerReport = new TradeReport(tradeRef, TradeSource.BrokerA, 1, 90);
        var correction = new TradeReport(tradeRef, TradeSource.BrokerA, 2, 100);
        reconciler.Apply(new TradeReport(tradeRef, TradeSource.Internal, 1, 100));
        var mismatch = reconciler.Apply(originalBrokerReport);

        var resolved = Assert.Single(reconciler.Apply(correction).Breaks);
        var afterReplay = reconciler.Apply(originalBrokerReport);
        var afterDuplicate = reconciler.Apply(correction);

        Assert.Equal(BreakStatus.Open, Assert.Single(mismatch.Breaks).Status);
        Assert.Equal(BreakStatus.AutoResolved, resolved.Status);
        Assert.Equal(ReconciliationStatus.Matched, afterReplay.Status);
        Assert.Equal(resolved, Assert.Single(afterReplay.Breaks));
        Assert.Equal(ReconciliationStatus.Matched, afterDuplicate.Status);
        Assert.Equal(resolved, Assert.Single(afterDuplicate.Breaks));
    }

    [Fact]
    public void ANewMismatchAfterResolutionCreatesADistinctBreak()
    {
        const string tradeRef = "TRADE-001";
        var reconciler = new TradeReconciler();
        reconciler.Apply(new TradeReport(tradeRef, TradeSource.Internal, 1, 100));
        var firstBreak = Assert.Single(
            reconciler.Apply(new TradeReport(tradeRef, TradeSource.BrokerA, 1, 90)).Breaks);

        var stillOpen = reconciler.Apply(new TradeReport(tradeRef, TradeSource.BrokerA, 2, 95));
        Assert.Equal(firstBreak, Assert.Single(stillOpen.Breaks));

        reconciler.Apply(new TradeReport(tradeRef, TradeSource.BrokerA, 3, 100));
        var reopened = reconciler.Apply(new TradeReport(tradeRef, TradeSource.BrokerA, 4, 95));

        Assert.Equal(ReconciliationStatus.Mismatched, reopened.Status);
        Assert.Collection(
            reopened.Breaks,
            resolved => Assert.Equal(firstBreak with { Status = BreakStatus.AutoResolved }, resolved),
            open =>
            {
                Assert.NotEqual(firstBreak.Id, open.Id);
                Assert.Equal(BreakStatus.Open, open.Status);
                Assert.Equal(95L, open.BrokerQuantity);
            });
    }
}
