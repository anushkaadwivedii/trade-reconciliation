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
}
