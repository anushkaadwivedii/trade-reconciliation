using System;
using Xunit;

namespace TradeReconciliation.Tests;

public sealed class PriceReconciliationTests
{
    [Fact]
    public void MatchingQuantitiesCanHaveAPriceBreak()
    {
        var reconciler = new TradeReconciler();
        var internalReport = TradeReportNormalizer.Normalize(
            TradeSource.Internal, """{"tradeRef":"T1","version":1,"quantity":100,"price":10.01000001}""");
        var brokerReport = TradeReportNormalizer.Normalize(
            TradeSource.BrokerA, """{"execution_id":"T1","revision":1,"qty":100,"px":10.01000002}""");
        reconciler.Apply(internalReport);

        var mismatch = reconciler.Apply(brokerReport);
        Assert.Equal(ReconciliationStatus.Mismatched, mismatch.Status);
        Assert.Empty(mismatch.Breaks);
        var opened = Assert.Single(mismatch.PriceBreaks);
        Assert.Equal(BreakStatus.Open, opened.Status);
        Assert.Equal(10.01000001m, opened.InternalPrice);
        Assert.Equal(10.01000002m, opened.BrokerPrice);

        var missing = reconciler.Apply(brokerReport with { Version = 2, Price = null });
        Assert.Equal(ReconciliationStatus.Mismatched, missing.Status);
        Assert.Equal(opened, Assert.Single(missing.PriceBreaks));

        var corrected = reconciler.Apply(brokerReport with { Version = 3, Price = internalReport.Price });
        Assert.Equal(ReconciliationStatus.Matched, corrected.Status);
        Assert.Equal(opened with { Status = BreakStatus.AutoResolved }, Assert.Single(corrected.PriceBreaks));
    }

    [Fact]
    public void PriceAndQuantityBreaksResolveIndependently()
    {
        var reconciler = new TradeReconciler();
        var broker = new TradeReport("T1", TradeSource.BrokerA, 1, 90, 10.02m);
        reconciler.Apply(new TradeReport("T1", TradeSource.Internal, 1, 100, 10.01m));

        var mismatch = reconciler.Apply(broker);
        var quantity = Assert.Single(mismatch.Breaks);
        var price = Assert.Single(mismatch.PriceBreaks);
        Assert.NotEqual(quantity.Id, price.Id);

        var quantityFixed = reconciler.Apply(broker with { Version = 2, Quantity = 100 });
        Assert.Equal(ReconciliationStatus.Mismatched, quantityFixed.Status);
        Assert.Equal(quantity with { Status = BreakStatus.AutoResolved }, Assert.Single(quantityFixed.Breaks));
        Assert.Equal(price, Assert.Single(quantityFixed.PriceBreaks));
    }

    [Theory]
    [InlineData("10.000000001")]
    [InlineData("10000000000")]
    [InlineData("0")]
    [InlineData("-10")]
    [InlineData("null")]
    [InlineData("1e1")]
    [InlineData("\"10\"")]
    public void RejectsPricesOutsideTheFeedContract(string rawPrice)
    {
        var payload = $$"""{"tradeRef":"T1","version":1,"quantity":100,"price":{{rawPrice}}}""";
        var error = Assert.Throws<FormatException>(
            () => TradeReportNormalizer.Normalize(TradeSource.Internal, payload));
        Assert.Contains("'price'", error.Message);
    }
}
