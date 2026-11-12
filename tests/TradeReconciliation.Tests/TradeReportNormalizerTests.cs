using System;
using Xunit;

namespace TradeReconciliation.Tests;

public sealed class TradeReportNormalizerTests
{
    [Theory]
    [InlineData(TradeSource.Internal, """{"tradeRef":"TRADE-001","version":2,"quantity":3000000000}""")]
    [InlineData(TradeSource.BrokerA, """{"execution_id":"TRADE-001","revision":2,"qty":3000000000,"desk":"A"}""")]
    public void NormalizesBothFeedFormats(TradeSource source, string payload)
    {
        var report = TradeReportNormalizer.Normalize(source, payload);

        Assert.Equal(new TradeReport("TRADE-001", source, 2, 3_000_000_000L), report);
    }

    [Theory]
    [InlineData("{", "valid JSON")]
    [InlineData("[]", "JSON object")]
    [InlineData("""{"version":1,"quantity":100}""", "tradeRef")]
    [InlineData("""{"tradeRef":" ","version":1,"quantity":100}""", "tradeRef")]
    [InlineData("""{"tradeRef":"TRADE-001","version":0,"quantity":100}""", "version")]
    [InlineData("""{"tradeRef":"TRADE-001","version":2147483648,"quantity":100}""", "version")]
    [InlineData("""{"tradeRef":"TRADE-001","version":1,"quantity":"100"}""", "quantity")]
    [InlineData("""{"tradeRef":"TRADE-001","version":1,"quantity":90.5}""", "quantity")]
    [InlineData("""{"tradeRef":"TRADE-001","version":1,"quantity":0}""", "quantity")]
    [InlineData("""{"tradeRef":"TRADE-001","version":1,"quantity":100,"quantity":90}""", "unique property names")]
    public void RejectsInvalidReportsWithAReason(string payload, string reason)
    {
        var exception = Assert.Throws<FormatException>(
            () => TradeReportNormalizer.Normalize(TradeSource.Internal, payload));

        Assert.Contains(reason, exception.Message);
    }

    [Fact]
    public void BrokerReportMustUseTheBrokerSchema()
    {
        const string internalPayload = """{"tradeRef":"TRADE-001","version":1,"quantity":100}""";

        var exception = Assert.Throws<FormatException>(
            () => TradeReportNormalizer.Normalize(TradeSource.BrokerA, internalPayload));

        Assert.Contains("execution_id", exception.Message);
    }

    [Fact]
    public void ReconcilesAnInternalReportAndBrokerCorrectionFromJson()
    {
        const string internalReport = """{"tradeRef":"TRADE-001","version":1,"quantity":100}""";
        const string brokerReport = """{"execution_id":"TRADE-001","revision":1,"qty":90}""";
        const string brokerCorrection = """{"execution_id":"TRADE-001","revision":2,"qty":100}""";
        var reconciler = new TradeReconciler();

        var waiting = reconciler.Apply(TradeReportNormalizer.Normalize(TradeSource.Internal, internalReport));
        var mismatched = reconciler.Apply(TradeReportNormalizer.Normalize(TradeSource.BrokerA, brokerReport));
        var matched = reconciler.Apply(TradeReportNormalizer.Normalize(TradeSource.BrokerA, brokerCorrection));

        Assert.Equal(ReconciliationStatus.WaitingForBroker, waiting.Status);
        Assert.Equal(ReconciliationStatus.Mismatched, mismatched.Status);
        var originalBreak = Assert.Single(mismatched.Breaks);
        Assert.Equal(90L, originalBreak.BrokerQuantity);
        Assert.Equal(ReconciliationStatus.Matched, matched.Status);
        Assert.Equal(originalBreak with { Status = BreakStatus.AutoResolved }, Assert.Single(matched.Breaks));
    }
}
