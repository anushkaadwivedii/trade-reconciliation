using System;
using Xunit;

namespace TradeReconciliation.Tests;

public sealed class MissingReportTrackerTests
{
    private static readonly DateTimeOffset ExecutionTime = new(2026, 1, 5, 14, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan GracePeriod = TimeSpan.FromSeconds(30);

    [Theory]
    [InlineData(TradeSource.Internal, TradeSource.BrokerA)]
    [InlineData(TradeSource.BrokerA, TradeSource.Internal)]
    public void OnlyTheOtherFeedCanOpenABreakAfterTheGracePeriod(
        TradeSource firstSource, TradeSource missingSource)
    {
        var tracker = new MissingReportTracker(GracePeriod);
        Assert.Null(tracker.Observe("T1", firstSource, ExecutionTime));
        Assert.Empty(tracker.AdvanceWatermark(firstSource, ExecutionTime.AddMinutes(1)));
        Assert.Empty(tracker.AdvanceWatermark(missingSource, ExecutionTime + GracePeriod));

        var afterGrace = (ExecutionTime + GracePeriod).AddTicks(1);
        var opened = Assert.Single(tracker.AdvanceWatermark(missingSource, afterGrace));
        Assert.Equal(new MissingReportBreak("T1", missingSource, BreakStatus.Open), opened);
        Assert.Empty(tracker.AdvanceWatermark(missingSource, afterGrace.AddSeconds(1)));

        var resolved = tracker.Observe("T1", missingSource, ExecutionTime);
        Assert.Equal(opened with { Status = BreakStatus.AutoResolved }, resolved);
        Assert.Null(tracker.Observe("T1", missingSource, ExecutionTime));
        Assert.Empty(tracker.AdvanceWatermark(missingSource, afterGrace.AddSeconds(2)));
    }

    [Fact]
    public void BothReportsArrivingBeforeDeadlineNeverOpenABreak()
    {
        var tracker = new MissingReportTracker(GracePeriod);
        tracker.Observe("T1", TradeSource.Internal, ExecutionTime);
        tracker.AdvanceWatermark(TradeSource.BrokerA, ExecutionTime.AddSeconds(29));

        Assert.Null(tracker.Observe("T1", TradeSource.BrokerA, ExecutionTime));
        Assert.Empty(tracker.AdvanceWatermark(TradeSource.BrokerA, ExecutionTime.AddMinutes(1)));
    }

    [Fact]
    public void ATradeArrivingBehindAnExistingWatermarkCanOpenABreakImmediately()
    {
        var tracker = new MissingReportTracker(GracePeriod);
        var watermark = ExecutionTime.AddMinutes(1);
        Assert.Empty(tracker.AdvanceWatermark(TradeSource.BrokerA, watermark));
        Assert.Empty(tracker.AdvanceWatermark(TradeSource.BrokerA, ExecutionTime));

        Assert.Equal(new MissingReportBreak("T1", TradeSource.BrokerA, BreakStatus.Open),
            tracker.Observe("T1", TradeSource.Internal, ExecutionTime));
        Assert.Null(tracker.Observe("T2", TradeSource.Internal, watermark));
    }

    [Fact]
    public void OneWatermarkCanOpenBreaksForSeveralTradesInReferenceOrder()
    {
        var tracker = new MissingReportTracker(GracePeriod);
        tracker.Observe("T2", TradeSource.Internal, ExecutionTime);
        tracker.Observe("T1", TradeSource.Internal, ExecutionTime);
        tracker.Observe("T3", TradeSource.Internal, ExecutionTime.AddSeconds(45));

        var opened = tracker.AdvanceWatermark(TradeSource.BrokerA, ExecutionTime.AddMinutes(1));

        Assert.Collection(opened,
            first => Assert.Equal("T1", first.TradeRef),
            second => Assert.Equal("T2", second.TradeRef));
    }
}
