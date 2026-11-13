using System;
using System.Collections.Generic;

namespace TradeReconciliation;

public sealed record MissingReportBreak(
    string TradeRef,
    TradeSource MissingSource,
    BreakStatus Status);

public sealed class MissingReportTracker(TimeSpan gracePeriod)
{
    private readonly TimeSpan _gracePeriod = gracePeriod >= TimeSpan.Zero
        ? gracePeriod
        : throw new ArgumentOutOfRangeException(nameof(gracePeriod));

    private readonly SortedDictionary<string, TradeState> _trades = new(StringComparer.Ordinal);
    private readonly Dictionary<TradeSource, DateTimeOffset> _watermarks = new();

    // Returns a break only when observing a report changes its status.
    public MissingReportBreak? Observe(string tradeRef, TradeSource source, DateTimeOffset executionTime)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tradeRef);
        var otherSource = OtherSource(source);

        if (!_trades.TryGetValue(tradeRef, out var trade))
        {
            trade = new TradeState(source, executionTime);
            _trades.Add(tradeRef, trade);
            return TryOpen(tradeRef, trade, otherSource);
        }

        if (source == trade.FirstSource || trade.HasBothReports)
        {
            return null;
        }

        trade.HasBothReports = true;
        return trade.HasOpenBreak
            ? new MissingReportBreak(tradeRef, source, BreakStatus.AutoResolved)
            : null;
    }

    // A watermark marks how far one feed has progressed in event time.
    public IReadOnlyList<MissingReportBreak> AdvanceWatermark(TradeSource source, DateTimeOffset watermark)
    {
        OtherSource(source); // Validate before recording progress.
        if (_watermarks.TryGetValue(source, out var previous) && watermark <= previous)
        {
            return Array.Empty<MissingReportBreak>();
        }

        _watermarks[source] = watermark;
        var opened = new List<MissingReportBreak>();
        foreach (var (tradeRef, trade) in _trades)
        {
            var change = TryOpen(tradeRef, trade, source);
            if (change is not null)
            {
                opened.Add(change);
            }
        }

        return opened.AsReadOnly();
    }

    private MissingReportBreak? TryOpen(string tradeRef, TradeState trade, TradeSource missingSource)
    {
        if (trade.HasBothReports || trade.HasOpenBreak || trade.FirstSource == missingSource
            || !_watermarks.TryGetValue(missingSource, out var watermark)
            || watermark - trade.ExecutionTime <= _gracePeriod)
        {
            return null;
        }

        trade.HasOpenBreak = true;
        return new MissingReportBreak(tradeRef, missingSource, BreakStatus.Open);
    }

    private static TradeSource OtherSource(TradeSource source) => source switch
    {
        TradeSource.Internal => TradeSource.BrokerA,
        TradeSource.BrokerA => TradeSource.Internal,
        _ => throw new ArgumentOutOfRangeException(nameof(source))
    };

    private sealed class TradeState(TradeSource firstSource, DateTimeOffset executionTime)
    {
        public TradeSource FirstSource { get; } = firstSource;
        public DateTimeOffset ExecutionTime { get; } = executionTime;
        public bool HasBothReports { get; set; }
        public bool HasOpenBreak { get; set; }
    }
}
