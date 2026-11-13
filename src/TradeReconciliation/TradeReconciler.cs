using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace TradeReconciliation;

public enum TradeSource
{
    Internal,
    BrokerA
}

public enum ReconciliationStatus
{
    WaitingForBroker,
    Mismatched,
    Matched,
    WaitingForInternal
}

public enum BreakStatus
{
    Open,
    AutoResolved
}

public sealed record TradeReport(
    string TradeRef,
    TradeSource Source,
    int Version,
    long Quantity,
    decimal? Price = null,
    DateTimeOffset? ExecutionTime = null);

public sealed record QuantityBreak(
    Guid Id,
    long InternalQuantity,
    long BrokerQuantity,
    BreakStatus Status);

public sealed record PriceBreak(
    Guid Id,
    decimal InternalPrice,
    decimal BrokerPrice,
    BreakStatus Status);

public sealed record ReconciliationResult(
    ReconciliationStatus Status,
    IReadOnlyList<QuantityBreak> Breaks)
{
    public IReadOnlyList<PriceBreak> PriceBreaks { get; init; } = Array.Empty<PriceBreak>();
}

public sealed class TradeReconciler
{
    private readonly Dictionary<string, TradeState> _trades = new(StringComparer.Ordinal);

    public ReconciliationResult Apply(TradeReport report)
    {
        if (report.Source is not (TradeSource.Internal or TradeSource.BrokerA))
        {
            throw new ArgumentOutOfRangeException(nameof(report), "Unsupported trade source.");
        }

        if (!_trades.TryGetValue(report.TradeRef, out var trade))
        {
            trade = new TradeState();
            _trades.Add(report.TradeRef, trade);
        }

        var current = report.Source == TradeSource.Internal ? trade.Internal : trade.Broker;
        if (current is null || report.Version > current.Version)
        {
            if (report.Source == TradeSource.Internal)
            {
                trade.Internal = report;
            }
            else
            {
                trade.Broker = report;
            }
        }

        var internalReport = trade.Internal;
        var brokerReport = trade.Broker;
        if (internalReport is null)
        {
            return new ReconciliationResult(
                ReconciliationStatus.WaitingForInternal,
                Array.Empty<QuantityBreak>());
        }

        if (brokerReport is null)
        {
            return new ReconciliationResult(
                ReconciliationStatus.WaitingForBroker,
                Array.Empty<QuantityBreak>());
        }

        var quantitiesMatch = internalReport.Quantity == brokerReport.Quantity;
        var hasOpenBreak = trade.Breaks.Count > 0 && trade.Breaks[^1].Status == BreakStatus.Open;

        if (quantitiesMatch && hasOpenBreak)
        {
            trade.Breaks[^1] = trade.Breaks[^1] with { Status = BreakStatus.AutoResolved };
        }
        else if (!quantitiesMatch && !hasOpenBreak)
        {
            trade.Breaks.Add(new QuantityBreak(
                CreateBreakId(report.TradeRef, trade.Breaks.Count + 1),
                internalReport.Quantity,
                brokerReport.Quantity,
                BreakStatus.Open));
        }

        var priceMismatch = UpdatePriceBreak(trade, internalReport, brokerReport);
        var status = quantitiesMatch && !priceMismatch
            ? ReconciliationStatus.Matched
            : ReconciliationStatus.Mismatched;

        return new ReconciliationResult(status, Array.AsReadOnly(trade.Breaks.ToArray()))
        {
            PriceBreaks = Array.AsReadOnly(trade.PriceBreaks.ToArray())
        };
    }

    private static bool UpdatePriceBreak(TradeState trade, TradeReport internalReport, TradeReport brokerReport)
    {
        var hasOpenBreak = trade.PriceBreaks.Count > 0 && trade.PriceBreaks[^1].Status == BreakStatus.Open;
        if (internalReport.Price is not decimal internalPrice || brokerReport.Price is not decimal brokerPrice)
        {
            // A missing price cannot confirm that an open break is fixed.
            return hasOpenBreak;
        }

        if (internalPrice == brokerPrice)
        {
            if (hasOpenBreak)
            {
                trade.PriceBreaks[^1] = trade.PriceBreaks[^1] with { Status = BreakStatus.AutoResolved };
            }

            return false;
        }

        if (!hasOpenBreak)
        {
            trade.PriceBreaks.Add(new PriceBreak(
                CreateBreakId(internalReport.TradeRef, trade.PriceBreaks.Count + 1, ":price"),
                internalPrice,
                brokerPrice,
                BreakStatus.Open));
        }

        return true;
    }

    // The same ordered reports produce the same break IDs when replayed.
    private static Guid CreateBreakId(string tradeRef, int breakNumber, string suffix = "")
    {
        var key = FormattableString.Invariant($"{tradeRef}:{breakNumber}{suffix}");
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        return new Guid(hash.AsSpan(0, 16));
    }

    private sealed class TradeState
    {
        public TradeReport? Internal { get; set; }
        public TradeReport? Broker { get; set; }
        public List<QuantityBreak> Breaks { get; } = new();
        public List<PriceBreak> PriceBreaks { get; } = new();
    }
}
