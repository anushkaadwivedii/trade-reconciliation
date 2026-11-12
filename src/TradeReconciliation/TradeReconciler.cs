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
    long Quantity);

public sealed record QuantityBreak(
    Guid Id,
    long InternalQuantity,
    long BrokerQuantity,
    BreakStatus Status);

public sealed record ReconciliationResult(
    ReconciliationStatus Status,
    IReadOnlyList<QuantityBreak> Breaks);

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

        var status = quantitiesMatch
            ? ReconciliationStatus.Matched
            : ReconciliationStatus.Mismatched;

        return new ReconciliationResult(status, Array.AsReadOnly(trade.Breaks.ToArray()));
    }

    // The same ordered reports produce the same break IDs when replayed.
    private static Guid CreateBreakId(string tradeRef, int breakNumber)
    {
        var key = FormattableString.Invariant($"{tradeRef}:{breakNumber}");
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        return new Guid(hash.AsSpan(0, 16));
    }

    private sealed class TradeState
    {
        public TradeReport? Internal { get; set; }
        public TradeReport? Broker { get; set; }
        public List<QuantityBreak> Breaks { get; } = new();
    }
}
