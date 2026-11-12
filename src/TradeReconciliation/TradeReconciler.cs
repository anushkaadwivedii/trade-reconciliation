using System;
using System.Collections.Generic;

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
    Matched
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
    public ReconciliationResult Apply(TradeReport report)
    {
        throw new NotImplementedException();
    }
}
