using System;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace TradeReconciliation;

public static class TradeReportNormalizer
{
    public static TradeReport Normalize(TradeSource source, string payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        var (referenceField, versionField, quantityField, priceField, timeField) = source switch
        {
            TradeSource.Internal => ("tradeRef", "version", "quantity", "price", "execTimeUtc"),
            TradeSource.BrokerA => ("execution_id", "revision", "qty", "px", "executed_at"),
            _ => throw new ArgumentOutOfRangeException(nameof(source), source, "Unsupported trade source.")
        };

        try
        {
            using var document = JsonDocument.Parse(payload, new JsonDocumentOptions
            {
                AllowDuplicateProperties = false
            });

            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException("Trade report must be a JSON object.");
            }

            return new TradeReport(
                ReadTradeReference(root, referenceField),
                source,
                (int)ReadPositiveInteger(root, versionField, int.MaxValue),
                ReadPositiveInteger(root, quantityField, long.MaxValue),
                ReadOptionalPrice(root, priceField),
                root.TryGetProperty(timeField, out var timestamp) ? ParseUtcTimestamp(timestamp, timeField) : null);
        }
        catch (JsonException exception)
        {
            throw new FormatException("Trade report must be valid JSON with unique property names.", exception);
        }
    }

    public static DateTimeOffset ParseUtcTimestamp(JsonElement element, string field)
    {
        if (element.ValueKind == JsonValueKind.String
            && element.GetString() is { } value
            && Regex.IsMatch(value, @"\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z\z")
            && DateTimeOffset.TryParseExact(value, "yyyy-MM-dd'T'HH:mm:ss.FFFFFF'Z'",
                CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var timestamp))
        {
            return timestamp;
        }

        throw new FormatException($"Field '{field}' must be a UTC timestamp ending in Z with at most six fractional digits.");
    }

    private static string ReadTradeReference(JsonElement root, string field)
    {
        if (root.TryGetProperty(field, out var element)
            && element.ValueKind == JsonValueKind.String)
        {
            var reference = element.GetString();
            if (!string.IsNullOrWhiteSpace(reference))
            {
                return reference;
            }
        }

        throw new FormatException($"Field '{field}' must be a nonblank string.");
    }

    private static decimal? ReadOptionalPrice(JsonElement root, string field)
    {
        if (!root.TryGetProperty(field, out var element))
        {
            return null;
        }

        // Check the raw digits before conversion to avoid rounding in .NET or PostgreSQL.
        if (element.ValueKind == JsonValueKind.Number
            && Regex.IsMatch(element.GetRawText(), @"\A[0-9]{1,10}(\.[0-9]{1,8})?\z")
            && element.TryGetDecimal(out var price)
            && price > 0)
        {
            return price;
        }

        throw new FormatException($"Field '{field}' must be a positive decimal with up to 10 whole and 8 fractional digits.");
    }

    private static long ReadPositiveInteger(JsonElement root, string field, long maxValue)
    {
        if (root.TryGetProperty(field, out var element)
            && element.ValueKind == JsonValueKind.Number
            && element.TryGetInt64(out var value)
            && value > 0
            && value <= maxValue)
        {
            return value;
        }

        throw new FormatException($"Field '{field}' must be a positive integer no larger than {maxValue}.");
    }
}
