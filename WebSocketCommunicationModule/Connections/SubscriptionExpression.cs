namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections;

// Ignore literal casing, never fold regex escapes such as \d and \D.
internal static class SubscriptionExpression
{
    internal static string Canonical(string value) =>
        value.IndexOfAny(@"\.^$|?*+()[]{}".ToCharArray()) >= 0 ? "R" + value : "L" + value.ToUpperInvariant();
    internal static bool Equal(string a, string b) => Canonical(a) == Canonical(b);
    internal static int Hash(string value) => StringComparer.Ordinal.GetHashCode(Canonical(value));
    internal static string Encode(string value)
    {
        var canonical = Canonical(value);
        return canonical.Length + ":" + canonical;
    }
}
