// Core/ has no Eco dependencies so it can be compiled into the unit-test project by link.
namespace EcoToPrometheus.Core
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    public enum MetricType { Counter, Gauge }

    /// <summary>Object references the hook captures as ids and the worker resolves to names off the game thread.</summary>
    public enum RefKind { Player, Currency, Settlement, Demographic, ElectedTitle }

    /// <summary>One label. Ordering is by name, then value, both ordinal, which is the order Prometheus wants.</summary>
    public readonly record struct Label(string Name, string Value) : IComparable<Label>
    {
        public int CompareTo(Label other)
        {
            var c = string.CompareOrdinal(this.Name, other.Name);
            return c != 0 ? c : string.CompareOrdinal(this.Value, other.Value);
        }
    }

    /// <summary>A label whose value is looked up later from an Eco id (see <see cref="INameResolver"/>).</summary>
    public readonly record struct DeferredLabel(string Name, RefKind Kind, int Id);

    /// <summary>
    /// Stable key for one series: <c>family|name=value|name=value</c> with labels sorted by name.
    /// Used as the registry dictionary key and as the state-file key, so it must round-trip exactly.
    /// </summary>
    public static class SeriesKey
    {
        public static string Build(string family, Label[] sortedLabels)
        {
            if (sortedLabels.Length == 0) return family;
            var sb = new StringBuilder(family.Length + sortedLabels.Length * 24);
            sb.Append(family);
            foreach (var l in sortedLabels)
            {
                sb.Append('|');
                Escape(sb, l.Name);
                sb.Append('=');
                Escape(sb, l.Value);
            }
            return sb.ToString();
        }

        public static (string Family, Label[] Labels) Parse(string key)
        {
            // The first split must keep the escapes so that an escaped '=' inside a value survives to the second split.
            var parts = Split(key, '|', unescape: false);
            var family = Unescape(parts[0]);
            if (parts.Count == 1) return (family, Array.Empty<Label>());
            var labels = new Label[parts.Count - 1];
            for (int i = 1; i < parts.Count; i++)
            {
                var kv = Split(parts[i], '=', unescape: true);
                if (kv.Count != 2) throw new FormatException($"Bad series key segment '{parts[i]}' in '{key}'.");
                labels[i - 1] = new Label(kv[0], kv[1]);
            }
            return (family, labels);
        }

        static void Escape(StringBuilder sb, string s)
        {
            foreach (var ch in s)
            {
                if (ch == '\\' || ch == '|' || ch == '=') sb.Append('\\');
                sb.Append(ch);
            }
        }

        /// <summary>Splits on unescaped <paramref name="sep"/>; escape pairs are either resolved or copied through verbatim.</summary>
        static List<string> Split(string s, char sep, bool unescape)
        {
            var result = new List<string>();
            var sb = new StringBuilder();
            for (int i = 0; i < s.Length; i++)
            {
                var ch = s[i];
                if (ch == '\\' && i + 1 < s.Length)
                {
                    if (!unescape) sb.Append(ch);
                    sb.Append(s[++i]);
                    continue;
                }
                if (ch == sep) { result.Add(sb.ToString()); sb.Clear(); continue; }
                sb.Append(ch);
            }
            result.Add(sb.ToString());
            return result;
        }

        static string Unescape(string s)
        {
            if (s.IndexOf('\\') < 0) return s;
            var sb = new StringBuilder(s.Length);
            for (int i = 0; i < s.Length; i++)
            {
                if (s[i] == '\\' && i + 1 < s.Length) i++;
                sb.Append(s[i]);
            }
            return sb.ToString();
        }
    }
}
