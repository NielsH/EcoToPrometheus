namespace EcoToPrometheus.Core
{
    using System;
    using System.Buffers;
    using System.Globalization;
    using System.Text;
    using System.Text.Json;

    /// <summary>
    /// Formats a <see cref="MetricsSnapshot"/> as Prometheus text exposition (version 0.0.4) or as JSON.
    /// Pure string building over an immutable snapshot: runs on every scrape, never touches game state.
    /// </summary>
    public static class Exposition
    {
        public const string PrometheusContentType = "text/plain; version=0.0.4; charset=utf-8";

        /// <summary>True once the writers exist (phase 2). Gates <c>MetricsPlugin.IsReady</c>.</summary>
        public static readonly bool Available = true;

        /// <summary>Rough bytes per series line, used only to size the builder up front.</summary>
        const int BytesPerSeriesHint = 96;
        const int BytesPerFamilyHint = 160;

        static readonly JsonWriterOptions JsonOptions = new() { Indented = false, SkipValidation = false };

        /// <summary>
        /// Text exposition: per family a <c># HELP</c> line (omitted when help is empty), a <c># TYPE</c> line and one
        /// sample line per series. Families and series come out in snapshot order (already sorted by the registry).
        /// No timestamps, <c>\n</c> line endings, ends with a newline.
        /// </summary>
        public static string ToPrometheusText(MetricsSnapshot snapshot)
        {
            var sb = new StringBuilder(snapshot.Families.Count * BytesPerFamilyHint + snapshot.SeriesCount * BytesPerSeriesHint);
            foreach (var family in snapshot.Families)
            {
                if (family.Help.Length > 0)
                {
                    sb.Append("# HELP ").Append(family.Name).Append(' ');
                    AppendEscapedHelp(sb, family.Help);
                    sb.Append('\n');
                }
                sb.Append("# TYPE ").Append(family.Name).Append(' ').Append(TypeName(family.Type)).Append('\n');

                foreach (var series in family.Series)
                {
                    sb.Append(family.Name);
                    var labels = series.Labels;
                    if (labels.Length > 0)
                    {
                        sb.Append('{');
                        for (int i = 0; i < labels.Length; i++)
                        {
                            if (i > 0) sb.Append(',');
                            sb.Append(labels[i].Name).Append("=\"");
                            AppendEscapedLabelValue(sb, labels[i].Value);
                            sb.Append('"');
                        }
                        sb.Append('}');
                    }
                    sb.Append(' ');
                    AppendValue(sb, series.Value);
                    sb.Append('\n');
                }
            }
            return sb.ToString();
        }

        /// <summary>
        /// JSON view of the same snapshot:
        /// <c>{ "generatedAt", "seriesCount", "families": [ { "name", "type", "help", "series": [ { "labels": {}, "value" } ] } ] }</c>.
        /// Values are JSON numbers; NaN and infinities, which JSON cannot carry, are written as the strings
        /// <c>"NaN"</c>, <c>"+Inf"</c> and <c>"-Inf"</c> (the Prometheus spellings).
        /// </summary>
        public static string ToJson(MetricsSnapshot snapshot)
        {
            var buffer = new ArrayBufferWriter<byte>(snapshot.Families.Count * BytesPerFamilyHint + snapshot.SeriesCount * BytesPerSeriesHint + 64);
            using (var w = new Utf8JsonWriter(buffer, JsonOptions))
            {
                w.WriteStartObject();
                w.WriteString("generatedAt", DateTime.SpecifyKind(snapshot.GeneratedAtUtc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture));
                w.WriteNumber("seriesCount", snapshot.SeriesCount);
                w.WriteStartArray("families");
                foreach (var family in snapshot.Families)
                {
                    w.WriteStartObject();
                    w.WriteString("name", family.Name);
                    w.WriteString("type", TypeName(family.Type));
                    w.WriteString("help", family.Help);
                    w.WriteStartArray("series");
                    foreach (var series in family.Series)
                    {
                        w.WriteStartObject();
                        w.WriteStartObject("labels");
                        foreach (var label in series.Labels) w.WriteString(label.Name, label.Value);
                        w.WriteEndObject();
                        if (double.IsFinite(series.Value)) w.WriteNumber("value", series.Value);
                        else                               w.WriteString("value", FormatValue(series.Value));
                        w.WriteEndObject();
                    }
                    w.WriteEndArray();
                    w.WriteEndObject();
                }
                w.WriteEndArray();
                w.WriteEndObject();
            }
            return Encoding.UTF8.GetString(buffer.WrittenSpan);
        }

        /// <summary>Escapes a label value for the text format: backslash, double quote and newline.</summary>
        public static string EscapeLabelValue(string value)
        {
            if (value.AsSpan().IndexOfAny('\\', '"', '\n') < 0) return value;
            var sb = new StringBuilder(value.Length + 8);
            AppendEscapedLabelValue(sb, value);
            return sb.ToString();
        }

        /// <summary>Escapes HELP text for the text format: backslash and newline only (quotes are literal there).</summary>
        public static string EscapeHelp(string help)
        {
            if (help.AsSpan().IndexOfAny('\\', '\n') < 0) return help;
            var sb = new StringBuilder(help.Length + 8);
            AppendEscapedHelp(sb, help);
            return sb.ToString();
        }

        /// <summary>
        /// Sample value as Prometheus spells it: shortest round-trip invariant form (<c>1.5</c>, <c>1E+21</c>),
        /// <c>+Inf</c>, <c>-Inf</c> or <c>NaN</c>.
        /// </summary>
        public static string FormatValue(double value)
        {
            if (double.IsNaN(value))              return "NaN";
            if (double.IsPositiveInfinity(value)) return "+Inf";
            if (double.IsNegativeInfinity(value)) return "-Inf";
            return value.ToString("R", CultureInfo.InvariantCulture);
        }

        static string TypeName(MetricType type) => type == MetricType.Counter ? "counter" : "gauge";

        static void AppendValue(StringBuilder sb, double value)
        {
            if (double.IsFinite(value)) sb.Append(value.ToString("R", CultureInfo.InvariantCulture));
            else                        sb.Append(FormatValue(value));
        }

        static void AppendEscapedLabelValue(StringBuilder sb, string value)
        {
            foreach (var ch in value)
            {
                switch (ch)
                {
                    case '\\': sb.Append("\\\\"); break;
                    case '"':  sb.Append("\\\""); break;
                    case '\n': sb.Append("\\n");  break;
                    default:   sb.Append(ch);     break;
                }
            }
        }

        static void AppendEscapedHelp(StringBuilder sb, string help)
        {
            foreach (var ch in help)
            {
                switch (ch)
                {
                    case '\\': sb.Append("\\\\"); break;
                    case '\n': sb.Append("\\n");  break;
                    default:   sb.Append(ch);     break;
                }
            }
        }
    }
}
