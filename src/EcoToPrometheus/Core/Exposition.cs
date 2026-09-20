namespace EcoToPrometheus.Core
{
    using System;

    /// <summary>
    /// Formats a <see cref="MetricsSnapshot"/> as Prometheus text exposition or JSON.
    /// Phase 2 implements this (plus tests); until then <see cref="Available"/> is false and the controller serves 503.
    /// </summary>
    public static class Exposition
    {
        public const string PrometheusContentType = "text/plain; version=0.0.4; charset=utf-8";

        /// <summary>Flipped to true by phase 2 once the writers exist. Gates <c>MetricsPlugin.IsReady</c>.</summary>
        public static readonly bool Available = false;

        public static string ToPrometheusText(MetricsSnapshot snapshot) =>
            throw new NotImplementedException("Phase 2: Exposition.ToPrometheusText");

        public static string ToJson(MetricsSnapshot snapshot) =>
            throw new NotImplementedException("Phase 2: Exposition.ToJson");

        /// <summary>Escapes a label value for the text format: backslash, double quote and newline.</summary>
        public static string EscapeLabelValue(string value) =>
            throw new NotImplementedException("Phase 2: Exposition.EscapeLabelValue");
    }
}
