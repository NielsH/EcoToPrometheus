namespace EcoToPrometheus.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.Json;
    using EcoToPrometheus.Core;
    using Xunit;

    public class ExpositionTests
    {
        static readonly DateTime When = new(2026, 9, 20, 12, 34, 56, DateTimeKind.Utc);

        /// <summary>Families are registered out of order on purpose; the registry sorts them and the writer keeps that order.</summary>
        static MetricsSnapshot BuildSnapshot()
        {
            var r = new MetricRegistry();
            r.RegisterFamily("zeta_events_total", MetricType.Counter, "Count of events\nwith a back\\slash");
            r.IncrementCounter("zeta_events_total", new[] { new Label("player", "B\"o\\b\nX"), new Label("kind", "a") }, 1e21);
            r.IncrementCounter("zeta_events_total", new[] { new Label("player", "Ann"), new Label("kind", "a") }, 1.5);
            r.RegisterFamily("mid_special", MetricType.Gauge, string.Empty);
            r.SetGauge("mid_special", new[] { new Label("v", "nan") }, double.NaN);
            r.SetGauge("mid_special", new[] { new Label("v", "inf") }, double.PositiveInfinity);
            r.RegisterFamily("alpha_gauge", MetricType.Gauge, "A gauge.");
            r.SetGauge("alpha_gauge", CounterIncrement.NoLabels, 0);
            return r.Publish(When);
        }

        const string Golden =
            "# HELP alpha_gauge A gauge.\n" +
            "# TYPE alpha_gauge gauge\n" +
            "alpha_gauge 0\n" +
            "# TYPE mid_special gauge\n" +
            "mid_special{v=\"inf\"} +Inf\n" +
            "mid_special{v=\"nan\"} NaN\n" +
            "# HELP zeta_events_total Count of events\\nwith a back\\\\slash\n" +
            "# TYPE zeta_events_total counter\n" +
            "zeta_events_total{kind=\"a\",player=\"Ann\"} 1.5\n" +
            "zeta_events_total{kind=\"a\",player=\"B\\\"o\\\\b\\nX\"} 1E+21\n";

        [Fact]
        public void Text_matches_golden()
        {
            Assert.Equal(Golden, Exposition.ToPrometheusText(BuildSnapshot()));
        }

        [Fact]
        public void Text_of_empty_snapshot_is_empty()
        {
            Assert.Equal(string.Empty, Exposition.ToPrometheusText(MetricsSnapshot.Empty));
        }

        [Fact]
        public void Text_has_no_trailing_spaces_and_ends_with_newline()
        {
            var text = Exposition.ToPrometheusText(BuildSnapshot());
            Assert.EndsWith("\n", text);
            Assert.DoesNotContain("\r", text);
            foreach (var line in text.Split('\n')) Assert.False(line.EndsWith(' '), $"trailing space in '{line}'");
        }

        [Theory]
        [InlineData(0.0, "0")]
        [InlineData(1.5, "1.5")]
        [InlineData(1e21, "1E+21")]
        [InlineData(40000.0, "40000")]
        [InlineData(0.1, "0.1")]
        [InlineData(double.NaN, "NaN")]
        [InlineData(double.PositiveInfinity, "+Inf")]
        [InlineData(double.NegativeInfinity, "-Inf")]
        public void FormatValue_spells_like_prometheus(double value, string expected)
        {
            Assert.Equal(expected, Exposition.FormatValue(value));
        }

        [Fact]
        public void FormatValue_is_culture_invariant()
        {
            var previous = System.Globalization.CultureInfo.CurrentCulture;
            try
            {
                System.Globalization.CultureInfo.CurrentCulture = new System.Globalization.CultureInfo("de-DE");
                Assert.Equal("1.5", Exposition.FormatValue(1.5));
                Assert.Contains(" 1.5\n", Exposition.ToPrometheusText(BuildSnapshot()));
            }
            finally
            {
                System.Globalization.CultureInfo.CurrentCulture = previous;
            }
        }

        [Theory]
        [InlineData("plain", "plain")]
        [InlineData("back\\slash", "back\\\\slash")]
        [InlineData("say \"hi\"", "say \\\"hi\\\"")]
        [InlineData("two\nlines", "two\\nlines")]
        [InlineData("", "")]
        public void EscapeLabelValue_escapes_backslash_quote_newline(string input, string expected)
        {
            Assert.Equal(expected, Exposition.EscapeLabelValue(input));
        }

        [Fact]
        public void EscapeHelp_leaves_quotes_alone()
        {
            Assert.Equal("a \"q\" b\\\\c\\nd", Exposition.EscapeHelp("a \"q\" b\\c\nd"));
        }

        [Fact]
        public void Json_has_exactly_the_documented_keys()
        {
            using var doc = JsonDocument.Parse(Exposition.ToJson(BuildSnapshot()));
            var root = doc.RootElement;
            Assert.Equal(new[] { "generatedAt", "seriesCount", "families" }, Keys(root));
            Assert.Equal(5, root.GetProperty("seriesCount").GetInt32());
            Assert.Equal(When, root.GetProperty("generatedAt").GetDateTime().ToUniversalTime());

            var families = root.GetProperty("families").EnumerateArray().ToList();
            Assert.Equal(3, families.Count);
            Assert.Equal(new[] { "alpha_gauge", "mid_special", "zeta_events_total" }, families.Select(f => f.GetProperty("name").GetString()));
            foreach (var f in families) Assert.Equal(new[] { "name", "type", "help", "series" }, Keys(f));

            var alpha = families[0];
            Assert.Equal("gauge", alpha.GetProperty("type").GetString());
            Assert.Equal("A gauge.", alpha.GetProperty("help").GetString());
            var alphaSeries = Assert.Single(alpha.GetProperty("series").EnumerateArray());
            Assert.Equal(new[] { "labels", "value" }, Keys(alphaSeries));
            Assert.Empty(alphaSeries.GetProperty("labels").EnumerateObject());
            Assert.Equal(0.0, alphaSeries.GetProperty("value").GetDouble());

            var zeta = families[2];
            Assert.Equal("counter", zeta.GetProperty("type").GetString());
            Assert.Equal("Count of events\nwith a back\\slash", zeta.GetProperty("help").GetString());
            var zetaSeries = zeta.GetProperty("series").EnumerateArray().ToList();
            Assert.Equal(2, zetaSeries.Count);
            Assert.Equal(new[] { "kind", "player" }, Keys(zetaSeries[0].GetProperty("labels")));
            Assert.Equal("Ann", zetaSeries[0].GetProperty("labels").GetProperty("player").GetString());
            Assert.Equal(1.5, zetaSeries[0].GetProperty("value").GetDouble());
            Assert.Equal("B\"o\\b\nX", zetaSeries[1].GetProperty("labels").GetProperty("player").GetString());
            Assert.Equal(1e21, zetaSeries[1].GetProperty("value").GetDouble());
        }

        [Fact]
        public void Json_writes_non_finite_values_as_prometheus_strings()
        {
            using var doc = JsonDocument.Parse(Exposition.ToJson(BuildSnapshot()));
            var mid = doc.RootElement.GetProperty("families")[1];
            Assert.Equal("mid_special", mid.GetProperty("name").GetString());
            Assert.Equal(string.Empty, mid.GetProperty("help").GetString());
            var series = mid.GetProperty("series").EnumerateArray().ToList();
            Assert.Equal(JsonValueKind.String, series[0].GetProperty("value").ValueKind);
            Assert.Equal("+Inf", series[0].GetProperty("value").GetString());
            Assert.Equal("NaN", series[1].GetProperty("value").GetString());
        }

        [Fact]
        public void Json_round_trips_through_JsonDocument()
        {
            var first = Exposition.ToJson(BuildSnapshot());
            using var doc = JsonDocument.Parse(first);
            var again = JsonSerializer.Serialize(doc.RootElement);
            using var doc2 = JsonDocument.Parse(again);
            Assert.Equal(first, JsonSerializer.Serialize(doc2.RootElement));
        }

        [Fact]
        public void Json_of_empty_snapshot_is_valid()
        {
            using var doc = JsonDocument.Parse(Exposition.ToJson(MetricsSnapshot.Empty));
            Assert.Equal(0, doc.RootElement.GetProperty("seriesCount").GetInt32());
            Assert.Empty(doc.RootElement.GetProperty("families").EnumerateArray());
            Assert.EndsWith("Z", doc.RootElement.GetProperty("generatedAt").GetString());
        }

        static IEnumerable<string> Keys(JsonElement obj) => obj.EnumerateObject().Select(p => p.Name).ToList();
    }
}
