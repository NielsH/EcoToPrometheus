namespace EcoToPrometheus.Tests
{
    using System;
    using System.Linq;
    using EcoToPrometheus.Core;
    using Xunit;

    public class MetricRegistryTests
    {
        static readonly DateTime Now = new(2026, 9, 20, 0, 0, 0, DateTimeKind.Utc);

        static Label[] L(params (string Name, string Value)[] pairs) => pairs.Select(p => new Label(p.Name, p.Value)).ToArray();

        static SeriesSnapshot Only(MetricsSnapshot snap, string family) => Assert.Single(snap.Families.Single(f => f.Name == family).Series);

        [Fact]
        public void Increment_creates_then_adds()
        {
            var r = new MetricRegistry();
            r.IncrementCounter("c_total", L(("a", "1")), 2);
            Assert.Equal(2, Only(r.Publish(Now), "c_total").Value);
            r.IncrementCounter("c_total", L(("a", "1")), 3);
            Assert.Equal(5, Only(r.Publish(Now), "c_total").Value);
            Assert.Equal(1, r.SeriesCount);
        }

        [Fact]
        public void Labels_are_sorted_regardless_of_input_order()
        {
            var r = new MetricRegistry();
            r.IncrementCounter("c_total", L(("b", "2"), ("a", "1")), 1);
            r.IncrementCounter("c_total", L(("a", "1"), ("b", "2")), 1);
            var s = Only(r.Publish(Now), "c_total");
            Assert.Equal(2, s.Value);
            Assert.Equal(L(("a", "1"), ("b", "2")), s.Labels);
        }

        [Fact]
        public void Input_label_array_is_not_mutated()
        {
            var r = new MetricRegistry();
            var input = L(("b", "2"), ("a", "1"));
            r.IncrementCounter("c_total", input, 1);
            Assert.Equal("b", input[0].Name);
        }

        [Fact]
        public void SetGauge_with_same_value_does_not_dirty()
        {
            var r = new MetricRegistry();
            r.SetGauge("g", L(("a", "1")), 1);
            var first = r.Publish(Now);
            r.SetGauge("g", L(("a", "1")), 1);
            Assert.Same(first, r.Publish(Now));
            r.SetGauge("g", L(("a", "1")), 2);
            var third = r.Publish(Now);
            Assert.NotSame(first, third);
            Assert.Equal(2, Only(third, "g").Value);
        }

        [Fact]
        public void Publish_only_rebuilds_when_dirty()
        {
            var r = new MetricRegistry();
            r.IncrementCounter("c_total", CounterIncrement.NoLabels, 1);
            var a = r.Publish(Now);
            var b = r.Publish(Now.AddSeconds(1));
            Assert.Same(a, b);
            Assert.Equal(Now, b.GeneratedAtUtc);
            r.IncrementCounter("c_total", CounterIncrement.NoLabels, 1);
            Assert.NotSame(a, r.Publish(Now.AddSeconds(2)));
        }

        [Fact]
        public void Current_is_Empty_before_first_publish_and_snapshot_after()
        {
            var r = new MetricRegistry();
            Assert.Same(MetricsSnapshot.Empty, r.Current);
            r.IncrementCounter("c_total", CounterIncrement.NoLabels, 1);
            var published = r.Publish(Now);
            Assert.Same(published, r.Current);
        }

        [Fact]
        public void Families_and_series_come_out_sorted()
        {
            var r = new MetricRegistry();
            r.SetGauge("zz", L(("x", "2")), 1);
            r.SetGauge("zz", L(("x", "10")), 1);
            r.IncrementCounter("aa_total", L(("p", "b"), ("q", "1")), 1);
            r.IncrementCounter("aa_total", L(("p", "a"), ("q", "2")), 1);
            r.IncrementCounter("aa_total", CounterIncrement.NoLabels, 1);
            var snap = r.Publish(Now);
            Assert.Equal(new[] { "aa_total", "zz" }, snap.Families.Select(f => f.Name));
            var aa = snap.Families[0].Series;
            Assert.Empty(aa[0].Labels);
            Assert.Equal("a", aa[1].Labels[0].Value);
            Assert.Equal("b", aa[2].Labels[0].Value);
            // ordinal, so "10" < "2"
            Assert.Equal(new[] { "10", "2" }, snap.Families[1].Series.Select(s => s.Labels[0].Value));
        }

        [Fact]
        public void ClearGaugeFamily_removes_only_that_family()
        {
            var r = new MetricRegistry();
            r.SetGauge("g1", L(("a", "1")), 1);
            r.SetGauge("g1", L(("a", "2")), 1);
            r.SetGauge("g2", L(("a", "1")), 1);
            r.IncrementCounter("c_total", CounterIncrement.NoLabels, 1);
            var before = r.Publish(Now);
            r.ClearGaugeFamily("g1");
            var after = r.Publish(Now);
            Assert.NotSame(before, after);
            Assert.Equal(new[] { "c_total", "g2" }, after.Families.Select(f => f.Name));
            Assert.Equal(2, r.SeriesCount);
            // clearing an absent family is a no-op that does not dirty
            r.ClearGaugeFamily("g1");
            Assert.Same(after, r.Publish(Now));
        }

        [Fact]
        public void Export_Import_round_trip()
        {
            var r = new MetricRegistry();
            r.RegisterFamily("c_total", MetricType.Counter, "help");
            r.IncrementCounter("c_total", L(("player", "Zoë|=\\"), ("item", "Bread")), 0.1 + 0.2);
            r.IncrementCounter("c_total", CounterIncrement.NoLabels, 1e300);
            r.SetGauge("g", CounterIncrement.NoLabels, 5);
            var exported = r.ExportCounters().ToList();
            Assert.Equal(2, exported.Count);

            var r2 = new MetricRegistry();
            r2.ImportCounters(exported);
            var snap = r2.Publish(Now);
            var fam = Assert.Single(snap.Families);
            Assert.Equal("c_total", fam.Name);
            Assert.Equal(MetricType.Counter, fam.Type);
            Assert.Equal(2, fam.Series.Count);
            Assert.Equal(1e300, fam.Series[0].Value);
            Assert.Equal(0.1 + 0.2, fam.Series[1].Value);
            Assert.Equal(L(("item", "Bread"), ("player", "Zoë|=\\")), fam.Series[1].Labels);
            Assert.Equal(r.ExportCounters().OrderBy(k => k.Key), r2.ExportCounters().OrderBy(k => k.Key));

            // help arrives later from the hooks and fills the empty slot
            r2.RegisterFamily("c_total", MetricType.Counter, "help");
            Assert.Equal("help", r2.Publish(Now).Families[0].Help);
        }

        [Fact]
        public void ResetCounters_keeps_gauges()
        {
            var r = new MetricRegistry();
            r.IncrementCounter("c_total", CounterIncrement.NoLabels, 1);
            r.SetGauge("g", CounterIncrement.NoLabels, 1);
            r.ResetCounters();
            var snap = r.Publish(Now);
            Assert.Equal("g", Assert.Single(snap.Families).Name);
            Assert.Empty(r.ExportCounters());
            Assert.True(r.TryGetFamily("c_total", out _));
        }

        [Fact]
        public void Counter_gauge_type_conflict_throws()
        {
            var r = new MetricRegistry();
            r.IncrementCounter("x", CounterIncrement.NoLabels, 1);
            Assert.Throws<InvalidOperationException>(() => r.SetGauge("x", CounterIncrement.NoLabels, 1));
            Assert.Throws<InvalidOperationException>(() => r.RegisterFamily("x", MetricType.Gauge, string.Empty));
            r.SetGauge("y", CounterIncrement.NoLabels, 1);
            Assert.Throws<InvalidOperationException>(() => r.IncrementCounter("y", CounterIncrement.NoLabels, 1));
        }

        [Fact]
        public void RegisterFamily_first_help_wins_unless_empty()
        {
            var r = new MetricRegistry();
            r.RegisterFamily("f", MetricType.Gauge, "first");
            r.RegisterFamily("f", MetricType.Gauge, "second");
            Assert.True(r.TryGetFamily("f", out var info));
            Assert.Equal("first", info.Help);
            r.RegisterFamily("e", MetricType.Gauge, string.Empty);
            r.RegisterFamily("e", MetricType.Gauge, "filled");
            Assert.True(r.TryGetFamily("e", out info));
            Assert.Equal("filled", info.Help);
        }

        [Fact]
        public void Registered_family_without_series_is_not_published()
        {
            var r = new MetricRegistry();
            r.RegisterFamily("empty", MetricType.Gauge, "nothing yet");
            Assert.Empty(r.Publish(Now).Families);
        }
    }
}
