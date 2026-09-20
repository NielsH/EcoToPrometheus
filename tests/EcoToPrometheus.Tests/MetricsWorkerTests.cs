namespace EcoToPrometheus.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using EcoToPrometheus.Core;
    using Xunit;

    public class MetricsWorkerTests
    {
        static readonly DateTime Now = new(2026, 9, 20, 0, 0, 0, DateTimeKind.Utc);

        sealed class FakeResolver : INameResolver
        {
            public readonly Dictionary<(RefKind, int), string> Names = new();
            public string? Resolve(RefKind kind, int id) => this.Names.TryGetValue((kind, id), out var n) ? n : null;
        }

        sealed class FakeGauge : IGaugeSource
        {
            readonly Action<IGaugeSink> sample;
            public int Calls;
            public FakeGauge(string group, Action<IGaugeSink> sample) { this.Group = group; this.sample = sample; }
            public string Group { get; }
            public void Sample(IGaugeSink sink) { this.Calls++; this.sample(sink); }
        }

        sealed class Harness
        {
            public readonly MetricRegistry  Registry = new();
            public readonly FakeResolver    Resolver = new();
            public readonly WorkerOptions   Options  = new() { ModVersion = "2.0.0-test", EcoVersion = "0.14.1.2" };
            public readonly List<string>    Warnings = new();
            public readonly List<(string Message, Exception Ex)> Errors = new();
            public readonly MetricsWorker   Worker;

            public Harness(Action<WorkerOptions>? configure = null)
            {
                configure?.Invoke(this.Options);
                this.Worker = new MetricsWorker(this.Registry, this.Resolver, this.Options, this.Warnings.Add, (m, e) => this.Errors.Add((m, e)));
            }
        }

        static EventRecord Record(string family, Label[] labels, DeferredLabel[] deferred, double delta = 1) =>
            new(new[] { new CounterIncrement(family, labels, deferred, delta) }, 0);

        static SeriesSnapshot Only(MetricsSnapshot snap, string family) => Assert.Single(snap.Families.Single(f => f.Name == family).Series);

        [Fact]
        public void Enqueue_from_four_threads_then_Tick_drains_everything()
        {
            const int threads = 4, perThread = 10_000;
            var h = new Harness();
            var start = new ManualResetEventSlim(false);
            var workers = Enumerable.Range(0, threads).Select(_ => new Thread(() =>
            {
                start.Wait();
                for (int i = 0; i < perThread; i++)
                    h.Worker.Enqueue(Record("eco_action_chop_tree_total", new[] { new Label("species", "Oak") }, CounterIncrement.NoDeferred));
            })).ToList();
            foreach (var t in workers) t.Start();
            start.Set();
            foreach (var t in workers) t.Join();

            var before = h.Worker.Snapshot();
            Assert.Equal(threads * perThread, before.EventsQueued);
            Assert.False(before.HasPublished);

            h.Worker.Tick(Now);

            var after = h.Worker.Snapshot();
            Assert.Equal(threads * perThread, after.EventsProcessedTotal);
            Assert.Equal(0, after.EventsQueued);
            Assert.True(after.QueueHighWater >= 1);
            Assert.True(after.QueueHighWater <= threads * perThread);
            Assert.True(after.HasPublished);
            Assert.True(h.Worker.HasPublished);
            Assert.Equal(Now, after.LastTickUtc);
            Assert.Empty(after.HandlerErrorsTotal);
            Assert.Equal(threads * perThread, Only(h.Registry.Current, "eco_action_chop_tree_total").Value);
            Assert.Equal(threads * perThread, Only(h.Registry.Current, "eco_exporter_events_processed_total").Value);
            Assert.Equal(0, Only(h.Registry.Current, "eco_exporter_events_queued").Value);
        }

        [Fact]
        public void Deferred_labels_are_resolved_and_unknown_ids_get_placeholder()
        {
            var h = new Harness();
            h.Resolver.Names[(RefKind.Player, 7)] = "Ann";
            h.Resolver.Names[(RefKind.Currency, 3)] = "Gold";
            h.Worker.Enqueue(Record("f_total", new[] { new Label("item", "Bread") },
                new[] { new DeferredLabel("player", RefKind.Player, 7), new DeferredLabel("currency", RefKind.Currency, 3) }));
            h.Worker.Enqueue(Record("f_total", new[] { new Label("item", "Bread") },
                new[] { new DeferredLabel("player", RefKind.Player, 42), new DeferredLabel("currency", RefKind.Currency, 3) }));
            h.Worker.Tick(Now);

            var series = h.Registry.Current.Families.Single(f => f.Name == "f_total").Series;
            Assert.Equal(2, series.Count);
            Assert.Equal(new[] { new Label("currency", "Gold"), new Label("item", "Bread"), new Label("player", "Ann") }, series[0].Labels);
            Assert.Equal(new[] { new Label("currency", "Gold"), new Label("item", "Bread"), new Label("player", "player_42") }, series[1].Labels);
        }

        [Fact]
        public void Resolved_and_gauge_label_values_are_truncated()
        {
            var h = new Harness(o => o.MaxLabelValueLength = 5);
            h.Resolver.Names[(RefKind.Player, 1)] = "Abcdefghij";
            h.Worker.Enqueue(Record("f_total", CounterIncrement.NoLabels, new[] { new DeferredLabel("player", RefKind.Player, 1) }));
            h.Worker.AddGaugeSource(new FakeGauge("Live", sink => sink.Set("g", new[] { new Label("name", "0123456789") }, 1)), TimeSpan.Zero);
            h.Worker.Tick(Now);

            Assert.Equal("Abcde", Only(h.Registry.Current, "f_total").Labels[0].Value);
            Assert.Equal("01234", Only(h.Registry.Current, "g").Labels[0].Value);
        }

        [Fact]
        public void Throwing_gauge_source_is_counted_and_does_not_stop_the_tick()
        {
            var h = new Harness();
            var good = new FakeGauge("Live", sink => sink.Set("good_gauge", CounterIncrement.NoLabels, 7));
            h.Worker.AddGaugeSource(new FakeGauge("Global", _ => throw new InvalidOperationException("boom")), TimeSpan.Zero);
            h.Worker.AddGaugeSource(good, TimeSpan.Zero);
            h.Worker.Enqueue(Record("c_total", CounterIncrement.NoLabels, CounterIncrement.NoDeferred));
            h.Worker.Tick(Now);

            var status = h.Worker.Snapshot();
            Assert.Equal(1, status.HandlerErrorsTotal["gauge"]);
            Assert.Equal(1, good.Calls);
            Assert.Equal(7, Only(h.Registry.Current, "good_gauge").Value);
            Assert.Equal(1, Only(h.Registry.Current, "c_total").Value);
            Assert.True(status.HasPublished);
            var errSeries = Only(h.Registry.Current, "eco_exporter_handler_errors_total");
            Assert.Equal(new Label("stage", "gauge"), errSeries.Labels[0]);
            Assert.Equal(1, errSeries.Value);
            var logged = Assert.Single(h.Errors);
            Assert.Contains("gauge failed", logged.Message);

            // second failure of the same type is counted but not logged again
            h.Worker.Tick(Now.AddSeconds(1));
            Assert.Equal(2, h.Worker.Snapshot().HandlerErrorsTotal["gauge"]);
            Assert.Single(h.Errors);
            Assert.Equal(2, Only(h.Registry.Current, "eco_exporter_handler_errors_total").Value);
        }

        [Fact]
        public void Gauge_sources_respect_their_interval()
        {
            var h = new Harness();
            var slow = new FakeGauge("Species", sink => sink.Set("slow", CounterIncrement.NoLabels, 1));
            h.Worker.AddGaugeSource(slow, TimeSpan.FromSeconds(30));
            h.Worker.Tick(Now);
            h.Worker.Tick(Now.AddSeconds(10));
            Assert.Equal(1, slow.Calls);
            h.Worker.Tick(Now.AddSeconds(30));
            Assert.Equal(2, slow.Calls);
        }

        [Fact]
        public void Self_metric_families_exist_after_a_tick()
        {
            var h = new Harness();
            h.Worker.Tick(Now);
            var names = h.Registry.Current.Families.Select(f => f.Name).ToList();
            foreach (var expected in new[]
            {
                "eco_exporter_events_queued", "eco_exporter_events_processed_total", "eco_exporter_queue_high_water",
                "eco_exporter_tick_duration_seconds", "eco_exporter_series", "eco_exporter_build_info",
            })
                Assert.Contains(expected, names);
            var build = Only(h.Registry.Current, "eco_exporter_build_info");
            Assert.Equal(1, build.Value);
            Assert.Equal(new[] { new Label("eco_version", "0.14.1.2"), new Label("version", "2.0.0-test") }, build.Labels);
            Assert.Equal(0, Only(h.Registry.Current, "eco_exporter_events_processed_total").Value);
            foreach (var f in h.Registry.Current.Families) Assert.NotEmpty(f.Help);
        }

        [Fact]
        public void Queue_warning_fires_once_when_high_water_crosses_threshold()
        {
            var h = new Harness(o => o.QueueWarnThreshold = 10);
            for (int i = 0; i < 9; i++) h.Worker.Enqueue(Record("c_total", CounterIncrement.NoLabels, CounterIncrement.NoDeferred));
            h.Worker.Tick(Now);
            Assert.Empty(h.Warnings);

            for (int i = 0; i < 10; i++) h.Worker.Enqueue(Record("c_total", CounterIncrement.NoLabels, CounterIncrement.NoDeferred));
            h.Worker.Tick(Now);
            var warning = Assert.Single(h.Warnings);
            Assert.Contains("backlog", warning);

            for (int i = 0; i < 50; i++) h.Worker.Enqueue(Record("c_total", CounterIncrement.NoLabels, CounterIncrement.NoDeferred));
            h.Worker.Tick(Now);
            h.Worker.Tick(Now);
            Assert.Single(h.Warnings);
            Assert.Equal(50, h.Worker.Snapshot().QueueHighWater);
        }

        [Fact]
        public void Bad_record_is_counted_and_the_rest_still_apply()
        {
            var h = new Harness();
            h.Worker.Enqueue(Record("x", CounterIncrement.NoLabels, CounterIncrement.NoDeferred));
            h.Worker.AddGaugeSource(new FakeGauge("Live", sink => sink.Set("x", CounterIncrement.NoLabels, 1)), TimeSpan.Zero);
            h.Worker.Tick(Now); // gauge write to counter family "x" throws inside the gauge stage
            Assert.Equal(1, h.Worker.Snapshot().HandlerErrorsTotal["gauge"]);

            h.Worker.Enqueue(Record("y_total", CounterIncrement.NoLabels, CounterIncrement.NoDeferred));
            h.Worker.Tick(Now.AddSeconds(1));
            Assert.Equal(1, Only(h.Registry.Current, "y_total").Value);
            Assert.Equal(2, h.Worker.Snapshot().EventsProcessedTotal);
        }
    }
}
