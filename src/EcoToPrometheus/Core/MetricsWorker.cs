namespace EcoToPrometheus.Core
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;

    public sealed class WorkerOptions
    {
        public int    MaxLabelValueLength { get; set; } = 64;
        public int    QueueWarnThreshold  { get; set; } = 100_000;
        public string ModVersion          { get; set; } = "0.0.0";
        public string EcoVersion          { get; set; } = "unknown";
    }

    /// <summary>Point-in-time health data for the status endpoint and the chat command.</summary>
    public sealed class WorkerStatus
    {
        public long                             EventsQueued          { get; init; }
        public long                             EventsProcessedTotal  { get; init; }
        public long                             QueueHighWater        { get; init; }
        public IReadOnlyDictionary<string, long> HandlerErrorsTotal   { get; init; } = new Dictionary<string, long>();
        public DateTime?                        LastTickUtc           { get; init; }
        public double                           LastTickMilliseconds  { get; init; }
        public int                              SeriesCount           { get; init; }
        public bool                             HasPublished          { get; init; }
    }

    /// <summary>
    /// The only thread that mutates the registry. Hooks call <see cref="Enqueue"/> and <see cref="RecordError"/>
    /// from any thread; the plugin calls <see cref="Tick"/> from one dedicated low-priority worker.
    /// Every stage of a tick is isolated; a failing stage is counted and the next stage still runs.
    /// </summary>
    public sealed class MetricsWorker : IGaugeSink
    {
        sealed class GaugeSchedule
        {
            public readonly IGaugeSource Source;
            public readonly TimeSpan     Interval;
            public DateTime              NextDueUtc = DateTime.MinValue;
            public GaugeSchedule(IGaugeSource source, TimeSpan interval) { this.Source = source; this.Interval = interval; }
        }

        readonly ConcurrentQueue<EventRecord>       queue        = new();
        readonly MetricRegistry                      registry;
        readonly INameResolver                       resolver;
        readonly WorkerOptions                       options;
        readonly Action<string>                      logWarning;
        readonly Action<string, Exception>           logError;
        readonly List<GaugeSchedule>                 gaugeSources = new();
        readonly ConcurrentDictionary<string, long>  errors       = new(StringComparer.Ordinal);
        readonly ConcurrentDictionary<string, byte>  loggedErrors = new(StringComparer.Ordinal);

        long     queued;
        long     processedTotal;
        long     highWater;
        bool     warnedHighWater;
        DateTime? lastTickUtc;
        double   lastTickMs;
        volatile bool hasPublished;

        public MetricsWorker(MetricRegistry registry, INameResolver resolver, WorkerOptions options, Action<string> logWarning, Action<string, Exception> logError)
        {
            this.registry   = registry;
            this.resolver   = resolver;
            this.options    = options;
            this.logWarning = logWarning;
            this.logError   = logError;
        }

        public MetricRegistry Registry     => this.registry;
        public WorkerOptions  Options      => this.options;
        public bool           HasPublished => this.hasPublished;

        // ---- called from any thread ------------------------------------------------------------

        /// <summary>Lock-free, allocation-free apart from the record itself. Never blocks, never drops.</summary>
        public void Enqueue(EventRecord record)
        {
            this.queue.Enqueue(record);
            var n = Interlocked.Increment(ref this.queued);
            long hw;
            while (n > (hw = Volatile.Read(ref this.highWater)))
                if (Interlocked.CompareExchange(ref this.highWater, n, hw) == hw) break;
        }

        /// <summary>Counts an exception per stage and logs the first occurrence of each (stage, exception type).</summary>
        public void RecordError(string stage, Exception ex)
        {
            this.errors.AddOrUpdate(stage, 1, (_, v) => v + 1);
            if (this.loggedErrors.TryAdd(stage + ":" + ex.GetType().FullName, 0))
                this.logError($"[Metrics] {stage} failed: {ex.GetType().Name}: {ex.Message} (further occurrences are counted, not logged)", ex);
        }

        public void AddGaugeSource(IGaugeSource source, TimeSpan interval)
        {
            lock (this.gaugeSources) this.gaugeSources.Add(new GaugeSchedule(source, interval));
        }

        // ---- called from the worker thread only -----------------------------------------------

        public void Tick(DateTime nowUtc)
        {
            var sw = Stopwatch.StartNew();
            this.Stage("worker", () => this.Drain());
            this.Stage("gauge",  () => this.SampleGauges(nowUtc));
            this.Stage("worker", () => this.WriteSelfMetrics(nowUtc));
            this.Stage("worker", () => { this.registry.Publish(nowUtc); this.hasPublished = true; });
            sw.Stop();
            this.lastTickUtc = nowUtc;
            this.lastTickMs  = sw.Elapsed.TotalMilliseconds;
        }

        public WorkerStatus Snapshot() => new()
        {
            EventsQueued         = Volatile.Read(ref this.queued),
            EventsProcessedTotal = Volatile.Read(ref this.processedTotal),
            QueueHighWater       = Volatile.Read(ref this.highWater),
            HandlerErrorsTotal   = new Dictionary<string, long>(this.errors),
            LastTickUtc          = this.lastTickUtc,
            LastTickMilliseconds = this.lastTickMs,
            SeriesCount          = this.registry.SeriesCount,
            HasPublished         = this.hasPublished,
        };

        void Stage(string name, Action action)
        {
            try { action(); }
            catch (Exception ex) { this.RecordError(name, ex); }
        }

        void Drain()
        {
            var processed = 0L;
            while (this.queue.TryDequeue(out var record))
            {
                Interlocked.Decrement(ref this.queued);
                processed++;
                try { this.Apply(record); }
                catch (Exception ex) { this.RecordError("worker", ex); }
            }
            if (processed > 0) Interlocked.Add(ref this.processedTotal, processed);

            var hw = Volatile.Read(ref this.highWater);
            if (!this.warnedHighWater && hw >= this.options.QueueWarnThreshold)
            {
                this.warnedHighWater = true;
                this.logWarning($"[Metrics] event queue backlog reached {hw} (threshold {this.options.QueueWarnThreshold}); the worker is falling behind.");
            }
        }

        void Apply(EventRecord record)
        {
            foreach (var inc in record.Increments)
            {
                Label[] labels;
                if (inc.Deferred.Length == 0)
                {
                    labels = inc.Labels;
                }
                else
                {
                    labels = new Label[inc.Labels.Length + inc.Deferred.Length];
                    Array.Copy(inc.Labels, labels, inc.Labels.Length);
                    for (int i = 0; i < inc.Deferred.Length; i++)
                    {
                        var d = inc.Deferred[i];
                        var value = this.resolver.Resolve(d.Kind, d.Id) ?? $"{d.Kind.ToString().ToLowerInvariant()}_{d.Id}";
                        labels[inc.Labels.Length + i] = new Label(d.Name, this.Truncate(value));
                    }
                }
                this.registry.IncrementCounter(inc.Family, labels, inc.Delta);
            }
        }

        void SampleGauges(DateTime nowUtc)
        {
            GaugeSchedule[] due;
            lock (this.gaugeSources) due = this.gaugeSources.ToArray();
            foreach (var g in due)
            {
                if (nowUtc < g.NextDueUtc) continue;
                g.NextDueUtc = nowUtc + g.Interval;
                try { g.Source.Sample(this); }
                catch (Exception ex) { this.RecordError("gauge", ex); }
            }
        }

        void WriteSelfMetrics(DateTime nowUtc)
        {
            var r = this.registry;
            r.RegisterFamily("eco_exporter_events_queued",          MetricType.Gauge,   "Records in the event queue not yet drained.");
            r.RegisterFamily("eco_exporter_events_processed_total", MetricType.Counter, "Records drained and aggregated.");
            r.RegisterFamily("eco_exporter_queue_high_water",       MetricType.Gauge,   "Largest event queue length since start.");
            r.RegisterFamily("eco_exporter_handler_errors_total",   MetricType.Counter, "Exceptions caught, by stage.");
            r.RegisterFamily("eco_exporter_tick_duration_seconds",  MetricType.Gauge,   "Wall time of the previous worker tick.");
            r.RegisterFamily("eco_exporter_series",                 MetricType.Gauge,   "Series held in the registry.");
            r.RegisterFamily("eco_exporter_build_info",             MetricType.Gauge,   "Exporter version and Eco server version.");

            r.SetGauge("eco_exporter_events_queued",    CounterIncrement.NoLabels, Volatile.Read(ref this.queued));
            r.SetGauge("eco_exporter_queue_high_water", CounterIncrement.NoLabels, Volatile.Read(ref this.highWater));
            r.SetGauge("eco_exporter_tick_duration_seconds", CounterIncrement.NoLabels, this.lastTickMs / 1000.0);
            r.SetGauge("eco_exporter_series", CounterIncrement.NoLabels, r.SeriesCount);
            r.SetGauge("eco_exporter_build_info", new[] { new Label("eco_version", this.options.EcoVersion), new Label("version", this.options.ModVersion) }, 1);

            // Counters are set absolutely from our own totals: the registry only has Increment, so track what was written.
            this.SetCounterAbsolute("eco_exporter_events_processed_total", CounterIncrement.NoLabels, Volatile.Read(ref this.processedTotal));
            foreach (var kv in this.errors)
                this.SetCounterAbsolute("eco_exporter_handler_errors_total", new[] { new Label("stage", kv.Key) }, kv.Value);
        }

        readonly Dictionary<string, double> written = new(StringComparer.Ordinal);

        void SetCounterAbsolute(string family, Label[] labels, double total)
        {
            var key = SeriesKey.Build(family, labels);
            this.written.TryGetValue(key, out var prev);
            if (total > prev) { this.registry.IncrementCounter(family, labels, total - prev); this.written[key] = total; }
            else if (!this.written.ContainsKey(key)) { this.registry.IncrementCounter(family, labels, 0); this.written[key] = 0; }
        }

        string Truncate(string value) =>
            value.Length <= this.options.MaxLabelValueLength ? value : value.Substring(0, this.options.MaxLabelValueLength);

        // ---- IGaugeSink -----------------------------------------------------------------------

        void IGaugeSink.Set(string family, Label[] labels, double value)
        {
            for (int i = 0; i < labels.Length; i++)
                if (labels[i].Value.Length > this.options.MaxLabelValueLength)
                    labels[i] = new Label(labels[i].Name, this.Truncate(labels[i].Value));
            this.registry.SetGauge(family, labels, value);
        }

        void IGaugeSink.ClearFamily(string family) => this.registry.ClearGaugeFamily(family);
    }
}
