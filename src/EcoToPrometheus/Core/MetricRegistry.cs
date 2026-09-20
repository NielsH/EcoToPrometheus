namespace EcoToPrometheus.Core
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public sealed class FamilyInfo
    {
        public string     Name { get; }
        public MetricType Type { get; }
        public string     Help { get; internal set; }

        public FamilyInfo(string name, MetricType type, string help)
        {
            this.Name = name;
            this.Type = type;
            this.Help = help;
        }
    }

    public readonly record struct SeriesSnapshot(Label[] Labels, double Value);

    public sealed class FamilySnapshot
    {
        public string                        Name   { get; }
        public MetricType                    Type   { get; }
        public string                        Help   { get; }
        public IReadOnlyList<SeriesSnapshot> Series { get; }

        public FamilySnapshot(string name, MetricType type, string help, IReadOnlyList<SeriesSnapshot> series)
        {
            this.Name   = name;
            this.Type   = type;
            this.Help   = help;
            this.Series = series;
        }
    }

    /// <summary>Immutable view of every family and series, published by the worker and read by scrapes without locking.</summary>
    public sealed class MetricsSnapshot
    {
        public static readonly MetricsSnapshot Empty = new(DateTime.MinValue, Array.Empty<FamilySnapshot>());

        public DateTime                      GeneratedAtUtc { get; }
        public IReadOnlyList<FamilySnapshot> Families       { get; }
        public int                           SeriesCount    { get; }

        public MetricsSnapshot(DateTime generatedAtUtc, IReadOnlyList<FamilySnapshot> families)
        {
            this.GeneratedAtUtc = generatedAtUtc;
            this.Families       = families;
            this.SeriesCount    = families.Sum(f => f.Series.Count);
        }
    }

    /// <summary>
    /// Counters and gauges keyed by family + sorted label tuple. NOT thread-safe: only the worker thread mutates it.
    /// Readers use <see cref="Current"/>, an immutable snapshot swapped in by <see cref="Publish"/>.
    /// </summary>
    public sealed class MetricRegistry
    {
        sealed class Series
        {
            public readonly string  Family;
            public readonly Label[] Labels;
            public double           Value;
            public Series(string family, Label[] labels, double value) { this.Family = family; this.Labels = labels; this.Value = value; }
        }

        readonly Dictionary<string, FamilyInfo> families = new(StringComparer.Ordinal);
        readonly Dictionary<string, Series>     counters = new(StringComparer.Ordinal);
        readonly Dictionary<string, Series>     gauges   = new(StringComparer.Ordinal);
        volatile MetricsSnapshot                current  = MetricsSnapshot.Empty;
        bool                                    dirty    = true;

        public MetricsSnapshot Current     => this.current;
        public int             SeriesCount => this.counters.Count + this.gauges.Count;

        /// <summary>Idempotent. The first registration wins for the type; a later non-empty help text replaces an empty one.</summary>
        public FamilyInfo RegisterFamily(string name, MetricType type, string help)
        {
            if (this.families.TryGetValue(name, out var existing))
            {
                if (existing.Type != type)
                    throw new InvalidOperationException($"Metric family '{name}' registered as {existing.Type} and again as {type}.");
                if (existing.Help.Length == 0 && help.Length > 0) { existing.Help = help; this.dirty = true; }
                return existing;
            }
            var info = new FamilyInfo(name, type, help);
            this.families[name] = info;
            this.dirty = true;
            return info;
        }

        public bool TryGetFamily(string name, out FamilyInfo info) => this.families.TryGetValue(name, out info!);

        /// <summary>Adds <paramref name="delta"/> to the series, creating it at <paramref name="delta"/> if new. Labels are copied and sorted.</summary>
        public void IncrementCounter(string family, Label[] labels, double delta)
        {
            var info = this.RegisterFamily(family, MetricType.Counter, string.Empty);
            if (info.Type != MetricType.Counter) throw new InvalidOperationException($"'{family}' is a gauge.");
            var sorted = Sorted(labels);
            var key = SeriesKey.Build(family, sorted);
            if (this.counters.TryGetValue(key, out var s)) s.Value += delta;
            else this.counters[key] = new Series(family, sorted, delta);
            this.dirty = true;
        }

        public void SetGauge(string family, Label[] labels, double value)
        {
            var info = this.RegisterFamily(family, MetricType.Gauge, string.Empty);
            if (info.Type != MetricType.Gauge) throw new InvalidOperationException($"'{family}' is a counter.");
            var sorted = Sorted(labels);
            var key = SeriesKey.Build(family, sorted);
            if (this.gauges.TryGetValue(key, out var s)) { if (s.Value != value) { s.Value = value; this.dirty = true; } }
            else { this.gauges[key] = new Series(family, sorted, value); this.dirty = true; }
        }

        public void ClearGaugeFamily(string family)
        {
            var keys = this.gauges.Where(kv => kv.Value.Family == family).Select(kv => kv.Key).ToList();
            foreach (var k in keys) this.gauges.Remove(k);
            if (keys.Count > 0) this.dirty = true;
        }

        /// <summary>Rebuilds <see cref="Current"/> if anything changed since the last publish. Families and series come out sorted.</summary>
        public MetricsSnapshot Publish(DateTime nowUtc)
        {
            if (!this.dirty) return this.current;
            var byFamily = new Dictionary<string, List<SeriesSnapshot>>(StringComparer.Ordinal);
            foreach (var s in this.counters.Values.Concat(this.gauges.Values))
            {
                if (!byFamily.TryGetValue(s.Family, out var list)) byFamily[s.Family] = list = new List<SeriesSnapshot>();
                list.Add(new SeriesSnapshot(s.Labels, s.Value));
            }
            var result = new List<FamilySnapshot>(byFamily.Count);
            foreach (var info in this.families.Values.OrderBy(f => f.Name, StringComparer.Ordinal))
            {
                if (!byFamily.TryGetValue(info.Name, out var list)) continue; // registered but no series yet
                list.Sort(CompareSeries);
                result.Add(new FamilySnapshot(info.Name, info.Type, info.Help, list));
            }
            this.current = new MetricsSnapshot(nowUtc, result);
            this.dirty   = false;
            return this.current;
        }

        /// <summary>Counter totals as series keys, for the state file.</summary>
        public IEnumerable<KeyValuePair<string, double>> ExportCounters() =>
            this.counters.Select(kv => new KeyValuePair<string, double>(kv.Key, kv.Value.Value));

        /// <summary>Replaces counter totals from the state file. Families are registered as counters with empty help; hooks fill in help later.</summary>
        public void ImportCounters(IEnumerable<KeyValuePair<string, double>> entries)
        {
            foreach (var (key, value) in entries)
            {
                var (family, labels) = SeriesKey.Parse(key);
                this.RegisterFamily(family, MetricType.Counter, string.Empty);
                this.counters[key] = new Series(family, labels, value);
            }
            this.dirty = true;
        }

        public void ResetCounters()
        {
            this.counters.Clear();
            this.dirty = true;
        }

        static Label[] Sorted(Label[] labels)
        {
            if (labels.Length <= 1) return labels;
            var copy = (Label[])labels.Clone();
            Array.Sort(copy);
            return copy;
        }

        static int CompareSeries(SeriesSnapshot a, SeriesSnapshot b)
        {
            var n = Math.Min(a.Labels.Length, b.Labels.Length);
            for (int i = 0; i < n; i++)
            {
                var c = a.Labels[i].CompareTo(b.Labels[i]);
                if (c != 0) return c;
            }
            return a.Labels.Length.CompareTo(b.Labels.Length);
        }
    }
}
