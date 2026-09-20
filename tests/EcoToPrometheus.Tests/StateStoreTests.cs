namespace EcoToPrometheus.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using EcoToPrometheus.Core;
    using Xunit;

    public class StateStoreTests : IDisposable
    {
        readonly TempDir dir = new();
        readonly string  path;

        public StateStoreTests() { this.path = this.dir.File("World.metrics.json"); }

        public void Dispose() => this.dir.Dispose();

        string[] Siblings() => Directory.GetFiles(this.dir.Path).Select(Path.GetFileName).OrderBy(n => n, StringComparer.Ordinal).ToArray()!;

        static StateFile Sample() => new()
        {
            WorldSeconds = 123456.789,
            Counters = new Dictionary<string, double>(StringComparer.Ordinal)
            {
                ["eco_action_chop_tree_total|player=Zoë 玩家|species=Oak"] = 42,
                ["eco_x_total|player=a\\|b\\=c\\\\d|q=\"quoted\"\nline"]    = 0.1 + 0.2,
                ["eco_big_total"]                                             = 1e300,
                ["eco_tiny_total"]                                            = double.Epsilon,
                ["eco_max_total"]                                             = double.MaxValue,
                ["eco_pi_total"]                                              = Math.PI,
                ["eco_neg_total"]                                             = -123456789.123456789,
                ["eco_nan_total"]                                             = double.NaN,
                ["eco_inf_total"]                                             = double.PositiveInfinity,
            },
            Self = new SelfState { EventsProcessedTotal = 99, QueueHighWater = 7 },
        };

        [Fact]
        public void Save_then_Load_round_trips_bit_exact()
        {
            var before = DateTime.UtcNow.AddSeconds(-1);
            var state = Sample();
            StateStore.Save(this.path, state);
            Assert.True(File.Exists(this.path));
            Assert.Equal(new[] { "World.metrics.json" }, this.Siblings());
            Assert.True(state.SavedAtUtc >= before);

            var loaded = StateStore.Load(this.path, out var result, out var message);
            Assert.Equal(StateLoadResult.Loaded, result);
            Assert.Null(message);
            Assert.NotNull(loaded);
            Assert.Equal(1, loaded.Version);
            Assert.Equal(state.SavedAtUtc, loaded.SavedAtUtc);
            Assert.Equal(DateTimeKind.Utc, loaded.SavedAtUtc.Kind);
            Assert.Equal(123456.789, loaded.WorldSeconds);
            Assert.Equal(99, loaded.Self.EventsProcessedTotal);
            Assert.Equal(7, loaded.Self.QueueHighWater);
            Assert.Equal(state.Counters.Count, loaded.Counters.Count);
            foreach (var (key, value) in state.Counters)
            {
                Assert.True(loaded.Counters.TryGetValue(key, out var got), $"missing key {key}");
                Assert.Equal(BitConverter.DoubleToInt64Bits(value), BitConverter.DoubleToInt64Bits(got));
            }
        }

        [Fact]
        public void Saved_keys_are_valid_series_keys_after_reload()
        {
            StateStore.Save(this.path, Sample());
            var loaded = StateStore.Load(this.path, out _, out _)!;
            var (family, labels) = SeriesKey.Parse(loaded.Counters.Keys.Single(k => k.StartsWith("eco_x_total", StringComparison.Ordinal)));
            Assert.Equal("eco_x_total", family);
            Assert.Equal(new[] { new Label("player", "a|b=c\\d"), new Label("q", "\"quoted\"\nline") }, labels);
        }

        [Fact]
        public void Missing_file_is_Missing()
        {
            var loaded = StateStore.Load(this.path, out var result, out var message);
            Assert.Null(loaded);
            Assert.Equal(StateLoadResult.Missing, result);
            Assert.Null(message);
            Assert.Empty(this.Siblings());
        }

        [Fact]
        public void Corrupt_json_is_Corrupt_and_renamed()
        {
            File.WriteAllText(this.path, "{ \"Version\": 1, \"Counters\": { not json");
            var loaded = StateStore.Load(this.path, out var result, out var message);
            Assert.Null(loaded);
            Assert.Equal(StateLoadResult.Corrupt, result);
            Assert.NotNull(message);
            Assert.False(File.Exists(this.path));
            var sibling = Assert.Single(this.Siblings());
            Assert.Matches(new Regex(@"^World\.metrics\.json\.corrupt-\d{8}-\d{6}$"), sibling);
            Assert.Contains(sibling, message);

            // the next save starts clean: no .bak, no .tmp
            StateStore.Save(this.path, Sample());
            Assert.Equal(new[] { "World.metrics.json", sibling }, this.Siblings());
        }

        [Fact]
        public void Json_null_and_wrong_type_are_Corrupt()
        {
            File.WriteAllText(this.path, "null");
            Assert.Null(StateStore.Load(this.path, out var result, out _));
            Assert.Equal(StateLoadResult.Corrupt, result);

            File.WriteAllText(this.path, "{ \"Version\": \"one\" }");
            Assert.Null(StateStore.Load(this.path, out result, out _));
            Assert.Equal(StateLoadResult.Corrupt, result);
            Assert.Equal(2, this.Siblings().Count(n => n.Contains(".corrupt-", StringComparison.Ordinal)));
        }

        [Fact]
        public void Newer_version_is_NewerVersion_and_renamed()
        {
            File.WriteAllText(this.path, "{ \"Version\": 2, \"WorldSeconds\": 1, \"Counters\": {}, \"Self\": {} }");
            var loaded = StateStore.Load(this.path, out var result, out var message);
            Assert.Null(loaded);
            Assert.Equal(StateLoadResult.NewerVersion, result);
            Assert.NotNull(message);
            Assert.Contains("version 2", message);
            Assert.False(File.Exists(this.path));
            Assert.Equal(new[] { "World.metrics.json.v2.old" }, this.Siblings());
        }

        [Fact]
        public void Minimal_and_lenient_files_load()
        {
            File.WriteAllText(this.path, "{}");
            var loaded = StateStore.Load(this.path, out var result, out _);
            Assert.Equal(StateLoadResult.Loaded, result);
            Assert.NotNull(loaded);
            Assert.Equal(1, loaded.Version);
            Assert.Empty(loaded.Counters);
            Assert.NotNull(loaded.Self);

            File.WriteAllText(this.path, "{ \"version\": 1, \"counters\": { \"a\": 1, }, // comment\n \"self\": null }");
            loaded = StateStore.Load(this.path, out result, out _);
            Assert.Equal(StateLoadResult.Loaded, result);
            Assert.Equal(1, loaded!.Counters["a"]);
            Assert.NotNull(loaded.Self);
        }

        [Fact]
        public void Save_twice_leaves_exactly_one_bak_and_no_tmp()
        {
            var s = Sample();
            StateStore.Save(this.path, s);
            s.Counters["eco_big_total"] = 2;
            StateStore.Save(this.path, s);
            Assert.Equal(new[] { "World.metrics.json", "World.metrics.json.bak" }, this.Siblings());
            s.Counters["eco_big_total"] = 3;
            StateStore.Save(this.path, s);
            Assert.Equal(new[] { "World.metrics.json", "World.metrics.json.bak" }, this.Siblings());

            Assert.Equal(3, StateStore.Load(this.path, out _, out _)!.Counters["eco_big_total"]);
            Assert.Equal(2, StateStore.Load(this.path + ".bak", out _, out _)!.Counters["eco_big_total"]);
        }

        [Fact]
        public void Save_creates_missing_directory()
        {
            var nested = this.dir.File(Path.Combine("Storage", "deeper", "W.metrics.json"));
            StateStore.Save(nested, Sample());
            Assert.True(File.Exists(nested));
            Assert.Equal(StateLoadResult.Loaded, Load(nested));
        }

        [Fact]
        public void ArchiveForNewWorld_renames_and_returns_the_new_name()
        {
            StateStore.Save(this.path, Sample());
            var archived = StateStore.ArchiveForNewWorld(this.path);
            Assert.False(File.Exists(this.path));
            Assert.True(File.Exists(archived));
            Assert.Matches(new Regex(@"^World\.metrics\.json\.\d{8}-\d{6}\.old$"), Path.GetFileName(archived));
            Assert.Equal(new[] { Path.GetFileName(archived) }, this.Siblings());

            // a second archive within the same second must not collide
            StateStore.Save(this.path, Sample());
            var archived2 = StateStore.ArchiveForNewWorld(this.path);
            Assert.NotEqual(archived, archived2);
            Assert.True(File.Exists(archived2));
            Assert.Equal(StateLoadResult.Missing, Load(this.path));
        }

        static StateLoadResult Load(string path)
        {
            StateStore.Load(path, out var result, out _);
            return result;
        }

        [Fact]
        public void PruneUnknownFamilies_drops_only_unknown_and_reports_them_sorted()
        {
            var state = new StateFile();
            state.Counters["eco_action_chop_tree_total|player=Ann|species=Oak"] = 5;
            state.Counters["eco_action_chop_tree_total|player=Bob|species=Oak"] = 7;
            state.Counters["eco_old_renamed_total|player=Ann"]                 = 3;
            state.Counters["eco_gone_total"]                                    = 1;
            state.Counters["eco_exporter_events_processed_total"]               = 42;
            var known = new HashSet<string> { "eco_action_chop_tree_total", "eco_exporter_events_processed_total" };

            var dropped = StateStore.PruneUnknownFamilies(state, known.Contains);

            Assert.Equal(new[] { "eco_gone_total", "eco_old_renamed_total" }, dropped);
            Assert.Equal(3, state.Counters.Count);
            Assert.Equal(5, state.Counters["eco_action_chop_tree_total|player=Ann|species=Oak"]);
            Assert.Equal(42, state.Counters["eco_exporter_events_processed_total"]);
        }
    }
}
