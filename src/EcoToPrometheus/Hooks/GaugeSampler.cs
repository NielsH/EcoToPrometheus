namespace EcoToPrometheus.Hooks
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.ExceptionServices;
    using Eco.Core.Plugins;
    using Eco.Core.Systems;
    using Eco.Gameplay.Civics.Laws;
    using Eco.Gameplay.Civics.Misc;
    using Eco.Gameplay.Civics.Titles;
    using Eco.Gameplay.Economy;
    using Eco.Gameplay.Objects;
    using Eco.Gameplay.Players;
    using Eco.Gameplay.Settlements;
    using Eco.Gameplay.Stats;
    using Eco.Gameplay.Systems;
    using Eco.Shared;
    using Eco.Shared.IoC;
    using Eco.Shared.Voxel;
    using Eco.Simulation;
    using Eco.Simulation.Time;
    using Eco.Simulation.Types;
    using Eco.Simulation.WorldLayers;
    using EcoToPrometheus.Core;

    /// <summary>Gauge group names. The first four are config-selectable; <see cref="Exporter"/> is always on.</summary>
    public static class GaugeGroups
    {
        public const string Global   = "Global";
        public const string Species  = "Species";
        public const string Climate  = "Climate";
        public const string Live     = "Live";
        public const string Exporter = "Exporter";
    }

    /// <summary>Shared plumbing: per-gauge isolation and HELP registration through the worker's registry.</summary>
    static class GaugeUtil
    {
        static readonly Label[] NoLabels = CounterIncrement.NoLabels;

        /// <summary>Runs one reader; the first failure is remembered and rethrown after the rest ran, so one bad registrar does not blank the group.</summary>
        public static void Guard(ref Exception? first, Action reader)
        {
            try { reader(); }
            catch (Exception ex) { first ??= ex; }
        }

        public static void Rethrow(Exception? first) { if (first != null) ExceptionDispatchInfo.Capture(first).Throw(); }

        public static void Help(IGaugeSink sink, string family, MetricType type, string help)
        {
            if (sink is MetricsWorker worker) worker.Registry.RegisterFamily(family, type, help);
        }

        public static void Set(IGaugeSink sink, string family, double value) => sink.Set(family, NoLabels, value);
    }

    // ---- sim cadence: Species / Climate / Global -------------------------------------------------------

    /// <summary>Copy of what Eco just wrote to its own stats. Built inside the EcoSim tick; read by the worker.</summary>
    public sealed class SimSnapshot
    {
        public readonly int[]   Populations; // parallel to SimStatsCollector.SpeciesInfos
        public readonly float[] Climate;     // 11 values, order of SimStatsCollector.ClimateFamilies
        public readonly float[] Global;      // 28 values, order of SimStatsCollector.GlobalFamilies
        public readonly bool    HasGlobal;
        public readonly double  WorldSeconds;

        public SimSnapshot(int[] populations, float[] climate, float[] global, bool hasGlobal, double worldSeconds)
        {
            this.Populations  = populations;
            this.Climate      = climate;
            this.Global       = global;
            this.HasGlobal    = hasGlobal;
            this.WorldSeconds = worldSeconds;
        }
    }

    public readonly record struct SpeciesInfo(string Label, string Kind);

    /// <summary>
    /// Subscribes <c>SimStats.OnStatsCollected</c> (fires every 10 game minutes, inside the sim tick under SimStats.LockObj).
    /// The handler only copies floats into a fresh <see cref="SimSnapshot"/>; it never allocates beyond that and never locks.
    /// </summary>
    public sealed class SimStatsCollector
    {
        public static readonly (string Family, string[] Labels, string Help)[] ClimateFamilies =
        {
            ("eco_climate_co2_ppm",                         Array.Empty<string>(),                      "Total atmospheric CO2 [Eco stat TotalCO2, unit PPM]"),
            ("eco_climate_co2_lifetime_ppm",                new[] { "source", "pollution" },            "Lifetime CO2 by source [Eco stats LifetimeCO2From*, unit PPM]"),
            ("eco_climate_co2_lifetime_ppm",                new[] { "source", "animals" },              string.Empty),
            ("eco_climate_co2_lifetime_ppm",                new[] { "source", "plants" },               string.Empty),
            ("eco_climate_ground_pollution_ppm",            new[] { "type", "total" },                  "Ground pollution by type [Eco stats Total*Pollution, unit PPM]"),
            ("eco_climate_ground_pollution_ppm",            new[] { "type", "soil" },                   string.Empty),
            ("eco_climate_ground_pollution_ppm",            new[] { "type", "heavy_mineral" },          string.Empty),
            ("eco_climate_ground_pollution_ppm",            new[] { "type", "chemical" },               string.Empty),
            ("eco_climate_ground_pollution_ppm",            new[] { "type", "acid_rain" },              string.Empty),
            ("eco_climate_sea_level_meters",                Array.Empty<string>(),                      "Sea level [Eco stat SeaLevel, unit Meters]"),
            ("eco_climate_average_temperature_celsius",     Array.Empty<string>(),                      "Average global temperature [Eco stat AverageGlobalTemperature, unit Celsius]"),
        };

        public static readonly (string Family, string[] Labels, string Help)[] GlobalFamilies =
        {
            ("eco_citizens",                                Array.Empty<string>(),      "Citizens registered on the server. [Eco stat CitizenPopulation]"),
            ("eco_citizens_active",                         Array.Empty<string>(),      "Active citizens, including online-but-not-yet-active. [Eco stat ActiveCitizenPopulation]"),
            ("eco_citizens_active_median_house_value",      Array.Empty<string>(),      "Median house value of active citizens. [Eco stat MedianHouseValueOfActiveCitizens]"),
            ("eco_citizens_active_median_nutrition",        Array.Empty<string>(),      "Median nutrition value of active citizens. [Eco stat MedianNutritionValueOfActiveCitizens]"),
            ("eco_citizens_active_median_skill_rate",       Array.Empty<string>(),      "Median skill rate of active citizens. [Eco stat MedianSkillRateOfActiveCitizens]"),
            ("eco_citizens_active_median_unlocked_skills",  Array.Empty<string>(),      "Median unlocked specialties of active citizens. [Eco stat MedianUnlockedSkillsOfActiveCitizens]"),
            ("eco_citizens_active_median_specialty_percent", Array.Empty<string>(),     "Median specialty completion of active citizens. [Eco stat MedianSpecialtyPercentOfActiveCitizens]"),
            ("eco_population",                              new[] { "kind", "animal" }, "Total organisms by kind. [Eco stats AnimalPopulation, TreePopulation, PlantPopulation]"),
            ("eco_population",                              new[] { "kind", "tree" },   string.Empty),
            ("eco_population",                              new[] { "kind", "plant" },  string.Empty),
            ("eco_species_extinct",                         Array.Empty<string>(),      "Species with zero population. [Eco stat ExtinctSpecies]"),
            ("eco_laws_active",                             Array.Empty<string>(),      "Active laws. [Eco stat Laws; also refreshed by the slow live reader]"),
            ("eco_elected_titles",                          Array.Empty<string>(),      "Elected titles defined. [Eco stat ElectedTitles]"),
            ("eco_civic_elements_active",                   Array.Empty<string>(),      "Active civic objects. [Eco stat ActiveCivicElements]"),
            ("eco_elected_officials_active",                Array.Empty<string>(),      "Elected officials who are active players. [Eco stat ActiveElectedOfficials]"),
            ("eco_wealth_personal",                         Array.Empty<string>(),      "Personal wealth in the default currency. [Eco stat PersonalWealthInDefaultCurrency]"),
            ("eco_wealth_government",                       Array.Empty<string>(),      "Government holdings in the default currency. [Eco stat GovernmentHoldingsInDefaultCurrency]"),
            ("eco_trades_last_7d",                          Array.Empty<string>(),      "Trade volume in the last 7 days. [Eco stat TradesInLast7Days]"),
            ("eco_currencies_active",                       Array.Empty<string>(),      "Currencies with circulation. [Eco stat ActiveCurrencies]"),
            ("eco_taxed_transactions",                      Array.Empty<string>(),      "Taxed transactions. [Eco stat TaxedTransactions]"),
            ("eco_gdp",                                     Array.Empty<string>(),      "GDP in the default currency. [Eco stat GDP]"),
            ("eco_debt",                                    Array.Empty<string>(),      "Total debt in the default currency. [Eco stat Debt]"),
            ("eco_debt_per_active_citizen",                 Array.Empty<string>(),      "Debt per active citizen. [Eco stat DebtPerActiveCitizen]"),
            ("eco_store_item_types_for_sale",               Array.Empty<string>(),      "Item types listed for sale. [Eco stat ItemTypesForSale]"),
            ("eco_store_items_for_sale",                    Array.Empty<string>(),      "Items listed for sale. [Eco stat ItemsForSale]"),
            ("eco_stores_active",                           Array.Empty<string>(),      "Active stores. [Eco stat ActiveStores]"),
            ("eco_contracts_active",                        Array.Empty<string>(),      "Active contracts and work parties. [Eco stat ActiveContractsAndWorkParties]"),
            ("eco_contracts_completed_last_7d",             Array.Empty<string>(),      "Contracts and work parties completed in the last 7 days. [Eco stat CompletedContractsAndWorkPartiesLast7Days]"),
        };

        public const string SpeciesPopulation  = "eco_species_population";
        public const string LastCollection     = "eco_sim_stats_last_collection_seconds";

        readonly Action     handler;
        SpeciesInfo[]?      speciesInfos;   // fixed after the first collection: EcoSim.AllSpecies does not change after load
        Species[]?          speciesList;
        volatile SimSnapshot? latest;
        Exception?          lastError;
        bool                subscribed;

        public SimStatsCollector() => this.handler = this.OnStatsCollected;

        public SimSnapshot?   Latest       => this.latest;
        public SpeciesInfo[]? SpeciesInfos => this.speciesInfos;

        /// <summary>Set by the handler when a collection failed; the exporter source reports and clears it on the worker thread.</summary>
        public Exception? TakeError() { var e = this.lastError; this.lastError = null; return e; }

        public void Subscribe()
        {
            if (this.subscribed) return;
            this.subscribed = true;
            SimStats.OnStatsCollected += this.handler;
        }

        public void Unsubscribe()
        {
            if (!this.subscribed) return;
            this.subscribed = false;
            SimStats.OnStatsCollected -= this.handler;
        }

        void OnStatsCollected()
        {
            try
            {
                if (this.speciesList == null)
                {
                    var all   = EcoSim.AllSpecies.ToArray();
                    var infos = new SpeciesInfo[all.Length];
                    for (int i = 0; i < all.Length; i++)
                        infos[i] = new SpeciesInfo(MetricNames.TypeLabel(all[i].GetType().Name, "Species"), all[i] is TreeSpecies ? "tree" : all[i] is PlantSpecies ? "plant" : all[i] is AnimalSpecies ? "animal" : "other");
                    this.speciesInfos = infos;
                    this.speciesList  = all;
                }

                var species = this.speciesList;
                var pops    = new int[species.Length];
                for (int i = 0; i < species.Length; i++) pops[i] = species[i].Layer?.RoundedTotal ?? 0;

                var s       = WorldLayerManager.Obj.ClimateSim.State;
                var climate = new[]
                {
                    s.TotalCO2, s.LifetimeCO2FromPollution, s.LifetimeCO2FromAnimals, s.LifetimeCO2FromPlants,
                    s.TotalGroundPollution, s.TotalSoilPollution, s.TotalHeavyMineralPollution, s.TotalChemicalPollution, s.TotalAcidRainPollution,
                    s.SeaLevel, s.AverageGlobalTemperature,
                };

                var g      = StatGameplaySettings.LatestGlobalStats;
                var global = g == null ? Array.Empty<float>() : new[]
                {
                    g.CitizenPopulation, g.ActiveCitizenPopulation, g.MedianHouseValueOfActiveCitizens, g.MedianNutritionValueOfActiveCitizens,
                    g.MedianSkillRateOfActiveCitizens, g.MedianUnlockedSkillsOfActiveCitizens, g.MedianSpecialtyPercentOfActiveCitizens,
                    g.AnimalPopulation, g.TreePopulation, g.PlantPopulation, g.ExtinctSpecies,
                    g.Laws, g.ElectedTitles, g.ActiveCivicElements, g.ActiveElectedOfficials,
                    g.PersonalWealthInDefaultCurrency, g.GovernmentHoldingsInDefaultCurrency, g.TradesInLast7Days, g.ActiveCurrencies, g.TaxedTransactions,
                    g.GDP, g.Debt, g.DebtPerActiveCitizen,
                    g.ItemTypesForSale, g.ItemsForSale, g.ActiveStores, g.ActiveContractsAndWorkParties, g.CompletedContractsAndWorkPartiesLast7Days,
                };

                this.latest = new SimSnapshot(pops, climate, global, g != null, WorldTime.Seconds);
            }
            catch (Exception ex) { this.lastError = ex; }
        }
    }

    /// <summary>One of the three sim-cadence groups; writes the collector's latest snapshot when it changed.</summary>
    public sealed class SimStatsSource : IGaugeSource
    {
        readonly SimStatsCollector collector;
        SimSnapshot?               written;
        bool                       helpRegistered;

        public SimStatsSource(SimStatsCollector collector, string group) { this.collector = collector; this.Group = group; }

        public string Group { get; }

        public void Sample(IGaugeSink sink)
        {
            var snap = this.collector.Latest;
            if (snap == null || ReferenceEquals(snap, this.written)) return; // nothing collected yet, or already written
            if (!this.helpRegistered) { this.RegisterHelp(sink); this.helpRegistered = true; }

            Exception? first = null;
            switch (this.Group)
            {
                case GaugeGroups.Species: this.WriteSpecies(sink, snap, ref first); break;
                case GaugeGroups.Climate: WriteTable(sink, SimStatsCollector.ClimateFamilies, snap.Climate, ref first); break;
                case GaugeGroups.Global:  if (snap.HasGlobal) WriteTable(sink, SimStatsCollector.GlobalFamilies, snap.Global, ref first); break;
            }
            GaugeUtil.Guard(ref first, () => GaugeUtil.Set(sink, SimStatsCollector.LastCollection, snap.WorldSeconds));
            this.written = snap;
            GaugeUtil.Rethrow(first);
        }

        void RegisterHelp(IGaugeSink sink)
        {
            GaugeUtil.Help(sink, SimStatsCollector.LastCollection, MetricType.Gauge, "World time of the last SimStats collection (every 10 game minutes).");
            switch (this.Group)
            {
                case GaugeGroups.Species: GaugeUtil.Help(sink, SimStatsCollector.SpeciesPopulation, MetricType.Gauge, "Population per species [Eco stat <Name>Species, unit Organisms]"); break;
                case GaugeGroups.Climate: foreach (var f in SimStatsCollector.ClimateFamilies) if (f.Help.Length > 0) GaugeUtil.Help(sink, f.Family, MetricType.Gauge, f.Help); break;
                case GaugeGroups.Global:  foreach (var f in SimStatsCollector.GlobalFamilies)  if (f.Help.Length > 0) GaugeUtil.Help(sink, f.Family, MetricType.Gauge, f.Help); break;
            }
        }

        void WriteSpecies(IGaugeSink sink, SimSnapshot snap, ref Exception? first)
        {
            var infos = this.collector.SpeciesInfos;
            if (infos == null) return;
            var n = Math.Min(infos.Length, snap.Populations.Length);
            for (int i = 0; i < n; i++)
            {
                var info = infos[i];
                var pop  = snap.Populations[i];
                GaugeUtil.Guard(ref first, () => sink.Set(SimStatsCollector.SpeciesPopulation, new[] { new Label("kind", info.Kind), new Label("species", info.Label) }, pop));
            }
        }

        static void WriteTable(IGaugeSink sink, (string Family, string[] Labels, string Help)[] table, float[] values, ref Exception? first)
        {
            var n = Math.Min(table.Length, values.Length);
            for (int i = 0; i < n; i++)
            {
                var row   = table[i];
                var value = values[i];
                var labels = row.Labels.Length == 0 ? CounterIncrement.NoLabels : new[] { new Label(row.Labels[0], row.Labels[1]) };
                GaugeUtil.Guard(ref first, () => sink.Set(row.Family, labels, value));
            }
        }
    }

    // ---- worker cadence: Live -------------------------------------------------------------------------

    /// <summary>Cheap O(1) reads every worker tick, plus <c>eco_world_info</c> once.</summary>
    public sealed class LiveSource : IGaugeSource
    {
        readonly Func<double> uptimeSeconds;
        bool                  infoWritten;

        public LiveSource(Func<double> uptimeSeconds) => this.uptimeSeconds = uptimeSeconds;

        public string Group => GaugeGroups.Live;

        public void Sample(IGaugeSink sink)
        {
            Exception? first = null;
            if (!this.infoWritten)
            {
                GaugeUtil.Help(sink, "eco_players_online",         MetricType.Gauge, "Players currently connected.");
                GaugeUtil.Help(sink, "eco_world_time_seconds",     MetricType.Gauge, "In-game world clock in seconds.");
                GaugeUtil.Help(sink, "eco_world_day",              MetricType.Gauge, "In-game world clock in days.");
                GaugeUtil.Help(sink, "eco_server_uptime_seconds",  MetricType.Gauge, "Seconds since the metrics plugin initialised.");
                GaugeUtil.Help(sink, "eco_world_info",             MetricType.Gauge, "Save name, world size and Eco version; always 1.");
                GaugeUtil.Guard(ref first, () =>
                {
                    var size = World.VoxelSize;
                    sink.Set("eco_world_info", new[]
                    {
                        new Label("eco_version", EcoVersion.Version),
                        new Label("save",        StorageManager.SaveName ?? string.Empty),
                        new Label("size_x",      size.x.ToString()),
                        new Label("size_y",      size.y.ToString()),
                        new Label("size_z",      size.z.ToString()),
                    }, 1);
                    this.infoWritten = true;
                });
            }
            GaugeUtil.Guard(ref first, () => GaugeUtil.Set(sink, "eco_players_online",        UserManager.Obj.OnlineUserCount));
            GaugeUtil.Guard(ref first, () => GaugeUtil.Set(sink, "eco_world_time_seconds",    WorldTime.Seconds));
            GaugeUtil.Guard(ref first, () => GaugeUtil.Set(sink, "eco_world_day",             WorldTime.Day));
            GaugeUtil.Guard(ref first, () => GaugeUtil.Set(sink, "eco_server_uptime_seconds", this.uptimeSeconds()));
            GaugeUtil.Rethrow(first);
        }
    }

    /// <summary>Exporter self-metrics owned by the hooks (always registered): listener state, filtered events, HELP for the rule set.</summary>
    public sealed class ExporterSource : IGaugeSource
    {
        readonly ActionListener    listener;
        readonly Func<bool>        listenerAttached;
        readonly SimStatsCollector collector;
        readonly Dictionary<string, long> written = new(StringComparer.Ordinal);
        FamilyRuleSet?             helpRegisteredFor;

        public ExporterSource(ActionListener listener, Func<bool> listenerAttached, SimStatsCollector collector)
        {
            this.listener         = listener;
            this.listenerAttached = listenerAttached;
            this.collector        = collector;
        }

        public string Group => GaugeGroups.Exporter;

        public void Sample(IGaugeSink sink)
        {
            if (sink is not MetricsWorker worker) return;
            var registry = worker.Registry;
            var rules    = this.listener.Rules;
            if (!ReferenceEquals(rules, this.helpRegisteredFor))
            {
                rules.RegisterHelp(registry);
                this.helpRegisteredFor = rules;
            }
            registry.RegisterFamily("eco_exporter_listener_attached",     MetricType.Gauge,   "1 while the GameAction listener is registered, 0 after detach or Enabled=false.");
            registry.RegisterFamily("eco_exporter_events_filtered_total", MetricType.Counter, "Actions seen by the listener and skipped to mirror Eco's own stats filter.");

            GaugeUtil.Set(sink, "eco_exporter_listener_attached", this.listenerAttached() ? 1 : 0);
            this.CounterAbsolute(registry, "eco_exporter_events_filtered_total", "nostats",     this.listener.FilteredNoStats);
            this.CounterAbsolute(registry, "eco_exporter_events_filtered_total", "conditional", this.listener.FilteredConditional);

            var error = this.collector.TakeError();
            if (error != null) worker.RecordError("gauge", error);
        }

        // The registry only increments; track what was written so the counter can be set from an absolute total.
        void CounterAbsolute(MetricRegistry registry, string family, string reason, long total)
        {
            var labels = new[] { new Label("reason", reason) };
            var key    = family + "|" + reason;
            this.written.TryGetValue(key, out var prev);
            if (total > prev || !this.written.ContainsKey(key))
            {
                registry.IncrementCounter(family, labels, total - prev);
                this.written[key] = total;
            }
        }
    }

    // ---- slow cadence: registrar-enumerating gauges ---------------------------------------------------

    /// <summary>Gauges that walk a registrar; sampled every SlowGaugeIntervalSeconds.</summary>
    public sealed class SlowSource : IGaugeSource
    {
        const string Circulation = "eco_currency_circulation";
        const string Settlements = "eco_settlements";
        bool         helpRegistered;

        public string Group => GaugeGroups.Live;

        public void Sample(IGaugeSink sink)
        {
            if (!this.helpRegistered)
            {
                GaugeUtil.Help(sink, "eco_players_total",         MetricType.Gauge, "Users known to the server.");
                GaugeUtil.Help(sink, Circulation,                 MetricType.Gauge, "Currency in circulation, by currency and type (Backed or Credit).");
                GaugeUtil.Help(sink, Settlements,                 MetricType.Gauge, "Settlements by founded state and type.");
                GaugeUtil.Help(sink, "eco_laws_active",           MetricType.Gauge, "Active laws.");
                GaugeUtil.Help(sink, "eco_elected_titles_active", MetricType.Gauge, "Active elected titles.");
                this.helpRegistered = true;
            }

            Exception? first = null;
            GaugeUtil.Guard(ref first, () =>
            {
                GaugeUtil.Set(sink, "eco_players_total", UserManager.Obj.TotalUserCount);
            });
            GaugeUtil.Guard(ref first, () =>
            {
                var rows = new List<(string Name, string Type, float Circulation)>();
                foreach (var c in CurrencyManager.Currencies) rows.Add((c.Name ?? string.Empty, c.CurrencyType.ToString(), c.Circulation));
                sink.ClearFamily(Circulation); // removed currencies must vanish
                foreach (var r in rows) sink.Set(Circulation, new[] { new Label("currency", r.Name), new Label("type", r.Type) }, r.Circulation);
            });
            GaugeUtil.Guard(ref first, () =>
            {
                var counts = new Dictionary<(bool Founded, string Type), int>();
                foreach (var s in Registrars.Get<Settlement>())
                {
                    var key = (s.Founded, s.SettlementType.DisplayName.ToString());
                    counts[key] = counts.TryGetValue(key, out var c) ? c + 1 : 1;
                }
                sink.ClearFamily(Settlements);
                foreach (var kv in counts)
                    sink.Set(Settlements, new[] { new Label("founded", kv.Key.Founded ? "true" : "false"), new Label("settlement_type", kv.Key.Type) }, kv.Value);
            });
            GaugeUtil.Guard(ref first, () =>
            {
                var n = 0;
                foreach (var _ in CivicsUtils.AllActiveAndValid<Law>(null)) n++;
                GaugeUtil.Set(sink, "eco_laws_active", n);
            });
            GaugeUtil.Guard(ref first, () =>
            {
                var n = 0;
                foreach (var _ in CivicsUtils.AllActiveAndValid<ElectedTitle>(null)) n++;
                GaugeUtil.Set(sink, "eco_elected_titles_active", n);
            });
            GaugeUtil.Rethrow(first);
        }
    }

    /// <summary>World object count; enumerates every object so it runs only every WorldObjectGaugeIntervalSeconds.</summary>
    public sealed class WorldObjectsSource : IGaugeSource
    {
        bool helpRegistered;

        public string Group => GaugeGroups.Live;

        public void Sample(IGaugeSink sink)
        {
            if (!this.helpRegistered) { GaugeUtil.Help(sink, "eco_world_objects", MetricType.Gauge, "World objects placed in the world."); this.helpRegistered = true; }
            var n = 0;
            foreach (var _ in ServiceHolder<IWorldObjectManager>.Obj.All) n++;
            GaugeUtil.Set(sink, "eco_world_objects", n);
        }
    }
}
