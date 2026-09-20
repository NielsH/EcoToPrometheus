namespace EcoToPrometheus
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Eco.Core.Plugins;
    using Eco.Core.Plugins.Interfaces;
    using Eco.Core.Utils;
    using Eco.Core.Utils.Threading;
    using Eco.Gameplay.GameActions;
    using Eco.Shared;
    using Eco.Shared.Localization;
    using Eco.Shared.Logging;
    using Eco.Simulation.Time;
    using EcoToPrometheus.Core;
    using EcoToPrometheus.Hooks;

    /// <summary>Lets the server list the mod. Eco calls the static members by reflection.</summary>
    public class EcoToPrometheusMod : IModInit
    {
        public static ModRegistration Register() => new()
        {
            ModName        = "EcoToPrometheus",
            ModDisplayName = "EcoToPrometheus",
            ModDescription = "Prometheus metrics endpoint on the Eco web API.",
        };
    }

    /// <summary>
    /// Plugin lifecycle. Eco offers no exception isolation for plugins (a throwing Initialize aborts startup, a faulted
    /// worker exits the process), so every entry point here catches everything and degrades to "disabled" instead.
    /// </summary>
    [Priority(PriorityAttribute.Low)]
    public class MetricsPlugin : IModKitPlugin, IInitializablePlugin, IThreadedPlugin, IConfigurablePlugin, ISaveablePlugin
    {
        public const string Name = "Metrics";

        public static MetricsPlugin? Obj { get; private set; }

        readonly PluginConfig<MetricsConfig> config;
        readonly EcoNameResolver             resolver;
        readonly SimStatsCollector           simStats = new();
        readonly Stopwatch                   uptime   = Stopwatch.StartNew();
        readonly object                      attachLock = new();
        IntervalActionWorker?                tickWorker;
        volatile bool                        enabled;
        volatile bool                        listenerAttached;
        string                               lastError = string.Empty;

        public MetricsConfig  Config   => this.config.Config;
        public MetricRegistry Registry { get; } = new();
        public MetricsWorker  Worker   { get; }
        public ActionListener Listener { get; }
        public FoodEatenHook  FoodHook { get; }

        /// <summary>True once the plugin is enabled, the worker has published at least once, and the exposition writer exists.</summary>
        public bool IsReady          => this.enabled && this.Worker.HasPublished && Exposition.Available;
        public bool Enabled          => this.enabled;
        public bool ListenerAttached => this.listenerAttached;

        /// <summary>Set by the attach/detach paths (phase 3: Initialize and the /metrics chat command).</summary>
        internal void SetListenerAttached(bool attached) => this.listenerAttached = attached;

        public MetricsPlugin()
        {
            Obj = this;
            this.config = new PluginConfig<MetricsConfig>(Name);
            var options = new WorkerOptions
            {
                MaxLabelValueLength = this.Config.MaxLabelValueLength,
                QueueWarnThreshold  = this.Config.QueueWarnThreshold,
                ModVersion          = typeof(MetricsPlugin).Assembly.GetName().Version?.ToString(3) ?? "0.0.0",
                EcoVersion          = SafeEcoVersion(),
            };
            this.resolver = new EcoNameResolver();
            this.Worker   = new MetricsWorker(this.Registry, this.resolver, options, LogWarning, LogError);
            this.Listener = new ActionListener(this.Worker);
            this.FoodHook = new FoodEatenHook(this.Worker);
            this.AttachHandler   = this.Attach;
            this.DetachHandler   = this.Detach;
            this.OnCountersReset = () => this.SaveState("reset");
        }

        // ---- IInitializablePlugin ----------------------------------------------------------------

        public void Initialize(TimedTask timer)
        {
            try
            {
                this.SaveConfig();            // materialise Configs/Metrics.eco with defaults on first start
                this.ValidateConfig();
                this.enabled = this.Config.Enabled;
                if (!this.enabled)
                {
                    Log.WriteLine(Localizer.DoStr("[Metrics] disabled by config (Enabled=false)."));
                    return;
                }
                this.resolver.SubscribeInvalidation();
                this.RebuildRules();
                this.RegisterGaugeSources();
                this.LoadState();
                this.Attach();
                var rules = this.Listener.Rules;
                Log.WriteLine(Localizer.DoStr($"[Metrics] initialised v{this.Worker.Options.ModVersion} for Eco {this.Worker.Options.EcoVersion}; worker interval {this.Config.WorkerIntervalSeconds}s; listener attached with {rules.CuratedCount} curated and {rules.AutoCount} auto rules ({rules.NoStatsCount} NoStats types filtered)."));
                // ---- phase 4: the endpoint refuses everything when key auth is on but no token exists ----
                if (!this.Config.AllowAnonymous && !Web.ApiKeyOrAnonymousAttribute.AnyTokenConfigured())
                    LogWarning("[Metrics] Users.eco has no APIAuthToken/APIAdminAuthToken; every request will be refused until one is set (or AllowAnonymous=true).");
            }
            catch (Exception ex)
            {
                this.enabled   = false;
                this.lastError = ex.Message;
                LogError("[Metrics] Initialize failed; the plugin is disabled for this run.", ex);
            }
        }

        // ---- IThreadedPlugin -----------------------------------------------------------------------

        public void Run()
        {
            try
            {
                if (!this.enabled) return;
                var interval = TimeSpan.FromSeconds(this.Config.WorkerIntervalSeconds);
                this.tickWorker = PeriodicWorkerFactory.CreateWithInterval(interval, this.SafeTick);
                this.tickWorker.Start(ThreadPriorityTaskFactory.Lowest);
            }
            catch (Exception ex)
            {
                this.enabled   = false;
                this.lastError = ex.Message;
                LogError("[Metrics] Run failed; the plugin is disabled for this run.", ex);
            }
        }

        void SafeTick()
        {
            // The worker isolates its own stages; this guard is for anything that escapes it, because a faulted
            // Eco worker task calls Environment.Exit(1).
            try
            {
                var now = DateTime.UtcNow;
                if (this.resetPending) this.ApplyPendingReset();   // phase 4: /metrics reset, applied on the worker thread
                this.WriteStateMetrics();
                this.Worker.Tick(now);
                this.MaintainState(now);
            }
            catch (Exception ex) { this.Worker.RecordError("worker", ex); }
        }

        public async Task ShutdownAsync()
        {
            try
            {
                var worker = this.tickWorker;
                if (worker != null)
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    try { await worker.ShutdownAsync().WaitAsync(cts.Token); }
                    catch (OperationCanceledException) { LogWarning("[Metrics] worker did not stop within 5 s; continuing shutdown."); }
                }
                this.Detach();
                this.simStats.Unsubscribe();
                this.resolver.UnsubscribeInvalidation();
                this.SaveState("shutdown");
            }
            catch (Exception ex) { LogError("[Metrics] ShutdownAsync failed.", ex); }
        }

        // ---- hooks (phase 3) ----------------------------------------------------------------------

        /// <summary>Registers the GameAction listener and the food event. Idempotent; used by Initialize and the chat command.</summary>
        public void Attach()
        {
            lock (this.attachLock)
            {
                if (this.listenerAttached) return;
                ActionUtil.AddListener(this.Listener);
                this.FoodHook.Attach();
                this.simStats.Subscribe();
                this.SetListenerAttached(true);
            }
        }

        /// <summary>Kill switch: removes the listener and the food event. Sim-cadence gauges keep updating. Idempotent.</summary>
        public void Detach()
        {
            lock (this.attachLock)
            {
                if (!this.listenerAttached) return;
                ActionUtil.RemoveListener(this.Listener);
                this.FoodHook.Detach();
                this.SetListenerAttached(false);
            }
        }

        /// <summary>Rebuilds the rule set from the current config (allowlist, exclusions) and swaps it into the listener.</summary>
        public void RebuildRules()
        {
            var rules = FamilyRules.Build(this.Config);
            this.FoodHook.Configure(rules);
            this.Listener.Rules = rules;
            foreach (var warning in rules.Warnings) LogWarning("[Metrics] " + warning);
        }

        void RegisterGaugeSources()
        {
            var groups = new HashSet<string>(this.Config.GaugeGroups ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
            var tick   = TimeSpan.FromSeconds(this.Config.WorkerIntervalSeconds);
            var slow   = TimeSpan.FromSeconds(this.Config.SlowGaugeIntervalSeconds);
            var objects = TimeSpan.FromSeconds(this.Config.WorldObjectGaugeIntervalSeconds);

            this.Worker.AddGaugeSource(new ExporterSource(this.Listener, () => this.listenerAttached, this.simStats), tick);
            foreach (var group in new[] { GaugeGroups.Species, GaugeGroups.Climate, GaugeGroups.Global })
                if (groups.Contains(group)) this.Worker.AddGaugeSource(new SimStatsSource(this.simStats, group), tick);
            if (groups.Contains(GaugeGroups.Live))
            {
                this.Worker.AddGaugeSource(new LiveSource(() => this.uptime.Elapsed.TotalSeconds), tick);
                this.Worker.AddGaugeSource(new SlowSource(), slow);
                this.Worker.AddGaugeSource(new WorldObjectsSource(), objects);
            }
        }

        // ---- IServerPlugin / IConfigurablePlugin ---------------------------------------------------

        public string GetCategory() => Localizer.DoStr("Mods");

        public string GetStatus()
        {
            if (!this.enabled) return this.lastError.Length > 0 ? $"Disabled: {this.lastError}" : "Disabled";
            var s = this.Worker.Snapshot();
            return $"{(this.IsReady ? "Serving" : "Starting")}; series {s.SeriesCount}; queued {s.EventsQueued}; processed {s.EventsProcessedTotal}; listener {(this.listenerAttached ? "attached" : "detached")}";
        }

        public IPluginConfig                    PluginConfig => this.config;
        public ThreadSafeAction<object, string> ParamChanged { get; set; } = new();
        public object                           GetEditObject() => this.Config;
        public void OnEditObjectChanged(object o, string param)
        {
            this.SaveConfig();
            if (!this.enabled) return;
            try { this.RebuildRules(); }
            catch (Exception ex) { LogError("[Metrics] rebuilding rules after a config change failed; the previous rules stay active.", ex); }
        }

        // ---- helpers -----------------------------------------------------------------------------

        void ValidateConfig()
        {
            var c = this.Config;
            c.WorkerIntervalSeconds          = Clamp(nameof(c.WorkerIntervalSeconds),          c.WorkerIntervalSeconds,          1,  60);
            c.SlowGaugeIntervalSeconds       = Clamp(nameof(c.SlowGaugeIntervalSeconds),       c.SlowGaugeIntervalSeconds,       5,  600);
            c.WorldObjectGaugeIntervalSeconds = Clamp(nameof(c.WorldObjectGaugeIntervalSeconds), c.WorldObjectGaugeIntervalSeconds, 60, 3600);
            c.StateSaveIntervalSeconds       = Clamp(nameof(c.StateSaveIntervalSeconds),       c.StateSaveIntervalSeconds,       10, 3600);
            c.MaxLabelValueLength            = Clamp(nameof(c.MaxLabelValueLength),            c.MaxLabelValueLength,            8,  1024);
            c.QueueWarnThreshold             = Clamp(nameof(c.QueueWarnThreshold),             c.QueueWarnThreshold,             1000, int.MaxValue);
            c.PlayerLabelFamilies ??= Array.Empty<string>();
            c.ExcludedFamilies    ??= Array.Empty<string>();
            c.GaugeGroups         ??= Array.Empty<string>();
        }

        static int Clamp(string name, int value, int min, int max)
        {
            if (value >= min && value <= max) return value;
            var clamped = Math.Clamp(value, min, max);
            LogWarning($"[Metrics] config {name}={value} is out of range [{min}, {max}]; using {clamped}.");
            return clamped;
        }

        static string SafeEcoVersion()
        {
            try { return EcoVersion.Version; }
            catch { return "unknown"; }
        }

        internal static void LogWarning(string message) => Log.WriteWarningLineLocStr(message);
        internal static void LogError(string message, Exception ex) => Log.WriteError(Localizer.DoStr(message), ex);

        // ---- phase 4 additions ---------------------------------------------------------------------
        // Web endpoint scrape stats, the /metrics chat command's hooks, and counter reset. Nothing here touches Core/Hooks.

        /// <summary>Wired at merge to phase 3's listener attach/detach. Null makes the chat command reply "not available".</summary>
        public Action? AttachHandler { get; set; }
        public Action? DetachHandler { get; set; }

        /// <summary>Runs on the worker thread right after the counters are cleared, so phase 2/5 can persist the zeroed state.</summary>
        public Action? OnCountersReset { get; set; }

        volatile bool resetPending;
        long          scrapesTotal;
        long          lastScrapeBits;      // double bits, so a torn read is impossible on 32-bit hosts too
        long          lastScrapeUtcTicks;

        /// <summary>Render + compress time of the last /metrics or /json response. Exported by the Live gauge source as eco_exporter_scrape_duration_seconds.</summary>
        public double    LastScrapeSeconds => BitConverter.Int64BitsToDouble(Volatile.Read(ref this.lastScrapeBits));
        public DateTime? LastScrapeUtc     { get { var t = Volatile.Read(ref this.lastScrapeUtcTicks); return t == 0 ? null : new DateTime(t, DateTimeKind.Utc); } }
        public long      ScrapesTotal      => Volatile.Read(ref this.scrapesTotal);

        /// <summary>Called by the controller after each successful /metrics or /json response.</summary>
        public void RecordScrape(double seconds, DateTime nowUtc)
        {
            Volatile.Write(ref this.lastScrapeBits, BitConverter.DoubleToInt64Bits(seconds));
            Volatile.Write(ref this.lastScrapeUtcTicks, nowUtc.Ticks);
            Interlocked.Increment(ref this.scrapesTotal);
        }

        /// <summary>
        /// Zeroes every counter. The registry is worker-thread-only, so while the worker runs this only flags the reset and
        /// <see cref="SafeTick"/> applies it on the next tick (returns false); when no worker is running it is applied inline (true).
        /// </summary>
        public bool ResetCounters()
        {
            if (this.tickWorker != null && this.enabled) { this.resetPending = true; return false; }
            this.ApplyPendingReset();
            return true;
        }

        void ApplyPendingReset()
        {
            this.resetPending = false;
            this.Registry.ResetCounters();
            Log.WriteLine(Localizer.DoStr("[Metrics] counters reset to zero."));
            try { this.OnCountersReset?.Invoke(); }
            catch (Exception ex) { LogError("[Metrics] state save after counter reset failed.", ex); }
        }
        // ---- phase 5: persistence ------------------------------------------------------------------
        // Counter totals live in Storage/<SaveName>.metrics.json. Loaded once in Initialize (before the listener attaches
        // and before the worker thread exists, so the registry is safe to touch), saved by the worker every
        // StateSaveIntervalSeconds, on Eco's SaveAll (deferred to the next tick), after a reset, and on shutdown.

        string        statePath        = string.Empty;
        string        stateLoadReason  = "pending";
        bool          stateLoadOk;
        long          stateSavesOk, stateSavesFailed;
        long          writtenSavesOk, writtenSavesFailed, writtenScrapes;
        DateTime      lastStateSaveUtc = DateTime.MinValue;
        volatile bool saveRequested;

        public string    StatePath        => this.statePath;
        public string    StateLoadReason  => this.stateLoadReason;
        public bool      StateLoadOk      => this.stateLoadOk;
        public DateTime? LastStateSaveUtc => this.lastStateSaveUtc == DateTime.MinValue ? null : this.lastStateSaveUtc;
        public long      StateSavesTotal  => Volatile.Read(ref this.stateSavesOk);

        /// <summary>ISaveablePlugin: Eco's world save runs on its own thread, so only flag it; the worker saves on its next tick.</summary>
        public void SaveAll() => this.saveRequested = true;

        static string ResolveStatePath()
        {
            var dir = Path.GetFullPath(StorageManager.Config.StorageDirectory);
            return Path.Combine(dir, StorageManager.SaveName + ".metrics.json");
        }

        void LoadState()
        {
            try
            {
                this.statePath = ResolveStatePath();
                var state = StateStore.Load(this.statePath, out var result, out var message);
                switch (result)
                {
                    case StateLoadResult.Missing:
                        this.stateLoadReason = "missing";
                        Log.WriteLine(Localizer.DoStr($"[Metrics] no state file at {this.statePath}; counters start at zero."));
                        return;
                    case StateLoadResult.Corrupt:
                    case StateLoadResult.NewerVersion:
                        this.stateLoadReason = result == StateLoadResult.Corrupt ? "corrupt" : "newer_version";
                        LogWarning($"[Metrics] state file unusable ({message}); counters start at zero.");
                        return;
                }

                var worldSeconds = WorldTime.Seconds;
                if (state!.WorldSeconds > worldSeconds + 60)
                {
                    var archived = StateStore.ArchiveForNewWorld(this.statePath);
                    this.stateLoadReason = "new_world";
                    LogWarning($"[Metrics] state file is from a previous world (saved at world time {state.WorldSeconds:F0}s, world is now at {worldSeconds:F0}s); archived as {archived}, counters start at zero.");
                    return;
                }

                this.Registry.ImportCounters(state.Counters);
                this.stateLoadReason  = "ok";
                this.stateLoadOk      = true;
                this.lastStateSaveUtc = DateTime.UtcNow;   // start the save interval from now, not from the epoch
                Log.WriteLine(Localizer.DoStr($"[Metrics] restored {state.Counters.Count} counter series from {this.statePath} (saved {state.SavedAtUtc:u})."));
            }
            catch (Exception ex)
            {
                this.stateLoadReason = "error";
                LogError("[Metrics] loading the state file failed; counters start at zero.", ex);
            }
        }

        /// <summary>Worker thread. Periodic save plus any save Eco requested through SaveAll.</summary>
        void MaintainState(DateTime nowUtc)
        {
            if (this.statePath.Length == 0) return;
            var due = (nowUtc - this.lastStateSaveUtc).TotalSeconds >= this.Config.StateSaveIntervalSeconds;
            if (!due && !this.saveRequested) return;
            var reason = this.saveRequested ? "world_save" : "interval";
            this.saveRequested = false;
            this.SaveState(reason);
        }

        /// <summary>Writes the counters. Worker thread, or the shutdown thread after the worker stopped. Never throws.</summary>
        void SaveState(string reason)
        {
            if (this.statePath.Length == 0) return;
            try
            {
                var snapshot = this.Worker.Snapshot();
                var state = new StateFile
                {
                    WorldSeconds = WorldTime.Seconds,
                    Self         = new SelfState { EventsProcessedTotal = snapshot.EventsProcessedTotal, QueueHighWater = snapshot.QueueHighWater },
                };
                foreach (var (key, value) in this.Registry.ExportCounters()) state.Counters[key] = value;
                StateStore.Save(this.statePath, state);
                this.lastStateSaveUtc = DateTime.UtcNow;
                Interlocked.Increment(ref this.stateSavesOk);
                if (this.Config.LogLevel == MetricsLogLevel.Verbose)
                    Log.WriteLine(Localizer.DoStr($"[Metrics] state saved ({reason}): {state.Counters.Count} series."));
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref this.stateSavesFailed);
                this.Worker.RecordError("state", ex);
            }
        }

        /// <summary>Worker thread. State and scrape self-metrics; counters are set from absolute totals the way the worker does it.</summary>
        void WriteStateMetrics()
        {
            var r = this.Registry;
            r.RegisterFamily("eco_exporter_state_saves_total",                 MetricType.Counter, "State file writes, by result.");
            r.RegisterFamily("eco_exporter_state_last_save_timestamp_seconds", MetricType.Gauge,   "Unix time of the last successful state save.");
            r.RegisterFamily("eco_exporter_state_load_ok",                     MetricType.Gauge,   "1 if the state file loaded at startup, 0 otherwise (the status endpoint has the reason).");
            r.RegisterFamily("eco_exporter_scrape_duration_seconds",           MetricType.Gauge,   "Render and compress time of the previous /metrics or /json response.");
            r.RegisterFamily("eco_exporter_scrapes_total",                     MetricType.Counter, "Successful /metrics and /json responses.");

            CounterAbsolute(r, "eco_exporter_state_saves_total", new[] { new Label("result", "ok") },    Volatile.Read(ref this.stateSavesOk),     ref this.writtenSavesOk);
            CounterAbsolute(r, "eco_exporter_state_saves_total", new[] { new Label("result", "error") }, Volatile.Read(ref this.stateSavesFailed), ref this.writtenSavesFailed);
            CounterAbsolute(r, "eco_exporter_scrapes_total",     CounterIncrement.NoLabels,               this.ScrapesTotal,                        ref this.writtenScrapes);

            r.SetGauge("eco_exporter_state_last_save_timestamp_seconds", CounterIncrement.NoLabels, this.lastStateSaveUtc == DateTime.MinValue ? 0 : new DateTimeOffset(this.lastStateSaveUtc).ToUnixTimeSeconds());
            r.SetGauge("eco_exporter_state_load_ok",                     CounterIncrement.NoLabels, this.stateLoadOk ? 1 : 0);
            r.SetGauge("eco_exporter_scrape_duration_seconds",           CounterIncrement.NoLabels, this.LastScrapeSeconds);
        }

        /// <summary>The registry only increments; a restored total from the state file is preserved because only the delta since the last write is added.</summary>
        static void CounterAbsolute(MetricRegistry registry, string family, Label[] labels, long total, ref long written)
        {
            if (total > written) { registry.IncrementCounter(family, labels, total - written); written = total; }
            else if (!registry.TryGetFamily(family, out _) || written == 0 && total == 0) registry.IncrementCounter(family, labels, 0);
        }
    }
}
