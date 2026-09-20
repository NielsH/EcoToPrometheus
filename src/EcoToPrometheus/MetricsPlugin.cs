namespace EcoToPrometheus
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Eco.Core.Plugins;
    using Eco.Core.Plugins.Interfaces;
    using Eco.Core.Utils;
    using Eco.Core.Utils.Threading;
    using Eco.Shared;
    using Eco.Shared.Localization;
    using Eco.Shared.Logging;
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
    public class MetricsPlugin : IModKitPlugin, IInitializablePlugin, IThreadedPlugin, IConfigurablePlugin
    {
        public const string Name = "Metrics";

        public static MetricsPlugin? Obj { get; private set; }

        readonly PluginConfig<MetricsConfig> config;
        IntervalActionWorker?                tickWorker;
        volatile bool                        enabled;
        volatile bool                        listenerAttached;
        string                               lastError = string.Empty;

        public MetricsConfig  Config   => this.config.Config;
        public MetricRegistry Registry { get; } = new();
        public MetricsWorker  Worker   { get; }
        public ActionListener Listener { get; }

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
            this.Worker   = new MetricsWorker(this.Registry, new EcoNameResolver(), options, LogWarning, LogError);
            this.Listener = new ActionListener(this.Worker);
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
                // Phase 3 attaches the GameAction listener and registers gauge sources here.
                Log.WriteLine(Localizer.DoStr($"[Metrics] initialised v{this.Worker.Options.ModVersion} for Eco {this.Worker.Options.EcoVersion}; worker interval {this.Config.WorkerIntervalSeconds}s, listener not attached (phase 1 skeleton)."));
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
            try { this.Worker.Tick(DateTime.UtcNow); }
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
                // Phase 2/3: final state save happens here.
            }
            catch (Exception ex) { LogError("[Metrics] ShutdownAsync failed.", ex); }
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
        public void OnEditObjectChanged(object o, string param) => this.SaveConfig();

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
    }
}
