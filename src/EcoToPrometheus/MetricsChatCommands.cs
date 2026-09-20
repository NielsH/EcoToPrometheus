namespace EcoToPrometheus
{
    using System;
    using System.Linq;
    using Eco.Gameplay.Players;
    using Eco.Gameplay.Systems.Messaging.Chat.Commands;
    using Eco.Gameplay.UI;
    using Eco.Shared.Localization;
    using Eco.Shared.Logging;
    using EcoToPrometheus.Core;

    /// <summary>
    /// <c>/metrics status|reset|detach|attach</c>, admin only. Discovered by <c>ChatCommandService.AddHandlersByReflection</c>
    /// (the mod DLL is a game assembly). The first parameter is <see cref="User"/>, so RCON and the web
    /// <c>/api/v1/command/exec</c> endpoint (which runs at User level anyway) cannot reach these; that is intended.
    /// Every body catches everything: a throwing command is logged by Eco but must never take the server down.
    /// </summary>
    [ChatCommandHandler]
    public static class MetricsChatCommands
    {
        [ChatCommand("Prometheus metrics exporter admin commands.", ChatAuthorizationLevel.Admin)]
        public static void Metrics(User user) =>
            Reply(user, "Usage: /metrics status | reset | detach | attach");

        [ChatSubCommand("Metrics", "Shows the exporter status (same fields as /api/v1/plugins/Metrics/status).", ChatAuthorizationLevel.Admin)]
        public static void Status(User user)
        {
            try
            {
                var plugin = MetricsPlugin.Obj;
                if (plugin == null) { Reply(user, "Metrics plugin not loaded."); return; }
                var s      = plugin.Worker.Snapshot();
                var errors = s.HandlerErrorsTotal.Values.Sum();
                Reply(user, $"Metrics v{plugin.Worker.Options.ModVersion} for Eco {plugin.Worker.Options.EcoVersion}: " +
                            $"{(plugin.Enabled ? "enabled" : "disabled")}, {(plugin.IsReady ? "serving" : "not ready")}, " +
                            $"listener {(plugin.ListenerAttached ? "attached" : "detached")}, exposition {(Exposition.Available ? "available" : "unavailable")}, " +
                            $"auth {(plugin.Config.AllowAnonymous ? "anonymous" : "API key")}.");
                Reply(user, $"Events: queued {s.EventsQueued}, processed {s.EventsProcessedTotal}, queue high-water {s.QueueHighWater}, handler errors {errors}.");
                Reply(user, $"Worker: series {s.SeriesCount}, interval {plugin.Config.WorkerIntervalSeconds}s, last tick {Utc(s.LastTickUtc)} ({s.LastTickMilliseconds:0.0} ms).");
                Reply(user, $"Scrapes: {plugin.ScrapesTotal} total, last {Utc(plugin.LastScrapeUtc)} ({plugin.LastScrapeSeconds * 1000:0.0} ms).");
                Reply(user, $"Player labels: {string.Join(", ", plugin.Config.PlayerLabelFamilies)}. Gauge groups: {string.Join(", ", plugin.Config.GaugeGroups)}.");
            }
            catch (Exception ex) { Fail(user, "status", ex); }
        }

        [ChatSubCommand("Metrics", "Zeroes every counter total (gauges are untouched) and saves the state file.", ChatAuthorizationLevel.Admin)]
        public static void Reset(User user)
        {
            try
            {
                var plugin = MetricsPlugin.Obj;
                if (plugin == null) { Reply(user, "Metrics plugin not loaded."); return; }
                var applied = plugin.ResetCounters();
                Log.WriteLine(Localizer.DoStr($"[Metrics] counter reset requested by {user.Name}."));
                Reply(user, applied ? "Metrics counters reset." : $"Metrics counter reset queued; the worker applies it within {plugin.Config.WorkerIntervalSeconds}s.");
            }
            catch (Exception ex) { Fail(user, "reset", ex); }
        }

        [ChatSubCommand("Metrics", "Detaches the GameAction listener (kill switch); gauges keep sampling.", ChatAuthorizationLevel.Admin)]
        public static void Detach(User user) => Toggle(user, "detach", p => p.DetachHandler, false);

        [ChatSubCommand("Metrics", "Re-attaches the GameAction listener after a detach.", ChatAuthorizationLevel.Admin)]
        public static void Attach(User user) => Toggle(user, "attach", p => p.AttachHandler, true);

        static void Toggle(User user, string verb, Func<MetricsPlugin, Action?> pick, bool wantAttached)
        {
            try
            {
                var plugin = MetricsPlugin.Obj;
                if (plugin == null)  { Reply(user, "Metrics plugin not loaded."); return; }
                if (!plugin.Enabled) { Reply(user, "Metrics plugin is disabled (Enabled=false); nothing to " + verb + "."); return; }
                if (plugin.ListenerAttached == wantAttached) { Reply(user, $"Metrics listener is already {(wantAttached ? "attached" : "detached")}."); return; }
                var handler = pick(plugin);
                if (handler == null) { Reply(user, $"Metrics {verb} is not available in this build."); return; }
                handler();
                Log.WriteLine(Localizer.DoStr($"[Metrics] listener {verb} requested by {user.Name}; listener now {(plugin.ListenerAttached ? "attached" : "detached")}."));
                Reply(user, $"Metrics listener {(plugin.ListenerAttached ? "attached" : "detached")}.");
            }
            catch (Exception ex) { Fail(user, verb, ex); }
        }

        static string Utc(DateTime? t) => t.HasValue ? t.Value.ToString("yyyy-MM-dd HH:mm:ss") + " UTC" : "never";

        static void Reply(User user, string text) => user.Player?.MsgLocStr(text);

        static void Fail(User user, string what, Exception ex)
        {
            MetricsPlugin.LogError($"[Metrics] /metrics {what} failed.", ex);
            Reply(user, $"Metrics {what} failed: {ex.GetType().Name}: {ex.Message}");
        }
    }
}
