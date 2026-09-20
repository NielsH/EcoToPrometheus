namespace EcoToPrometheus.Web
{
    using System;
    using System.Text.Json;
    using System.Text.Json.Serialization;
    using EcoToPrometheus.Core;
    using Microsoft.AspNetCore.Mvc;

    /// <summary>
    /// Discovered automatically because Eco adds every loaded mod DLL as an MVC application part. No auth attribute here:
    /// Eco's global fallback policy already requires an authenticated request (X-API-Key / api_key), which is the v2
    /// default. Phase 4 adds the config-driven filter that also allows anonymous access and refuses an empty token.
    /// </summary>
    [Route("api/v1/plugins/Metrics")]
    public class MetricsController : Controller
    {
        static readonly JsonSerializerOptions JsonOptions = new()
        {
            WriteIndented          = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
        };

        [HttpGet("metrics")]
        public IActionResult Metrics()
        {
            var plugin = MetricsPlugin.Obj;
            try
            {
                if (plugin == null || !plugin.IsReady) return this.NotReady(plugin);
                return this.Content(Exposition.ToPrometheusText(plugin.Registry.Current), Exposition.PrometheusContentType);
            }
            catch (Exception ex)
            {
                plugin?.Worker.RecordError("scrape", ex);
                return this.StatusCode(500, $"{ex.GetType().Name}: {ex.Message}");
            }
        }

        [HttpGet("json")]
        public IActionResult Json()
        {
            var plugin = MetricsPlugin.Obj;
            try
            {
                if (plugin == null || !plugin.IsReady) return this.NotReady(plugin);
                return this.Content(Exposition.ToJson(plugin.Registry.Current), "application/json");
            }
            catch (Exception ex)
            {
                plugin?.Worker.RecordError("scrape", ex);
                return this.StatusCode(500, $"{ex.GetType().Name}: {ex.Message}");
            }
        }

        [HttpGet("status")]
        public IActionResult Status()
        {
            var plugin = MetricsPlugin.Obj;
            try
            {
                if (plugin == null) return this.StatusCode(503, "Metrics plugin not loaded.");
                var s = plugin.Worker.Snapshot();
                var body = new
                {
                    enabled              = plugin.Enabled,
                    ready                = plugin.IsReady,
                    listenerAttached     = plugin.ListenerAttached,
                    expositionAvailable  = Exposition.Available,
                    version              = plugin.Worker.Options.ModVersion,
                    ecoVersion           = plugin.Worker.Options.EcoVersion,
                    eventsQueued         = s.EventsQueued,
                    eventsProcessedTotal = s.EventsProcessedTotal,
                    queueHighWater       = s.QueueHighWater,
                    handlerErrorsTotal   = s.HandlerErrorsTotal,
                    lastTickUtc          = s.LastTickUtc,
                    lastTickMilliseconds = s.LastTickMilliseconds,
                    seriesCount          = s.SeriesCount,
                    workerIntervalSeconds = plugin.Config.WorkerIntervalSeconds,
                    playerLabelFamilies  = plugin.Config.PlayerLabelFamilies,
                    gaugeGroups          = plugin.Config.GaugeGroups,
                };
                return this.Content(JsonSerializer.Serialize(body, JsonOptions), "application/json");
            }
            catch (Exception ex)
            {
                plugin?.Worker.RecordError("scrape", ex);
                return this.StatusCode(500, $"{ex.GetType().Name}: {ex.Message}");
            }
        }

        IActionResult NotReady(MetricsPlugin? plugin)
        {
            var reason = plugin == null ? "plugin not loaded"
                       : !plugin.Enabled ? "plugin disabled"
                       : !Exposition.Available ? "exposition writer not implemented yet (phase 1 skeleton)"
                       : "worker has not published a snapshot yet";
            return this.StatusCode(503, $"Metrics not ready: {reason}.");
        }
    }
}
