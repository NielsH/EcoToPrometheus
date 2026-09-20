namespace EcoToPrometheus.Web
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.IO.Compression;
    using System.Text;
    using System.Text.Json;
    using System.Text.Json.Serialization;
    using EcoToPrometheus.Core;
    using Microsoft.AspNetCore.Authorization;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;

    /// <summary>
    /// Discovered automatically because Eco adds every loaded mod DLL as an MVC application part.
    /// <c>[AllowAnonymous]</c> keeps Eco's global fallback policy (RequireAuthenticatedUser) out of the way; access is decided
    /// per request by <see cref="ApiKeyOrAnonymousAttribute"/> from <c>MetricsConfig.AllowAnonymous</c> and the Users.eco tokens.
    /// All three routes gzip their body when the client sends <c>Accept-Encoding: gzip</c> and are marked <c>Cache-Control: no-store</c>.
    /// </summary>
    [AllowAnonymous, ApiKeyOrAnonymous, Route("api/v1/plugins/Metrics")]
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
                return this.Scrape(plugin, Exposition.ToPrometheusText(plugin.Registry.Current), Exposition.PrometheusContentType);
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
                return this.Scrape(plugin, Exposition.ToJson(plugin.Registry.Current), "application/json");
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
                    allowAnonymous       = plugin.Config.AllowAnonymous,
                    version              = plugin.Worker.Options.ModVersion,
                    ecoVersion           = plugin.Worker.Options.EcoVersion,
                    eventsQueued         = s.EventsQueued,
                    eventsProcessedTotal = s.EventsProcessedTotal,
                    queueHighWater       = s.QueueHighWater,
                    handlerErrorsTotal   = s.HandlerErrorsTotal,
                    lastTickUtc          = s.LastTickUtc,
                    lastTickMilliseconds = s.LastTickMilliseconds,
                    seriesCount          = s.SeriesCount,
                    scrapesTotal         = plugin.ScrapesTotal,
                    lastScrapeUtc        = plugin.LastScrapeUtc,
                    lastScrapeSeconds    = plugin.LastScrapeSeconds,
                    stateLoadReason      = plugin.StateLoadReason,
                    stateLoadOk          = plugin.StateLoadOk,
                    statePath            = plugin.StatePath,
                    stateLastSaveUtc     = plugin.LastStateSaveUtc,
                    stateSavesTotal      = plugin.StateSavesTotal,
                    workerIntervalSeconds = plugin.Config.WorkerIntervalSeconds,
                    playerLabelFamilies  = plugin.Config.PlayerLabelFamilies,
                    gaugeGroups          = plugin.Config.GaugeGroups,
                };
                return this.Payload(JsonSerializer.Serialize(body, JsonOptions), "application/json");
            }
            catch (Exception ex)
            {
                plugin?.Worker.RecordError("scrape", ex);
                return this.StatusCode(500, $"{ex.GetType().Name}: {ex.Message}");
            }
        }

        /// <summary>Renders (already done by the caller) plus compresses under a stopwatch and records the time on the plugin.</summary>
        IActionResult Scrape(MetricsPlugin plugin, string body, string contentType)
        {
            var sw     = Stopwatch.StartNew();
            var result = this.Payload(body, contentType);
            plugin.RecordScrape(sw.Elapsed.TotalSeconds, DateTime.UtcNow);
            return result;
        }

        /// <summary>UTF-8 body, gzip-compressed when the client accepts it; never cached.</summary>
        IActionResult Payload(string body, string contentType)
        {
            var headers = this.Response.Headers;
            headers.CacheControl = "no-store";
            var bytes = Encoding.UTF8.GetBytes(body);
            if (AcceptsGzip(this.Request))
            {
                bytes = Gzip(bytes);
                headers.ContentEncoding = "gzip";
                headers.Append("Vary", "Accept-Encoding");
            }
            return this.File(bytes, contentType);
        }

        static bool AcceptsGzip(HttpRequest request)
        {
            var accepted = request.GetTypedHeaders().AcceptEncoding;
            if (accepted == null) return false;
            foreach (var enc in accepted)
            {
                if (!enc.Value.Equals("gzip", StringComparison.OrdinalIgnoreCase)) continue;
                return (enc.Quality ?? 1.0) > 0;
            }
            return false;
        }

        static byte[] Gzip(byte[] raw)
        {
            using var ms = new MemoryStream(raw.Length / 4 + 64);
            using (var gz = new GZipStream(ms, CompressionLevel.Fastest, leaveOpen: true))
                gz.Write(raw, 0, raw.Length);
            return ms.ToArray();
        }

        IActionResult NotReady(MetricsPlugin? plugin)
        {
            var reason = plugin == null ? "plugin not loaded"
                       : !plugin.Enabled ? "plugin disabled"
                       : !Exposition.Available ? "exposition writer not implemented yet (phase 1 skeleton)"
                       : "worker has not published a snapshot yet";
            this.Response.Headers.CacheControl = "no-store";
            return this.StatusCode(503, $"Metrics not ready: {reason}.");
        }
    }
}
