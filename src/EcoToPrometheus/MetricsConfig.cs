namespace EcoToPrometheus
{
    using System.ComponentModel;
    using Eco.Shared.Localization;

    public enum MetricsLogLevel { Quiet, Normal, Verbose }

    /// <summary>Configs/Metrics.eco. Materialised with these defaults on first start; editable in the server GUI and via POST /api/v1/plugins/config/Metrics.</summary>
    [TypeConverter(typeof(ExpandableObjectConverter))]
    public class MetricsConfig
    {
        [LocDescription("Master switch. When false the plugin loads but registers no listener and the endpoints answer 503.")]
        public bool Enabled { get; set; } = true;

        [LocDescription("When false, requests need Eco's API token (X-API-Key header or api_key query parameter) and an empty configured token is refused.")]
        public bool AllowAnonymous { get; set; } = false;

        [LocDescription("Queue drain and cheap live-gauge cadence in seconds (1 to 60).")]
        public int WorkerIntervalSeconds { get; set; } = 1;

        [LocDescription("Cadence in seconds for gauges that enumerate registrars: currencies, settlements, laws (5 to 600).")]
        public int SlowGaugeIntervalSeconds { get; set; } = 30;

        [LocDescription("Cadence in seconds for the world-object count, which enumerates every object (60 to 3600).")]
        public int WorldObjectGaugeIntervalSeconds { get; set; } = 300;

        [LocDescription("How often counter totals are written to Storage/<SaveName>.metrics.json, in seconds (10 to 3600).")]
        public int StateSaveIntervalSeconds { get; set; } = 60;

        [LocDescription("GameAction type names whose counters carry a player label. Case-insensitive. FoodEaten is accepted too.")]
        public string[] PlayerLabelFamilies { get; set; } =
        {
            "ChopTree", "ChopStump", "DigOrMine", "HarvestOrHunt", "PlantSeeds",
            "ItemCraftedAction", "CurrencyTrade", "BarterTrade",
            "GainSpecialty", "SpecialtyLevelUp", "CharacterLevelUp", "Play",
        };

        [LocDescription("GameAction type names that are never counted.")]
        public string[] ExcludedFamilies { get; set; } = { "ChatSent" };

        [LocDescription("Gauge groups to sample: Global, Species, Climate, Live. Remove Species to drop the ~94 population series.")]
        public string[] GaugeGroups { get; set; } = { "Global", "Species", "Climate", "Live" };

        [LocDescription("Label values longer than this are truncated.")]
        public int MaxLabelValueLength { get; set; } = 64;

        [LocDescription("Event queue backlog that triggers a warning in the log, once per crossing.")]
        public int QueueWarnThreshold { get; set; } = 100000;

        [LocDescription("Quiet, Normal or Verbose. Verbose logs every state save and every scrape.")]
        public MetricsLogLevel LogLevel { get; set; } = MetricsLogLevel.Normal;
    }
}
