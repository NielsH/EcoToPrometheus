namespace EcoToPrometheus.Hooks
{
    using EcoToPrometheus.Core;

    // Phase 3 implements the four gauge groups from the spec:
    //   Global  - StatGameplaySettings.LatestGlobalStats (33 [StatProp]s), copied on SimStats.OnStatsCollected
    //   Species - EcoSim.AllSpecies + species.Layer?.RoundedTotal, same cadence
    //   Climate - WorldLayerManager.Obj.ClimateSim.State, same cadence
    //   Live    - UserManager.OnlineUsers.Count, WorldTime, currencies, settlements, laws, world objects
    // Each implements IGaugeSource and is registered with MetricsWorker.AddGaugeSource at the cadence from config.

    /// <summary>Placeholder so the group name is defined in one place; phase 3 replaces this with the real sources.</summary>
    public static class GaugeGroups
    {
        public const string Global  = "Global";
        public const string Species = "Species";
        public const string Climate = "Climate";
        public const string Live    = "Live";
    }
}
