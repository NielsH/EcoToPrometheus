namespace EcoToPrometheus.Hooks
{
    using System;
    using System.Threading;
    using Eco.Core.Utils;
    using Eco.Gameplay.Aliases;
    using Eco.Gameplay.GameActions;
    using Eco.Gameplay.Property;
    using EcoToPrometheus.Core;

    /// <summary>
    /// Global GameAction listener (<c>ActionUtil.AddListener</c>). Runs synchronously on whichever game thread performed
    /// the action, right after Eco recorded it to its own stats database, so the body must be tiny and must never throw.
    /// Eco's own stats filter (<c>[NoStats]</c> types and <see cref="IConditionalStatistics"/> that decline) is re-applied
    /// here so the counters mirror Game.db; skips are counted per reason for <c>eco_exporter_events_filtered_total</c>.
    /// </summary>
    public sealed class ActionListener : IGameActionAware
    {
        readonly MetricsWorker worker;
        volatile FamilyRuleSet rules = FamilyRuleSet.Empty;
        long                   filteredNoStats;
        long                   filteredConditional;

        public ActionListener(MetricsWorker worker) => this.worker = worker;

        /// <summary>Swapped by the plugin when config changes; the hook reads it once per event.</summary>
        public FamilyRuleSet Rules { get => this.rules; set => this.rules = value; }

        /// <summary>Actions skipped because their type carries <c>[NoStats]</c>.</summary>
        public long FilteredNoStats     => Volatile.Read(ref this.filteredNoStats);

        /// <summary>Actions skipped because <see cref="IConditionalStatistics.ShouldRecord"/> returned false (e.g. ChopTree without a felled tree).</summary>
        public long FilteredConditional => Volatile.Read(ref this.filteredConditional);

        // Never grant authorisation; this listener only observes.
        public LazyResult ShouldOverrideAuth(IAlias? alias, IOwned? property, GameAction? action) => LazyResult.FailedNoMessage;

        public void ActionPerformed(GameAction action)
        {
            try
            {
                var rules = this.rules;
                if (rules.IsNoStats(action.GetType()))                                     { Interlocked.Increment(ref this.filteredNoStats);     return; }
                if (action is IConditionalStatistics conditional && !conditional.ShouldRecord()) { Interlocked.Increment(ref this.filteredConditional); return; }
                var record = rules.Build(action);
                if (record != null) this.worker.Enqueue(record);
            }
            catch (Exception ex)
            {
                this.worker.RecordError("listener", ex);
            }
        }
    }
}
