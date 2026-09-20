namespace EcoToPrometheus.Hooks
{
    using System;
    using Eco.Core.Utils;
    using Eco.Gameplay.Aliases;
    using Eco.Gameplay.GameActions;
    using Eco.Gameplay.Property;
    using EcoToPrometheus.Core;

    /// <summary>
    /// Global GameAction listener (<c>ActionUtil.AddListener</c>). Runs synchronously on whichever game thread performed
    /// the action, right after Eco recorded it to its own stats database, so the body must be tiny and must never throw.
    /// Phase 3 fills in the classification via <see cref="FamilyRules"/>; phase 1 only carries the contract.
    /// </summary>
    public sealed class ActionListener : IGameActionAware
    {
        readonly MetricsWorker worker;
        volatile FamilyRuleSet rules = FamilyRuleSet.Empty;

        public ActionListener(MetricsWorker worker) => this.worker = worker;

        /// <summary>Swapped by the worker when config changes; the hook reads it once per event.</summary>
        public FamilyRuleSet Rules { get => this.rules; set => this.rules = value; }

        // Never grant authorisation; this listener only observes.
        public LazyResult ShouldOverrideAuth(IAlias? alias, IOwned? property, GameAction? action) => LazyResult.FailedNoMessage;

        public void ActionPerformed(GameAction action)
        {
            try
            {
                var record = this.rules.Build(action);
                if (record != null) this.worker.Enqueue(record);
            }
            catch (Exception ex)
            {
                this.worker.RecordError("listener", ex);
            }
        }
    }
}
