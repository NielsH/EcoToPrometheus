namespace EcoToPrometheus.Hooks
{
    using System;
    using System.Collections.Generic;
    using Eco.Gameplay.GameActions;
    using EcoToPrometheus.Core;

    /// <summary>
    /// How one GameAction type turns into counter increments. <see cref="Build"/> runs on the game thread:
    /// it may read fields, type names and enums, and capture ids; it must not touch registrars or format strings.
    /// </summary>
    public sealed class FamilyRule
    {
        public string SourceName { get; }                          // GameAction type name, e.g. "ChopTree"
        public string Family     { get; }                          // eco_action_chop_tree_total
        public Func<GameAction, bool, CounterIncrement[]> Build { get; } // (action, withPlayerLabel) -> increments

        public FamilyRule(string sourceName, string family, Func<GameAction, bool, CounterIncrement[]> build)
        {
            this.SourceName = sourceName;
            this.Family     = family;
            this.Build      = build;
        }
    }

    /// <summary>
    /// Immutable Type -> rule map plus the config-derived allow/exclude sets. The listener holds a volatile reference,
    /// the worker builds a new one when config changes. Phase 3 fills <see cref="FamilyRules"/>; the empty set enqueues nothing.
    /// </summary>
    public sealed class FamilyRuleSet
    {
        public static readonly FamilyRuleSet Empty = new(new Dictionary<Type, FamilyRule>(), new HashSet<string>(StringComparer.OrdinalIgnoreCase), new HashSet<string>(StringComparer.OrdinalIgnoreCase));

        readonly Dictionary<Type, FamilyRule> byType;
        readonly HashSet<string>              playerLabelFamilies;
        readonly HashSet<string>              excludedFamilies;

        public FamilyRuleSet(Dictionary<Type, FamilyRule> byType, HashSet<string> playerLabelFamilies, HashSet<string> excludedFamilies)
        {
            this.byType              = byType;
            this.playerLabelFamilies = playerLabelFamilies;
            this.excludedFamilies    = excludedFamilies;
        }

        public int Count => this.byType.Count;

        /// <summary>Returns null when the action is excluded, filtered like Eco's own stats, or unknown to this set.</summary>
        public EventRecord? Build(GameAction action)
        {
            if (this.byType.Count == 0) return null;
            if (!this.byType.TryGetValue(action.GetType(), out var rule)) return null;
            if (this.excludedFamilies.Contains(rule.SourceName)) return null;
            var increments = rule.Build(action, this.playerLabelFamilies.Contains(rule.SourceName));
            return increments.Length == 0 ? null : new EventRecord(increments, Environment.TickCount64);
        }
    }

    /// <summary>Phase 3: the catalogue in code. Builds a <see cref="FamilyRuleSet"/> from the loaded GameAction types and the config.</summary>
    public static class FamilyRules
    {
        public static FamilyRuleSet Build(MetricsConfig config) => FamilyRuleSet.Empty;
    }
}
