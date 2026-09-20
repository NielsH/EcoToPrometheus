namespace EcoToPrometheus.Core
{
    /// <summary>Resolves an Eco id captured on the game thread to a display name. Implemented in Hooks/ with Eco APIs.</summary>
    public interface INameResolver
    {
        /// <returns>The display name, or null when unknown (the worker then uses a placeholder such as <c>player_42</c>).</returns>
        string? Resolve(RefKind kind, int id);
    }

    /// <summary>Where a gauge source writes its samples. Implemented by the worker, backed by the registry.</summary>
    public interface IGaugeSink
    {
        void Set(string family, Label[] labels, double value);

        /// <summary>Drops every series of a gauge family before re-sampling it, so series that vanished (a species that went extinct is still reported as 0 by Eco, but a removed currency is not) do not linger.</summary>
        void ClearFamily(string family);
    }

    /// <summary>A group of gauges sampled on a fixed cadence by the worker. Implemented in Hooks/ with Eco APIs.</summary>
    public interface IGaugeSource
    {
        /// <summary>Config group name (<c>Global</c>, <c>Species</c>, <c>Climate</c>, <c>Live</c>).</summary>
        string Group { get; }

        /// <summary>Called on the worker thread. Must be cheap and must not block on game state.</summary>
        void Sample(IGaugeSink sink);
    }
}
