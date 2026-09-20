namespace EcoToPrometheus.Core
{
    using System;

    /// <summary>
    /// One counter increment produced by a game event. <see cref="Labels"/> are already strings (type names,
    /// enum names); <see cref="Deferred"/> are ids the worker resolves to names off the game thread.
    /// </summary>
    public sealed class CounterIncrement
    {
        public readonly string          Family;
        public readonly Label[]         Labels;
        public readonly DeferredLabel[] Deferred;
        public readonly double          Delta;

        public CounterIncrement(string family, Label[] labels, DeferredLabel[] deferred, double delta)
        {
            this.Family   = family;
            this.Labels   = labels;
            this.Deferred = deferred;
            this.Delta    = delta;
        }

        public static readonly DeferredLabel[] NoDeferred = Array.Empty<DeferredLabel>();
        public static readonly Label[]         NoLabels   = Array.Empty<Label>();
    }

    /// <summary>
    /// What the hook enqueues for one game event: the count-family increment plus any companion value-family
    /// increments. Built entirely on the game thread from primitives; never holds Eco objects.
    /// </summary>
    public sealed class EventRecord
    {
        public readonly CounterIncrement[] Increments;
        public readonly long               EnqueuedTicks;

        public EventRecord(CounterIncrement[] increments, long enqueuedTicks)
        {
            this.Increments    = increments;
            this.EnqueuedTicks = enqueuedTicks;
        }
    }
}
