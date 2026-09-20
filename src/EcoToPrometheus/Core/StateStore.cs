namespace EcoToPrometheus.Core
{
    using System;
    using System.Collections.Generic;

    /// <summary>On-disk shape of <c>Storage/&lt;SaveName&gt;.metrics.json</c>, version 1.</summary>
    public sealed class StateFile
    {
        public int                        Version      { get; set; } = 1;
        public DateTime                   SavedAtUtc   { get; set; }
        public double                     WorldSeconds { get; set; }
        public Dictionary<string, double> Counters     { get; set; } = new(StringComparer.Ordinal);
        public SelfState                  Self         { get; set; } = new();
    }

    public sealed class SelfState
    {
        public long EventsProcessedTotal { get; set; }
        public long QueueHighWater       { get; set; }
    }

    public enum StateLoadResult { Loaded, Missing, Corrupt, NewerVersion, NewWorld }

    /// <summary>
    /// Atomic JSON load/save of the counter state. Phase 2 implements this (plus round-trip and corrupt-file tests).
    /// Save = write <c>.tmp</c>, then <c>File.Replace</c> keeping one <c>.bak</c>. New-world detection compares
    /// <see cref="StateFile.WorldSeconds"/> with the live world clock and renames a stale file to <c>.old</c>.
    /// </summary>
    public static class StateStore
    {
        public static StateFile? Load(string path, out StateLoadResult result, out string? message) =>
            throw new NotImplementedException("Phase 2: StateStore.Load");

        public static void Save(string path, StateFile state) =>
            throw new NotImplementedException("Phase 2: StateStore.Save");
    }
}
