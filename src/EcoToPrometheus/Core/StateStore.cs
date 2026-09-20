namespace EcoToPrometheus.Core
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Text.Encodings.Web;
    using System.Text.Json;
    using System.Text.Json.Serialization;

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
    /// Atomic JSON load/save of the counter state.
    /// Save writes <c>&lt;path&gt;.tmp</c> and swaps it in with <see cref="File.Replace(string, string, string)"/>, keeping one
    /// <c>.bak</c>. Load moves a file it cannot use out of the way (<c>.corrupt-&lt;stamp&gt;</c>, <c>.v&lt;N&gt;.old</c>)
    /// so the next save starts clean. New-world detection compares <see cref="StateFile.WorldSeconds"/> with the live
    /// world clock in the plugin and calls <see cref="ArchiveForNewWorld"/>. Counter values round-trip bit-exact.
    /// </summary>
    public static class StateStore
    {
        public const int CurrentVersion = 1;

        const string StampFormat = "yyyyMMdd-HHmmss";

        static readonly JsonSerializerOptions JsonOptions = new()
        {
            WriteIndented               = true,
            PropertyNameCaseInsensitive = true,
            Encoder                     = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            NumberHandling              = JsonNumberHandling.AllowNamedFloatingPointLiterals,
            ReadCommentHandling         = JsonCommentHandling.Skip,
            AllowTrailingCommas         = true,
        };

        /// <summary>
        /// Reads the state file. Missing -> (null, <see cref="StateLoadResult.Missing"/>). Unreadable or malformed ->
        /// (null, <see cref="StateLoadResult.Corrupt"/>) and the file is renamed to <c>&lt;path&gt;.corrupt-&lt;stamp&gt;</c>.
        /// Written by a newer mod -> (null, <see cref="StateLoadResult.NewerVersion"/>) and the file is renamed to
        /// <c>&lt;path&gt;.v&lt;N&gt;.old</c>. <paramref name="message"/> carries the reason for anything but Loaded/Missing.
        /// </summary>
        public static StateFile? Load(string path, out StateLoadResult result, out string? message)
        {
            message = null;
            if (!File.Exists(path))
            {
                result = StateLoadResult.Missing;
                return null;
            }

            StateFile state;
            try
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
                state = JsonSerializer.Deserialize<StateFile>(stream, JsonOptions) ?? throw new JsonException("The file holds JSON null.");
                state.Counters ??= new Dictionary<string, double>(StringComparer.Ordinal);
                state.Self     ??= new SelfState();
            }
            catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException or NotSupportedException)
            {
                var moved = TryMoveAside(path, path + ".corrupt-" + Stamp(DateTime.UtcNow), out var moveError);
                result  = StateLoadResult.Corrupt;
                message = $"{ex.GetType().Name}: {ex.Message}" + DescribeMove(moved, moveError);
                return null;
            }

            if (state.Version > CurrentVersion)
            {
                var moved = TryMoveAside(path, path + ".v" + state.Version.ToString(CultureInfo.InvariantCulture) + ".old", out var moveError);
                result  = StateLoadResult.NewerVersion;
                message = $"State file version {state.Version} is newer than supported version {CurrentVersion}" + DescribeMove(moved, moveError);
                return null;
            }

            result = StateLoadResult.Loaded;
            return state;
        }

        /// <summary>
        /// Writes <paramref name="state"/> atomically: serialise to <c>&lt;path&gt;.tmp</c>, flush to disk, then replace the
        /// target keeping the previous file as <c>&lt;path&gt;.bak</c> (or a plain move when there is no previous file).
        /// Creates the directory when missing and stamps <see cref="StateFile.SavedAtUtc"/>.
        /// </summary>
        public static void Save(string path, StateFile state)
        {
            var fullPath = Path.GetFullPath(path);
            var dir      = Path.GetDirectoryName(fullPath);
            if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

            state.SavedAtUtc = DateTime.UtcNow;

            var tmp = fullPath + ".tmp";
            using (var stream = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(stream, state, JsonOptions);
                stream.Flush(flushToDisk: true);
            }

            if (File.Exists(fullPath)) File.Replace(tmp, fullPath, fullPath + ".bak", ignoreMetadataErrors: true);
            else                       File.Move(tmp, fullPath);
        }

        /// <summary>
        /// Moves a state file that belongs to a previous world to <c>&lt;path&gt;.&lt;stamp&gt;.old</c> and returns that
        /// name. The caller decides (by comparing <see cref="StateFile.WorldSeconds"/> with the live clock) that this is needed.
        /// </summary>
        public static string ArchiveForNewWorld(string path)
        {
            var target = UniqueName(path + "." + Stamp(DateTime.UtcNow) + ".old");
            File.Move(path, target);
            return target;
        }

        /// <summary>
        /// Drops every counter whose family <paramref name="isKnown"/> rejects, so series from renamed or removed metrics
        /// do not linger across mod versions. Returns the distinct dropped family names, sorted, for the log.
        /// </summary>
        public static IReadOnlyList<string> PruneUnknownFamilies(StateFile state, Func<string, bool> isKnown)
        {
            var dropped = new SortedSet<string>(StringComparer.Ordinal);
            var remove  = new List<string>();
            foreach (var key in state.Counters.Keys)
            {
                string family;
                try { family = SeriesKey.Parse(key).Family; }
                catch (FormatException) { family = key; }
                if (isKnown(family)) continue;
                dropped.Add(family);
                remove.Add(key);
            }
            foreach (var key in remove) state.Counters.Remove(key);
            return new List<string>(dropped);
        }

        static string Stamp(DateTime utc) => utc.ToString(StampFormat, CultureInfo.InvariantCulture);

        /// <summary>Appends <c>-2</c>, <c>-3</c>, ... before the extension when the candidate already exists (same-second collisions).</summary>
        static string UniqueName(string candidate)
        {
            if (!File.Exists(candidate)) return candidate;
            var ext  = Path.GetExtension(candidate);
            var stem = candidate.Substring(0, candidate.Length - ext.Length);
            for (int i = 2; ; i++)
            {
                var name = stem + "-" + i.ToString(CultureInfo.InvariantCulture) + ext;
                if (!File.Exists(name)) return name;
            }
        }

        static string? TryMoveAside(string path, string target, out string? error)
        {
            error = null;
            try
            {
                target = UniqueName(target);
                File.Move(path, target);
                return target;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException)
            {
                error = ex.Message;
                return null;
            }
        }

        static string DescribeMove(string? moved, string? error) =>
            moved != null ? $"; moved to {moved}" : $"; could not move the file aside: {error}";
    }
}
