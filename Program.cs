using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using LiteDB;

class EcoMetricsExporter
{
    static string DbPath = "";
    static string ListenPrefix = "http://+:9101/";
    static int PollIntervalSeconds = 30;

    // Track last seen _id per collection so we only read new documents
    static readonly Dictionary<string, BsonValue> LastSeen = new();

    // Current metric state: only the latest value per unique series
    // Key = full metric line without the value (e.g. "eco_foo{citizen=\"123\"}")
    // Value = (doubleValue, timestampMs)
    static readonly object MetricsLock = new();

    // Gauges: latest value per series key (timeseries + citizen stats)
    static readonly Dictionary<string, (double Value, long TimestampMs)> GaugeMetrics = new();

    // Counters: running totals per series key (events)
    static readonly Dictionary<string, (double Value, long TimestampMs)> CounterMetrics = new();

    // Citizen ID -> display name mapping
    static readonly Dictionary<string, string> CitizenNames = new();

    // Collection long names from the index collection
    static readonly Dictionary<string, string> LongNames = new();

    // Metrics to skip entirely (not useful for dashboards)
    static readonly HashSet<string> SkippedMetrics = new(StringComparer.OrdinalIgnoreCase)
    {
        "eco_chat_sent",
        "eco_chop_stump",
        "eco_claim_or_unclaim_property",
        "eco_construct_or_deconstruct",
        "eco_create_tree_debris",
        "eco_create_work_order",
        "eco_drop_or_pickup_block",
        "eco_labor_work_order_action",
        "eco_move_world_object",
        "eco_place_or_pick_up_object",
    };

    // Label keys to exclude from event counters (reduce cardinality)
    static readonly HashSet<string> ExcludedEventLabels = new(StringComparer.OrdinalIgnoreCase)
    {
        "WorldObjectItem",
    };

    // Event metrics that should include a citizen_name label for per-player breakdown
    static readonly HashSet<string> CitizenLabeledMetrics = new(StringComparer.OrdinalIgnoreCase)
    {
        "eco_harvest_or_hunt",
        "eco_dig_or_mine",
        "eco_chop_tree",
        "eco_plant_seeds",
        "eco_currency_trade",
    };

    // Currency id → currency name, extracted from Game.eco transaction tooltips
    static readonly Dictionary<int, string> CurrencyNames = new();

    // Eco.Shared.Items.BoughtOrSold enum values (from Game.db's `enums` collection)
    //   32 = Buying, 33 = Selling
    static readonly Dictionary<int, string> BoughtOrSoldLabels = new()
    {
        { 32, "Buying" },
        { 33, "Selling" },
    };

    // HELP text emitted in the /metrics output. Keys are the metric family
    // name (the part before any '{labels}'). Missing entries just skip HELP/TYPE.
    static readonly Dictionary<string, string> MetricHelp = new()
    {
        ["eco_currency_trade_total"]          = "Number of trade transactions",
        ["eco_currency_trade_items_total"]    = "Total items exchanged across trade transactions",
        ["eco_currency_trade_currency_total"] = "Total currency exchanged across trade transactions",
    };

    // State file path (derived from DbPath, saved next to the database)
    static string StatePath = "";

    static void Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine("Usage: EcoToPrometheus <path-to-Game.db> [listen-url] [poll-interval-seconds]");
            Console.WriteLine("  Example: EcoToPrometheus /path/to/Game.db http://+:9101/ 30");
            Console.WriteLine("  Default listen URL: http://+:9101/");
            Console.WriteLine("  Default poll interval: 30 seconds");
            Console.WriteLine("  Place Game.eco next to Game.db to auto-resolve citizen IDs to player names");
            return;
        }

        DbPath = args[0];
        if (args.Length >= 2) ListenPrefix = args[1];
        if (args.Length >= 3) PollIntervalSeconds = int.Parse(args[2]);
        if (!File.Exists(DbPath))
        {
            Console.Error.WriteLine("Error: File not found: " + DbPath);
            return;
        }

        Console.WriteLine("Eco Metrics Exporter");
        Console.WriteLine("  Database:      " + DbPath);
        Console.WriteLine("  Listen URL:    " + ListenPrefix + "metrics");
        Console.WriteLine("  Poll interval: " + PollIntervalSeconds + "s");
        Console.WriteLine();

        var dbDir = Path.GetDirectoryName(DbPath) ?? ".";
        StatePath = Path.Combine(dbDir, "eco-metrics-state.json");

        LoadLongNames();
        LoadCitizenNames();
        LoadCurrencyNames();
        LoadState();

        Console.WriteLine("Initial data load...");
        PollDatabase();
        SaveState();
        int totalSeries;
        lock (MetricsLock)
        {
            totalSeries = GaugeMetrics.Count + CounterMetrics.Count;
        }
        Console.WriteLine("Loaded " + totalSeries + " metric series from existing data.");
        Console.WriteLine();

        var pollThread = new Thread(PollLoop) { IsBackground = true, Name = "DbPoller" };
        pollThread.Start();

        RunHttpServer();
    }

    static void LoadLongNames()
    {
        try
        {
            using var db = new LiteDatabase("Filename=" + DbPath + ";ReadOnly=true");
            var indexCol = db.GetCollection("index");
            foreach (var doc in indexCol.FindAll())
            {
                var shortName = doc["_id"].AsString;
                var longName = doc.ContainsKey("LongName") ? doc["LongName"].AsString : shortName;
                LongNames[shortName] = longName;
            }
            Console.WriteLine("Loaded " + LongNames.Count + " collection name mappings from index.");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Warning: Could not load index collection: " + ex.Message);
        }
    }

    static string EcoSavePath = "";

    static void LoadCitizenNames()
    {
        // Look for Game.eco next to the Game.db file
        var dbDir = Path.GetDirectoryName(DbPath) ?? ".";
        EcoSavePath = Path.Combine(dbDir, "Game.eco");

        if (!File.Exists(EcoSavePath))
        {
            Console.WriteLine("No Game.eco found next to Game.db, citizen IDs will be used as-is.");
            Console.WriteLine("  Expected: " + EcoSavePath);
            return;
        }

        RefreshCitizenNames();
    }

    static void RefreshCitizenNames()
    {
        if (!File.Exists(EcoSavePath)) return;

        try
        {
            var newNames = ExtractNamesFromSave(EcoSavePath);
            if (newNames.Count == 0) return;

            int added = 0;
            lock (MetricsLock)
            {
                foreach (var kv in newNames)
                {
                    if (!CitizenNames.ContainsKey(kv.Key))
                        added++;
                    CitizenNames[kv.Key] = kv.Value;
                }
            }

            Console.WriteLine("Loaded " + CitizenNames.Count + " citizen names from Game.eco" +
                (added > 0 ? " (" + added + " new)" : ""));
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Warning: Could not read Game.eco: " + ex.Message);
        }
    }

    // Currency names are not stored as a clean registrar in Game.eco — but every
    // in-game transaction tooltip embeds the name in a predictable format:
    //   <link="view:<classId>:<currencyId>"><icon name="CurrencySymbol"...>
    //     <style="Currency...">0.5 Jorimoni Credit</style></icon></link>
    // The amount varies but the currency name after it is stable. We regex over
    // the whole EconomyManager/Data blob and take the most common name per id.
    static readonly System.Text.RegularExpressions.Regex CurrencyTooltipRegex =
        new(@"<link=""view:\d+:(\d+)""><icon name=""CurrencySymbol""[^>]*><style=""Currency[^""]*"">([-\d.,]+)\s+([^<]+)</style>",
            System.Text.RegularExpressions.RegexOptions.Compiled);

    static void LoadCurrencyNames()
    {
        if (!File.Exists(EcoSavePath))
        {
            Console.WriteLine("No Game.eco found, currency IDs will be used as-is.");
            return;
        }
        RefreshCurrencyNames();
    }

    static void RefreshCurrencyNames()
    {
        if (!File.Exists(EcoSavePath)) return;
        try
        {
            var newMap = ExtractCurrencyNamesFromSave(EcoSavePath);
            if (newMap.Count == 0) return;

            int added = 0;
            lock (MetricsLock)
            {
                foreach (var kv in newMap)
                {
                    if (!CurrencyNames.ContainsKey(kv.Key)) added++;
                    CurrencyNames[kv.Key] = kv.Value;
                }
            }
            Console.WriteLine("Loaded " + CurrencyNames.Count + " currency names from Game.eco" +
                (added > 0 ? " (" + added + " new)" : ""));
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Warning: Could not extract currency names: " + ex.Message);
        }
    }

    static Dictionary<int, string> ExtractCurrencyNamesFromSave(string ecoPath)
    {
        var result = new Dictionary<int, string>();
        using var zip = ZipFile.OpenRead(ecoPath);
        var entry = zip.GetEntry("EconomyManager/Data");
        if (entry == null) return result;

        using var s = entry.Open();
        using var ms = new MemoryStream();
        s.CopyTo(ms);
        var data = ms.ToArray();

        // Use Latin1 so the regex can scan the raw bytes as 1-char-per-byte text
        // without tripping over binary (non-UTF-8) regions of the file.
        var text = Encoding.Latin1.GetString(data);

        // Tally each (id, name) pair; pick the most common name per id.
        var tallies = new Dictionary<int, Dictionary<string, int>>();
        foreach (System.Text.RegularExpressions.Match m in CurrencyTooltipRegex.Matches(text))
        {
            if (!int.TryParse(m.Groups[1].Value, out var id)) continue;
            var name = m.Groups[3].Value.Trim();
            if (string.IsNullOrEmpty(name) || name.Length > 40) continue;

            if (!tallies.TryGetValue(id, out var inner))
                tallies[id] = inner = new Dictionary<string, int>();
            inner.TryGetValue(name, out var c);
            inner[name] = c + 1;
        }

        foreach (var kv in tallies)
        {
            var best = kv.Value.OrderByDescending(x => x.Value).First();
            result[kv.Key] = best.Key;
        }
        return result;
    }

    static string ResolveCurrencyName(int id)
    {
        lock (MetricsLock)
        {
            return CurrencyNames.TryGetValue(id, out var n) ? n : "id_" + id;
        }
    }

    static void LoadState()
    {
        if (!File.Exists(StatePath))
        {
            Console.WriteLine("No state file found, starting fresh.");
            return;
        }

        try
        {
            var json = File.ReadAllText(StatePath);
            var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // Restore LastSeen positions
            if (root.TryGetProperty("LastSeen", out var lastSeenEl))
            {
                foreach (var prop in lastSeenEl.EnumerateObject())
                {
                    var val = prop.Value.GetString() ?? "";
                    if (val.StartsWith("oid:"))
                    {
                        LastSeen[prop.Name] = new ObjectId(val.Substring(4));
                    }
                    else if (val.StartsWith("int:") && long.TryParse(val.Substring(4), out var longVal))
                    {
                        LastSeen[prop.Name] = longVal;
                    }
                }
            }

            // Restore counter totals
            if (root.TryGetProperty("Counters", out var countersEl))
            {
                lock (MetricsLock)
                {
                    foreach (var prop in countersEl.EnumerateObject())
                    {
                        if (prop.Value.TryGetProperty("v", out var vEl) &&
                            prop.Value.TryGetProperty("t", out var tEl))
                        {
                            CounterMetrics[prop.Name] = (vEl.GetDouble(), tEl.GetInt64());
                        }
                    }
                }
            }

            int counterCount;
            lock (MetricsLock) { counterCount = CounterMetrics.Count; }
            Console.WriteLine("Restored state: " + LastSeen.Count + " collection positions, " +
                counterCount + " counter series from " + StatePath);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Warning: Could not load state file: " + ex.Message);
            Console.Error.WriteLine("Starting fresh.");
        }
    }

    static void SaveState()
    {
        try
        {
            var state = new Dictionary<string, object>();

            // Serialize LastSeen — prefix with type so we can deserialize correctly
            var lastSeenDict = new Dictionary<string, string>();
            foreach (var kv in LastSeen)
            {
                if (kv.Value.IsObjectId)
                    lastSeenDict[kv.Key] = "oid:" + kv.Value.AsObjectId.ToString();
                else if (kv.Value.IsInt64)
                    lastSeenDict[kv.Key] = "int:" + kv.Value.AsInt64;
                else if (kv.Value.IsInt32)
                    lastSeenDict[kv.Key] = "int:" + kv.Value.AsInt32;
                else
                    lastSeenDict[kv.Key] = kv.Value.ToString();
            }
            state["LastSeen"] = lastSeenDict;

            // Serialize counter metrics
            var countersDict = new Dictionary<string, object>();
            lock (MetricsLock)
            {
                foreach (var kv in CounterMetrics)
                {
                    countersDict[kv.Key] = new { v = kv.Value.Value, t = kv.Value.TimestampMs };
                }
            }
            state["Counters"] = countersDict;

            state["SavedAt"] = DateTime.UtcNow.ToString("o");

            var options = new JsonSerializerOptions { WriteIndented = true };
            var json = System.Text.Json.JsonSerializer.Serialize(state, options);

            // Atomic write: write to temp file, then replace
            var tmpPath = StatePath + ".tmp";
            File.WriteAllText(tmpPath, json);

            if (File.Exists(StatePath))
                File.Replace(tmpPath, StatePath, StatePath + ".bak");
            else
                File.Move(tmpPath, StatePath);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Warning: Could not save state: " + ex.Message);
        }
    }

    static Dictionary<string, string> ExtractNamesFromSave(string ecoPath)
    {
        var result = new Dictionary<string, string>();
        byte[] typeMarker = System.Text.Encoding.UTF8.GetBytes("Players.User");

        using var fs = new FileStream(ecoPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var zip = new ZipArchive(fs, ZipArchiveMode.Read);

        foreach (var entry in zip.Entries)
        {
            if (!entry.FullName.StartsWith("Users/") || entry.Length == 0)
                continue;

            byte[] data;
            using (var stream = entry.Open())
            using (var ms = new MemoryStream())
            {
                stream.CopyTo(ms);
                data = ms.ToArray();
            }

            // Extract SerializedID: it's right after "Players.User" type marker
            int markerPos = FindBytes(data, typeMarker, 0);
            if (markerPos < 0) continue;

            int idOffset = markerPos + typeMarker.Length;
            if (idOffset + 4 > data.Length) continue;

            int serializedId = BitConverter.ToInt32(data, idOffset);
            if (serializedId <= 0) continue;

            // Extract Name: search for pattern 0x00 + length + UTF8_string + 0x01 (null NameSuffix)
            string? name = FindNameInData(data);
            if (name != null)
            {
                result[serializedId.ToString()] = name;
            }
        }

        return result;
    }

    // Known false-positive strings found in the binary user data before the real Name property.
    // These come from serialized game objects (Markers, Inventory) that appear earlier
    // in the alphabetical property order than "Name" (position 24 of 57).
    // Source: MarkerFolderName enum (localized), inventory item tags/attributes.
    static readonly HashSet<string> SkippedNames = new(StringComparer.OrdinalIgnoreCase)
    {
        // MarkerFolderName enum values (localized CamelCase → split words)
        "World Objects",
        "Work Orders",
        "Contracts",
        "Tutorials",
        "None",
        // Inventory item attributes/tags
        "crops",
    };

    static string? FindNameInData(byte[] data)
    {
        // The Name property is serialized as:
        //   0x00 (not-null marker) + BinaryWriter length prefix + UTF8 bytes
        // Followed immediately by NameSuffix which is null:
        //   0x01 (null marker)
        // We scan for 0x00+len+str+0x01 and return the first plausible player name,
        // skipping known false positives like "World Objects".
        for (int i = 50; i < data.Length - 5; i++)
        {
            if (data[i] != 0x00) continue;

            int slen;
            int strStart;

            // BinaryWriter uses 7-bit encoded length
            if (data[i + 1] < 0x80)
            {
                slen = data[i + 1];
                strStart = i + 2;
            }
            else if (i + 2 < data.Length)
            {
                slen = (data[i + 1] & 0x7F) | (data[i + 2] << 7);
                strStart = i + 3;
            }
            else continue;

            if (slen < 2 || slen > 40) continue;
            int strEnd = strStart + slen;
            if (strEnd >= data.Length) continue;
            if (data[strEnd] != 0x01) continue; // null NameSuffix marker

            try
            {
                string candidate = Encoding.UTF8.GetString(data, strStart, slen);
                if (candidate.Length >= 2 && IsPlausiblePlayerName(candidate) && !SkippedNames.Contains(candidate))
                    return candidate;
            }
            catch { }
        }

        return null;
    }

    static bool IsPlausiblePlayerName(string name)
    {
        // Must be printable ASCII only (letters, digits, spaces, common punctuation).
        // Rejects non-ASCII bytes that decode as garbled UTF-8 characters.
        foreach (char c in name)
        {
            if (c < 0x20 || c > 0x7E) return false;
        }
        // Must contain at least one letter or digit (rejects pure punctuation)
        foreach (char c in name)
        {
            if (char.IsLetterOrDigit(c)) return true;
        }
        return false;
    }

    static int FindBytes(byte[] haystack, byte[] needle, int start)
    {
        for (int i = start; i <= haystack.Length - needle.Length; i++)
        {
            bool match = true;
            for (int j = 0; j < needle.Length; j++)
            {
                if (haystack[i + j] != needle[j])
                {
                    match = false;
                    break;
                }
            }
            if (match) return i;
        }
        return -1;
    }

    static string ResolveCitizenName(string citizenId)
    {
        return CitizenNames.TryGetValue(citizenId, out var name) ? name : citizenId;
    }

    static string SanitizeMetricName(string name)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsLetterOrDigit(c))
            {
                if (char.IsUpper(c) && i > 0 && char.IsLower(name[i - 1]))
                    sb.Append('_');
                sb.Append(char.ToLower(c));
            }
            else if (c == '_')
            {
                sb.Append('_');
            }
        }
        var result = sb.ToString().Trim('_');
        if (result.Length > 0 && char.IsDigit(result[0]))
            result = "_" + result;
        return result;
    }

    static string GetMetricName(string collectionShortName)
    {
        var baseName = LongNames.TryGetValue(collectionShortName, out var longName)
            ? longName
            : collectionShortName;
        return "eco_" + SanitizeMetricName(baseName);
    }

    static CollectionType ClassifyCollection(ILiteCollection<BsonDocument> col)
    {
        var sample = col.FindAll().Take(1).FirstOrDefault();
        if (sample == null) return CollectionType.Skip;

        var keys = sample.Keys.ToList();

        if (keys.Count == 2 && keys.Contains("_id") && keys.Contains("Value")
            && (sample["_id"].IsInt32 || sample["_id"].IsInt64))
        {
            return CollectionType.Timeseries;
        }

        if (sample.ContainsKey("Time") && sample.ContainsKey("Citizen") && sample.ContainsKey("Value"))
        {
            return CollectionType.CitizenStat;
        }

        if (sample.ContainsKey("Time"))
        {
            return CollectionType.Event;
        }

        return CollectionType.Skip;
    }

    static void PollDatabase()
    {
        try
        {
            using var db = new LiteDatabase("Filename=" + DbPath + ";ReadOnly=true");
            var collections = db.GetCollectionNames()
                .Where(n => n != "index" && n != "enums" && !n.StartsWith("$"))
                .ToList();

            int newDocsTotal = 0;

            foreach (var name in collections)
            {
                try
                {
                    var col = db.GetCollection(name);
                    if (col.Count() == 0) continue;

                    var type = ClassifyCollection(col);
                    if (type == CollectionType.Skip) continue;

                    var metricName = GetMetricName(name);

                    // Skip metrics the user doesn't need
                    if (SkippedMetrics.Contains(metricName)) continue;

                    List<BsonDocument> newDocs;

                    if (type == CollectionType.Timeseries)
                    {
                        if (LastSeen.TryGetValue(name, out var lastId))
                        {
                            newDocs = col.Query()
                                .Where(x => x["_id"] > lastId)
                                .OrderBy("_id")
                                .ToList();
                        }
                        else
                        {
                            newDocs = col.Query().OrderBy("_id").ToList();
                        }

                        if (newDocs.Count > 0)
                        {
                            // Only keep the latest value — it's a gauge
                            var latest = newDocs.Last();
                            var gameTime = latest["_id"].AsInt64;
                            var value = GetNumericValue(latest["Value"]);

                            lock (MetricsLock)
                            {
                                GaugeMetrics[metricName] = (value, gameTime * 1000);
                            }

                            LastSeen[name] = latest["_id"];
                            newDocsTotal += newDocs.Count;
                        }
                    }
                    else if (type == CollectionType.CitizenStat)
                    {
                        if (LastSeen.TryGetValue(name, out var lastOid))
                        {
                            newDocs = col.Query()
                                .Where(x => x["_id"] > lastOid)
                                .OrderBy("_id")
                                .ToList();
                        }
                        else
                        {
                            newDocs = col.Query().OrderBy("_id").ToList();
                        }

                        if (newDocs.Count > 0)
                        {
                            // Group by citizen, keep only the latest per citizen
                            var latestPerCitizen = new Dictionary<string, BsonDocument>();
                            foreach (var doc in newDocs)
                            {
                                var citizen = doc["Citizen"].ToString();
                                latestPerCitizen[citizen] = doc;
                            }

                            lock (MetricsLock)
                            {
                                foreach (var kv in latestPerCitizen)
                                {
                                    var doc = kv.Value;
                                    var citizen = kv.Key;
                                    var gameTime = (long)GetNumericValue(doc["Time"]);
                                    var value = GetNumericValue(doc["Value"]);

                                    var seriesKey = metricName + "_value{citizen=\"" + citizen + "\",citizen_name=\"" + EscapeLabelValue(ResolveCitizenName(citizen)) + "\"}";
                                    GaugeMetrics[seriesKey] = (value, gameTime * 1000);

                                    if (doc.ContainsKey("Count"))
                                    {
                                        var count = GetNumericValue(doc["Count"]);
                                        var countKey = metricName + "_count{citizen=\"" + citizen + "\",citizen_name=\"" + EscapeLabelValue(ResolveCitizenName(citizen)) + "\"}";
                                        GaugeMetrics[countKey] = (count, gameTime * 1000);
                                    }
                                }
                            }

                            LastSeen[name] = newDocs.Last()["_id"];
                            newDocsTotal += newDocs.Count;
                        }
                    }
                    else if (type == CollectionType.Event)
                    {
                        if (LastSeen.TryGetValue(name, out var lastOid))
                        {
                            newDocs = col.Query()
                                .Where(x => x["_id"] > lastOid)
                                .OrderBy("_id")
                                .ToList();
                        }
                        else
                        {
                            newDocs = col.Query().OrderBy("_id").ToList();
                        }

                        if (newDocs.Count > 0)
                        {
                            // For events, accumulate count into a running counter per label set
                            var includeCitizen = CitizenLabeledMetrics.Contains(metricName);
                            var isCurrencyTrade = metricName == "eco_currency_trade";

                            foreach (var doc in newDocs)
                            {
                                var gameTime = (long)GetNumericValue(doc["Time"]);
                                var count = doc.ContainsKey("Count") ? GetNumericValue(doc["Count"]) : 1;

                                var labels = new List<string>();

                                // For resource gathering metrics, resolve and include the citizen
                                if (includeCitizen && doc.ContainsKey("Citizen"))
                                {
                                    var citizenId = doc["Citizen"].ToString();
                                    var citizenName = ResolveCitizenName(citizenId);
                                    labels.Add("citizen=\"" + EscapeLabelValue(citizenId) + "\"");
                                    labels.Add("citizen_name=\"" + EscapeLabelValue(citizenName) + "\"");
                                }

                                // Currency trades: promote a couple of Int32 ID fields to
                                // meaningful string labels (BoughtOrSold enum, Currency name).
                                if (isCurrencyTrade)
                                {
                                    if (doc.ContainsKey("BoughtOrSold"))
                                    {
                                        var bosInt = doc["BoughtOrSold"].AsInt32;
                                        var bosLabel = BoughtOrSoldLabels.TryGetValue(bosInt, out var b)
                                            ? b : "unknown_" + bosInt;
                                        labels.Add("bought_or_sold=\"" + bosLabel + "\"");
                                    }
                                    if (doc.ContainsKey("Currency"))
                                    {
                                        var currencyId = doc["Currency"].AsInt32;
                                        labels.Add("currency=\"" + EscapeLabelValue(ResolveCurrencyName(currencyId)) + "\"");
                                    }
                                }

                                foreach (var key in doc.Keys)
                                {
                                    if (key == "_id" || key == "Time" || key == "Count" || key == "Citizen") continue;
                                    if (ExcludedEventLabels.Contains(key)) continue;
                                    var val = doc[key];
                                    // Only include string fields as labels — numeric fields
                                    // are IDs (Currency, BankAccount, etc.) and not
                                    // useful as Prometheus labels
                                    if (val.IsString && val.AsString.Length > 0)
                                    {
                                        // Skip coordinate labels (e.g. "2562,63,1456") — they cause
                                        // massive cardinality explosion and aren't useful for aggregation
                                        if (IsCoordinateValue(val.AsString)) continue;
                                        labels.Add(SanitizeMetricName(key) + "=\"" + EscapeLabelValue(val.AsString) + "\"");
                                    }
                                }

                                var labelStr = labels.Count > 0 ? "{" + string.Join(",", labels) + "}" : "";
                                var seriesKey = metricName + "_total" + labelStr;

                                lock (MetricsLock)
                                {
                                    if (CounterMetrics.TryGetValue(seriesKey, out var existing))
                                        CounterMetrics[seriesKey] = (existing.Value + count, gameTime * 1000);
                                    else
                                        CounterMetrics[seriesKey] = (count, gameTime * 1000);

                                    // Currency trades get two extra counters: sum of items moved
                                    // and sum of currency moved, keyed by the same label set.
                                    if (isCurrencyTrade)
                                    {
                                        var itemsDelta = doc.ContainsKey("NumberOfItems")
                                            ? GetNumericValue(doc["NumberOfItems"]) : 0;
                                        var currencyDelta = doc.ContainsKey("CurrencyAmount")
                                            ? GetNumericValue(doc["CurrencyAmount"]) : 0;

                                        var itemsKey = metricName + "_items_total" + labelStr;
                                        if (CounterMetrics.TryGetValue(itemsKey, out var iex))
                                            CounterMetrics[itemsKey] = (iex.Value + itemsDelta, gameTime * 1000);
                                        else
                                            CounterMetrics[itemsKey] = (itemsDelta, gameTime * 1000);

                                        var currencyKey = metricName + "_currency_total" + labelStr;
                                        if (CounterMetrics.TryGetValue(currencyKey, out var cex))
                                            CounterMetrics[currencyKey] = (cex.Value + currencyDelta, gameTime * 1000);
                                        else
                                            CounterMetrics[currencyKey] = (currencyDelta, gameTime * 1000);
                                    }
                                }
                            }

                            LastSeen[name] = newDocs.Last()["_id"];
                            newDocsTotal += newDocs.Count;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("Warning: Error processing collection '" + name + "': " + ex.Message);
                }
            }

            if (newDocsTotal > 0)
            {
                int totalSeries;
                lock (MetricsLock)
                {
                    totalSeries = GaugeMetrics.Count + CounterMetrics.Count;
                }
                Console.WriteLine("[" + DateTime.Now.ToString("HH:mm:ss") + "] Polled " + newDocsTotal + " new docs, serving " + totalSeries + " metric series");
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Error polling database: " + ex.Message);
        }
    }

    static double GetNumericValue(BsonValue val)
    {
        if (val.IsDouble) return val.AsDouble;
        if (val.IsInt32) return val.AsInt32;
        if (val.IsInt64) return val.AsInt64;
        if (val.IsDecimal) return (double)val.AsDecimal;
        return 0;
    }

    static string FormatDouble(double val)
    {
        if (double.IsPositiveInfinity(val)) return "+Inf";
        if (double.IsNegativeInfinity(val)) return "-Inf";
        if (double.IsNaN(val)) return "NaN";
        return val.ToString("G", CultureInfo.InvariantCulture);
    }

    static string EscapeLabelValue(string val)
    {
        return val.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n");
    }

    // Extract the metric family name (everything before '{') from a series key.
    // e.g. "eco_currency_trade_total{bought_or_sold=\"Selling\",...}" -> "eco_currency_trade_total"
    static string ExtractMetricFamily(string seriesKey)
    {
        int idx = seriesKey.IndexOf('{');
        return idx >= 0 ? seriesKey.Substring(0, idx) : seriesKey;
    }

    static bool IsCoordinateValue(string val)
    {
        // Matches patterns like "2562,63,1456" or "(2562, 63, 1456)" — XYZ world coordinates
        // that cause cardinality explosion in event counter labels
        var stripped = val.Replace("(", "").Replace(")", "").Replace(" ", "");
        var parts = stripped.Split(',');
        if (parts.Length < 2) return false;
        foreach (var part in parts)
        {
            if (!double.TryParse(part, NumberStyles.Any, CultureInfo.InvariantCulture, out _))
                return false;
        }
        return true;
    }

    static int PollCount = 0;

    static void PollLoop()
    {
        while (true)
        {
            Thread.Sleep(PollIntervalSeconds * 1000);
            PollDatabase();
            SaveState();

            // Refresh citizen names every ~10 polls (5 minutes at 30s interval)
            // to pick up new players
            PollCount++;
            if (PollCount % 10 == 0)
            {
                RefreshCitizenNames();
                RefreshCurrencyNames();
            }
        }
    }

    static void RunHttpServer()
    {
        var listener = new HttpListener();
        listener.Prefixes.Add(ListenPrefix);

        try
        {
            listener.Start();
        }
        catch (HttpListenerException ex)
        {
            Console.Error.WriteLine("Failed to start HTTP listener on " + ListenPrefix + ": " + ex.Message);
            Console.Error.WriteLine("On Linux, try a port > 1024 or run with elevated privileges.");
            Console.Error.WriteLine("On Windows, you may need: netsh http add urlacl url=http://+:9101/ user=Everyone");
            return;
        }

        Console.WriteLine("HTTP server listening on " + ListenPrefix);
        Console.WriteLine("Endpoints:");
        Console.WriteLine("  /metrics  - Prometheus metrics");
        Console.WriteLine("  /health   - Health check");
        Console.WriteLine("  /status   - Collection status and counts");
        Console.WriteLine();
        Console.WriteLine("Press Ctrl+C to stop.");

        while (true)
        {
            try
            {
                var context = listener.GetContext();
                var request = context.Request;
                var response = context.Response;

                switch (request.Url?.AbsolutePath)
                {
                    case "/metrics":
                        ServeMetrics(response);
                        break;
                    case "/health":
                        ServeText(response, "ok\n", 200);
                        break;
                    case "/status":
                        ServeStatus(response);
                        break;
                    default:
                        ServeText(response, "Eco Metrics Exporter\n\nEndpoints:\n  /metrics\n  /health\n  /status\n", 200);
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("HTTP error: " + ex.Message);
            }
        }
    }

    static void ServeMetrics(HttpListenerResponse response)
    {
        string body;
        lock (MetricsLock)
        {
            var sb = new StringBuilder();

            // Gauge metrics (timeseries + citizen stats)
            string? currentFamily = null;
            foreach (var kv in GaugeMetrics.OrderBy(x => x.Key))
            {
                var family = ExtractMetricFamily(kv.Key);
                if (family != currentFamily)
                {
                    if (MetricHelp.TryGetValue(family, out var help))
                    {
                        sb.AppendLine("# HELP " + family + " " + help);
                        sb.AppendLine("# TYPE " + family + " gauge");
                    }
                    currentFamily = family;
                }
                sb.AppendLine(kv.Key + " " + FormatDouble(kv.Value.Value) + " " + kv.Value.TimestampMs);
            }

            // Counter metrics (events)
            currentFamily = null;
            foreach (var kv in CounterMetrics.OrderBy(x => x.Key))
            {
                var family = ExtractMetricFamily(kv.Key);
                if (family != currentFamily)
                {
                    if (MetricHelp.TryGetValue(family, out var help))
                    {
                        sb.AppendLine("# HELP " + family + " " + help);
                        sb.AppendLine("# TYPE " + family + " counter");
                    }
                    currentFamily = family;
                }
                sb.AppendLine(kv.Key + " " + FormatDouble(kv.Value.Value) + " " + kv.Value.TimestampMs);
            }

            body = sb.ToString();
        }

        response.ContentType = "text/plain; version=0.0.4; charset=utf-8";
        var bytes = Encoding.UTF8.GetBytes(body);
        response.ContentLength64 = bytes.Length;
        response.OutputStream.Write(bytes, 0, bytes.Length);
        response.OutputStream.Close();
    }

    static void ServeStatus(HttpListenerResponse response)
    {
        int gaugeCount, counterCount;
        lock (MetricsLock)
        {
            gaugeCount = GaugeMetrics.Count;
            counterCount = CounterMetrics.Count;
        }

        var sb = new StringBuilder();
        sb.AppendLine("Eco Metrics Exporter Status");
        sb.AppendLine("Database: " + DbPath);
        sb.AppendLine("Poll interval: " + PollIntervalSeconds + "s");
        sb.AppendLine("Gauge series:   " + gaugeCount);
        sb.AppendLine("Counter series: " + counterCount);
        sb.AppendLine("Total series:   " + (gaugeCount + counterCount));
        sb.AppendLine("Collections tracked: " + LastSeen.Count);
        sb.AppendLine();
        foreach (var kv in LastSeen.OrderBy(x => x.Key))
        {
            var metricName = GetMetricName(kv.Key);
            sb.AppendLine("  " + kv.Key.PadRight(10) + " (" + metricName + ") last_id=" + kv.Value);
        }
        ServeText(response, sb.ToString(), 200);
    }

    static void ServeText(HttpListenerResponse response, string text, int statusCode)
    {
        response.StatusCode = statusCode;
        response.ContentType = "text/plain; charset=utf-8";
        var bytes = Encoding.UTF8.GetBytes(text);
        response.ContentLength64 = bytes.Length;
        response.OutputStream.Write(bytes, 0, bytes.Length);
        response.OutputStream.Close();
    }
}

enum CollectionType
{
    Skip,
    Timeseries,
    CitizenStat,
    Event
}
