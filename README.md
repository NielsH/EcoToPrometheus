# EcoToPrometheus v2

Eco 0.14 server mod (one DLL) that counts game events in-process and serves them as a
Prometheus text endpoint on the Eco web API. Prometheus scrapes the game server directly.
No external exporter, no database polling, no Telegraf.

- Every stat-producing GameAction (83 in Eco 0.14.1) becomes a counter with sensible labels,
  plus companion value counters (items and currency traded, calories, playtime, taxes, ...).
- Eco's own periodic stats (species populations, climate, the global economy and civics
  gauges) are copied when Eco computes them, never recomputed.
- Counters persist across restarts in `Storage/<SaveName>.metrics.json`; a regenerated world
  is detected and starts from zero.
- The hook that runs on the game thread only enqueues; aggregation, name resolution and disk
  writes run on one low-priority worker. Every entry point is isolated, so a bug in the mod
  cannot take the server down.

## Endpoints

| Route | Purpose |
| --- | --- |
| `GET /api/v1/plugins/Metrics/metrics` | Prometheus text exposition (gzip when accepted) |
| `GET /api/v1/plugins/Metrics/json` | Same snapshot as JSON |
| `GET /api/v1/plugins/Metrics/status` | Worker and state health |

Requests need Eco's API token (`X-API-Key` header or `api_key` query parameter, the
`APIAuthToken` or `APIAdminAuthToken` from `Configs/Users.eco`) unless `AllowAnonymous` is set.
The mod refuses an empty key and refuses everything while no token is configured, which Eco's
own handler does not; set a token before exposing port 3001.

Admin chat command: `/metrics status`, `/metrics reset`, `/metrics detach`, `/metrics attach`.

## Configuration

`Configs/Metrics.eco` is created with these defaults on first start. It is also editable in the
server GUI and through Eco's `POST /api/v1/plugins/config/Metrics`; label rules apply on the
next worker tick, the other settings after a restart.

| Setting | Default | Meaning |
| --- | --- | --- |
| `Enabled` | `true` | Master switch. |
| `AllowAnonymous` | `false` | Serve without an API key. |
| `WorkerIntervalSeconds` | `1` | Queue drain and cheap gauge cadence. |
| `SlowGaugeIntervalSeconds` | `30` | Currencies, settlements, laws. |
| `WorldObjectGaugeIntervalSeconds` | `300` | World-object count. |
| `StateSaveIntervalSeconds` | `60` | Counter persistence cadence. |
| `PlayerLabelFamilies` | chop, mine, harvest, plant, craft, trade, specialties, level-ups, Play | GameAction types whose counters carry `player`. `FoodEaten` is accepted too. |
| `ExcludedFamilies` | `ChatSent` | Never counted. |
| `GaugeGroups` | `Global, Species, Climate, Live` | Gauge groups to sample. |
| `MaxLabelValueLength` | `64` | Longer label values are truncated. |
| `QueueWarnThreshold` | `100000` | Backlog that logs a warning. |
| `LogLevel` | `Normal` | `Quiet`, `Normal` or `Verbose`. |

## Metrics

Naming: `eco_action_<snake_case>_total` for every GameAction (`ChopTree` becomes
`eco_action_chop_tree_total`), curated value counters such as `eco_trade_currency_total`,
`eco_playtime_seconds_total` and `eco_calories_consumed_total`, gauges such as
`eco_players_online`, `eco_species_population{species,kind}`, `eco_climate_co2_ppm`,
`eco_citizens`, `eco_gdp`, per-player skill gauges (`eco_player_specialty_level{player,specialty}`,
`eco_player_stars_earned`, `eco_player_skill_rate` with its food and housing parts in
`eco_player_skill_rate_bonus{source}`), and `eco_exporter_*` self-metrics. Every family carries HELP and
TYPE, so the endpoint documents itself:

```bash
curl -s -H "X-API-Key: $KEY" http://localhost:3001/api/v1/plugins/Metrics/metrics | grep '^# HELP'
```

Label values for items, species and skills are the C# type name minus the `Item`, `Species`
or `Skill` suffix (`IronAxe`, `Oak`, `Logging`). Players are display names. Other players in a
transaction, positions and free text are never labels.

## Prometheus

`tools/prometheus.example.yml` has a complete job. The essentials:

```yaml
scrape_configs:
  - job_name: eco
    scrape_interval: 30s
    metrics_path: /api/v1/plugins/Metrics/metrics
    http_headers:
      X-API-Key:
        files: [/etc/prometheus/secrets/eco_api_key]
    static_configs:
      - targets: ["gameserver:3001"]
```

Useful queries: `sum by (player) (increase(eco_action_chop_tree_total[1h]))`,
`rate(eco_trade_currency_total[1h]) / rate(eco_trade_items_total[1h])` for average prices,
`eco_players_online`, `time() - eco_exporter_state_last_save_timestamp_seconds` for an alert.

## Build

Requires the .NET 10 SDK.

```bash
dotnet build -c Release
dotnet test tests/EcoToPrometheus.Tests -c Release
```

Output: `src/EcoToPrometheus/bin/Release/net10.0/EcoToPrometheus.dll`, the only file needed.
The `Eco.ReferenceAssemblies` pin in the csproj must match the server's version (`/info` reports
`Version`). Bump the pin and rebuild when the server updates.

## Install

Copy `EcoToPrometheus.dll` anywhere under the server's `Mods/` folder and restart the server.
`Configs/Metrics.eco` appears on first start; `Storage/<SaveName>.metrics.json` after the first
save interval. To remove: delete the DLL and restart. The state file can stay.

## Layout

- `src/EcoToPrometheus/Core/` has no Eco dependency and is unit-tested (registry, worker,
  exposition, state file, naming).
- `src/EcoToPrometheus/Hooks/` is the catalogue (`FamilyRules.cs`), the listener, gauge sources
  and name resolution.
- `src/EcoToPrometheus/Web/` is the controller and the API-key filter.
- `tools/rig/MetricsRigCommands.cs` is a test-only chat command for a local server; it is not
  part of the DLL.

## v1

The original external exporter that polled `Game.db` with LiteDB lives in git history before
the v2 commits. It does not work with Eco 0.14 and is not maintained. Metric names changed in
v2, so dashboards built on v1 need to be rebuilt.
