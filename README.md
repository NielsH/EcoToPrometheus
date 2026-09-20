# EcoToPrometheus v2

Eco 0.14 server mod (single DLL) that counts game events in-process and serves them as a
Prometheus text endpoint on the Eco web API. Prometheus scrapes the game server directly;
no external exporter, no Telegraf.

**Status: phase 1 skeleton.** The plugin loads, materialises `Configs/Metrics.eco`, runs its
worker, and answers `503` on `/metrics` until the exposition writer lands in phase 2.

## Endpoints

| Route | Purpose |
| --- | --- |
| `GET /api/v1/plugins/Metrics/metrics` | Prometheus text exposition |
| `GET /api/v1/plugins/Metrics/json` | Same snapshot as JSON |
| `GET /api/v1/plugins/Metrics/status` | Worker health |

Requests need Eco's API token (`X-API-Key` header or `api_key` query parameter, from
`Configs/Users.eco` `APIAuthToken`) unless `AllowAnonymous` is set in `Configs/Metrics.eco`.

## Build

Requires the .NET 10 SDK.

```bash
dotnet build -c Release
```

Output: `src/EcoToPrometheus/bin/Release/net10.0/EcoToPrometheus.dll` (the only file needed).
The `Eco.ReferenceAssemblies` pin in the csproj must match the server's version (`/info` → `Version`).

## Install

Copy `EcoToPrometheus.dll` anywhere under the server's `Mods/` folder and restart the server.
`Configs/Metrics.eco` is created with defaults on first start.

## v1 (pre-0.14)

The original external exporter that polled `Game.db` with LiteDB lives in git history up to
tag `v1`. It does not work with Eco 0.14 and is not maintained.
