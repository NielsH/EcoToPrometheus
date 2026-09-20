# Changelog

## 2.0.0 (unreleased)

Rewritten as an in-process Eco 0.14 server mod. The v1 external LiteDB poller is gone.

- Scrape-only Prometheus text endpoint on the Eco web API, plus JSON and status routes.
- Counters from a global GameAction listener (83 curated families in Eco 0.14.1, automatic
  rules for modded actions), companion value counters, food-eaten events.
- Gauges copied from Eco's own periodic stats: species populations, climate, global economy
  and civics, plus live server gauges.
- Counter state persisted to `Storage/<SaveName>.metrics.json` with new-world detection.
- API-key filter that also refuses an empty key and an unconfigured token; optional anonymous
  mode; gzip.
- Admin chat command `/metrics status|reset|detach|attach`.
- Unit tests for the Eco-free core (94 tests).
