# Cricket Knowledge Graph

**36K nodes. 1.4M edges. Every ball from 21,324 matches -- Tests, ODIs, T20s, IPL, BBL, and more.**

> Part of the **Samyama** ecosystem — loaded into and queried via the graph engine at [samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph).
> This repo holds the loader and source-data specifics for the KG.

<a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue" alt="License"></a>

---

We loaded ball-by-ball data from every Cricsheet match, then asked:

> *"Who has scored the most runs across all formats?"*

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
RETURN p.name AS player, sum(b.runs) AS runs, sum(b.sixes) AS sixes
ORDER BY runs DESC LIMIT 5
```

| Player | Runs | Sixes |
|--------|------|-------|
| **V Kohli** | **36,500** | 607 |
| KC Sangakkara | 30,118 | 278 |
| DA Warner | 27,903 | 613 |
| RG Sharma | 26,691 | 937 |
| AB de Villiers | 26,272 | 662 |

**Flat stat tables give you numbers. A graph gives you connections** -- dismissal networks, partnership structures, cross-tournament player journeys. Powered by [Samyama Graph](https://github.com/samyama-ai/samyama-graph).

---

## Schema

**6 node labels** -- Player (12,933), Match (21,324), Tournament (1,053), Venue (877), Team (383), Season (49)

**12 edge types** -- BATTED_IN, BOWLED_IN, DISMISSED, FIELDED_DISMISSAL, PLAYED_FOR, COMPETED_IN, WON, WON_TOSS, HOSTED_AT, PART_OF, IN_SEASON, PLAYER_OF_MATCH

**Data source** -- [Cricsheet.org](https://cricsheet.org/) (CC-BY-4.0), 21,325 JSON files with ball-by-ball granularity

## Quick Start

### Load from snapshot (recommended)

```bash
# Download (21 MB)
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/cricket.sgsnap

# Start Samyama and import
./target/release/samyama
curl -X POST http://localhost:8080/api/tenants \
  -H 'Content-Type: application/json' \
  -d '{"id":"cricket","name":"Cricket KG"}'
curl -X POST http://localhost:8080/api/tenants/cricket/snapshot/import \
  -F "file=@cricket.sgsnap"
```

### Build from source

```bash
git clone https://github.com/samyama-ai/cricket-kg.git && cd cricket-kg
pip install -e ".[dev]"
mkdir -p data && curl -LO https://cricsheet.org/downloads/all_json.zip
unzip -q all_json.zip -d data/json
python -m etl.loader --data-dir data/json               # All 21,324 matches (~24 min)
python -m etl.loader --data-dir data/json --max-matches 500 --match-type T20  # Quick test
```

## Example Queries

```cypher
-- Dismissal network: who gets whom out?
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
RETURN bowler.name, batsman.name, count(d) AS times
ORDER BY times DESC LIMIT 5
-- Broad -> Warner (20), Ashwin -> Stokes (17), Ashwin -> Warner (17)

-- Player of the Match awards
MATCH (p:Player)-[:PLAYER_OF_MATCH]->(m:Match)
RETURN p.name, count(m) AS awards ORDER BY awards DESC LIMIT 5
-- V Kohli (88), AB de Villiers (71), CH Gayle (69)
```

See the full **[100-query showcase](docs/100-queries.md)** -- from single-table aggregations to network intelligence that SQL cannot express.

## Links

| | |
|---|---|
| Samyama Graph | [github.com/samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph) |
| The Book | [samyama-ai.github.io/samyama-graph-book](https://samyama-ai.github.io/samyama-graph-book/) |
| Cricsheet | [cricsheet.org](https://cricsheet.org/) |
| Contact | [samyama.dev/contact](https://samyama.dev/contact) |

## License

Apache 2.0. Data from Cricsheet.org is CC-BY-4.0.
