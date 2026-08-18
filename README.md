# Cricket Knowledge Graph

**36K nodes. 1.4M edges. Every ball from 21,324 matches -- Tests, ODIs, T20s, IPL, BBL, and more.**

> Part of the **Samyama** ecosystem — loaded into and queried via the graph engine at [samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph).
> This repo holds the loader and source-data specifics for the KG.

<a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue" alt="License"></a>
<a href="https://huggingface.co/datasets/VaidhyaMegha/cricket-kg"><img src="https://img.shields.io/badge/%F0%9F%A4%97%20dataset-VaidhyaMegha%2Fcricket--kg-yellow" alt="HuggingFace dataset"></a>

**The built graph is published as a dataset** -- you do not have to run the ETL to get it:
**[huggingface.co/datasets/VaidhyaMegha/cricket-kg](https://huggingface.co/datasets/VaidhyaMegha/cricket-kg)**
(`v1.0`, CC-BY-4.0). 36,619 nodes and 1,392,017 edges as node/edge CSVs, plus
`cricket.sgsnap` for a one-step engine import. This repository holds the **code**; that
dataset holds the **data**; the snapshot is the **graph**.

```python
from datasets import load_dataset
batting = load_dataset("VaidhyaMegha/cricket-kg", "edge_batted_in", revision="v1.0")
```

> **Using the data means crediting Cricsheet.** CC-BY-4.0 makes attribution a condition of
> use, and it travels to anything you redistribute or build on top of.

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

## Documentation

New here? Start with the guides:

| Guide | What it covers |
|-------|----------------|
| **[GETTING_STARTED.md](GETTING_STARTED.md)** | prerequisites (Python ≥ 3.10) · install · run the engine (Docker) · load the graph · first query |
| **[docs/QUERYING.md](docs/QUERYING.md)** | ask questions via **MCP (Claude)**, the **HTTP API**, or the **Samyama CLI** |
| [docs/100-queries.md](docs/100-queries.md) | 100 example Cypher queries |

---

## Schema

**6 node labels** -- Player (12,933), Match (21,324), Tournament (1,053), Venue (877), Team (383), Season (49)

**12 edge types** -- BATTED_IN, BOWLED_IN, DISMISSED, FIELDED_DISMISSAL, PLAYED_FOR, COMPETED_IN, WON, WON_TOSS, HOSTED_AT, PART_OF, IN_SEASON, PLAYER_OF_MATCH

**Data source** -- [Cricsheet.org](https://cricsheet.org/) (CC-BY-4.0), 21,325 JSON files with ball-by-ball granularity

## Quick Start

**Full walkthrough → [GETTING_STARTED.md](GETTING_STARTED.md)** (prerequisites, Docker, loading, querying).

Fastest path — run the engine and import the published snapshot into the `cricket` tenant
(needs **Python ≥ 3.10** for the tooling and **Docker** for the engine):

```bash
pip install -r requirements.txt
docker run --rm -p 8080:8080 -p 6379:6379 public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/cricket.sgsnap  # 21 MB
curl -X POST http://localhost:8080/api/tenants -H 'Content-Type: application/json' -d '{"id":"cricket","name":"Cricket KG"}'
curl -X POST http://localhost:8080/api/tenants/cricket/snapshot/import -F "file=@cricket.sgsnap"
```

Prefer to build from Cricsheet instead of the snapshot? See
[GETTING_STARTED.md](GETTING_STARTED.md) §4. To query it (Claude / HTTP / CLI), see
[docs/QUERYING.md](docs/QUERYING.md).

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
| **Published dataset** | **[huggingface.co/datasets/VaidhyaMegha/cricket-kg](https://huggingface.co/datasets/VaidhyaMegha/cricket-kg)** |
| Samyama Graph | [github.com/samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph) |
| The Book | [samyama-ai.github.io/samyama-graph-book](https://samyama-ai.github.io/samyama-graph-book/) |
| Cricsheet | [cricsheet.org](https://cricsheet.org/) |
| Contact | [samyama.dev/contact](https://samyama.dev/contact) |

## License

Apache 2.0 covers the **code** in this repository. The **data** is a separate matter: it comes
from [Cricsheet.org](https://cricsheet.org/) under
**[CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/)**, and CC-BY-4.0 -- not Apache
2.0 -- governs the [published dataset](https://huggingface.co/datasets/VaidhyaMegha/cricket-kg)
and any redistribution of it.

**Attribution is required.** Anything built on this data must credit Cricsheet:

> Source data from [Cricsheet.org](https://cricsheet.org/), licensed
> [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/).
