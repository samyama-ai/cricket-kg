# Getting Started — Cricket Knowledge Graph

From `git clone` to your first answer. The **snapshot path** is the fastest (a few minutes).

---

## 1. Prerequisites

- **Python ≥ 3.10** (required by the `samyama` SDK; macOS ships 3.9 — use `python3.10`+).
- **git**
- **Docker** — to run the Samyama engine (needed for the snapshot import and for serving MCP / CLI / API).

## 2. Install

```bash
git clone https://github.com/samyama-ai/cricket-kg.git
cd cricket-kg
python3 -m venv .venv && source .venv/bin/activate     # Python >= 3.10
pip install -r requirements.txt                         # note: pulls torch (~large) for semantic search
```

## 3. Run the engine (Docker)

```bash
docker run --rm -p 8080:8080 -p 6379:6379 public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0
```

## 4. Load the graph — into the `cricket` tenant

### Option A — snapshot (recommended, ~seconds)
```bash
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/cricket.sgsnap   # 21 MB
curl -X POST http://localhost:8080/api/tenants -H 'Content-Type: application/json' \
  -d '{"id":"cricket","name":"Cricket KG"}'
curl -X POST http://localhost:8080/api/tenants/cricket/snapshot/import -F "file=@cricket.sgsnap"
```

### Option B — build from source (from Cricsheet)
```bash
mkdir -p data && curl -LO https://cricsheet.org/downloads/all_json.zip
unzip -q all_json.zip -d data/json
python -m etl.loader --data-dir data/json --url http://localhost:8080                 # all 21,324 matches (~24 min)
python -m etl.loader --data-dir data/json --url http://localhost:8080 --max-matches 500 --match-type T20  # quick test
```
*(Both load into the `cricket` tenant. Omit `--url` to build an in-memory graph instead.)*

## 5. Ask your first question

Fastest is **Claude over MCP** — see **[docs/QUERYING.md](docs/QUERYING.md)**. Quick check over HTTP:

```bash
curl -s -X POST http://localhost:8080/api/query -H 'Content-Type: application/json' -d '{
  "graph": "cricket",
  "query": "MATCH (b:Player)-[:DISMISSED]->(bat:Player {name:\"V Kohli\"}) RETURN b.name AS bowler, count(*) AS times ORDER BY times DESC LIMIT 3"
}'
# → TG Southee (12), MM Ali (11), JR Hazlewood (11)
```

## 6. The ETL pipeline

- Data source: **Cricsheet** ball-by-ball JSON (`all_json.zip`).
- `etl/loader.py` — builds the graph (Match, Player, Team, Tournament, Venue, Season + BATTED_IN / BOWLED_IN / DISMISSED / …). Run `python -m etl.loader --help` for options.

## Next
- **[docs/QUERYING.md](docs/QUERYING.md)** — MCP (Claude), HTTP API, and the Samyama CLI
- **[docs/100-queries.md](docs/100-queries.md)** — 100 example queries · **[README](README.md#schema)** — schema
