# Cricket Knowledge Graph

![Language](https://img.shields.io/badge/language-Python-3776AB)


A knowledge graph of ball-by-ball cricket data from [Cricsheet.org](https://cricsheet.org/) — powered by [Samyama Graph Database](https://github.com/samyama-ai/samyama-graph).

**21,324 matches** | **36,619 nodes** | **1.37M edges** | **12,933 players** | **Ball-by-ball granularity** | **All formats: Tests, ODIs, T20s, IPL, BBL, CPL, PSL, The Hundred, ...**

## Why a Graph?

ESPN Cricinfo gives you flat stat tables. A knowledge graph gives you **connections**: dismissal networks, partnership structures, cross-tournament player journeys, and multi-hop questions that flat databases can't answer.

| Flat stats (Cricinfo) | Graph (cricket-kg) |
|---|---|
| "Bumrah has 130 T20I wickets" | "Bumrah dismissed left-handers 2.3x more at death overs in subcontinental venues" |
| Head-to-head: Kohli vs Starc | Full dismissal network: who sets up whom, fielder involvement, over-by-over |
| Partnership runs table | Partnership PageRank: which pairs are structurally critical to a team |
| "Similar players" by eye test | Vector similarity on batting/bowling performance embeddings |
| Manual squad selection | NSGA-II Pareto-optimal XIs (batting avg × SR × economy × experience) |

## Data Source

[Cricsheet.org](https://cricsheet.org/downloads/) — CC-BY-4.0 licensed ball-by-ball data:

- **21,325 JSON files**, one per match
- Every delivery: batter, bowler, non-striker, runs, extras, wickets (with dismissal kind + fielders)
- Full player registries with unique IDs across all matches
- Match metadata: venue, city, tournament, season, toss, outcome, officials

## Graph Schema

```
Player ─[PLAYED_FOR]──────> Team ─[COMPETED_IN]──> Match ─[HOSTED_AT]──> Venue
  │                           │                      │
  ├─[BATTED_IN {runs,balls,  ├─[WON]                ├─[PART_OF {match_number}]──> Tournament
  │   fours,sixes,sr}]       │                      │
  │           │               ├─[WON_TOSS            └─[IN_SEASON]──> Season
  │           ▼                 {decision}]
  │         Match
  │
  ├─[BOWLED_IN {overs,maidens,runs,wickets,economy}]──> Match
  │
  ├─[DISMISSED {kind,over}]──> Player (batsman)
  │
  ├─[FIELDED_DISMISSAL {kind}]──> Player (batsman)
  │
  └─[PLAYER_OF_MATCH]──> Match
```

### Node Labels (6)

| Label | Key Property | Count |
|---|---|---|
| Player | cricsheet_id, name | 12,933 |
| Match | file_id, match_type, date, winner | 21,324 |
| Venue | name, city | 877 |
| Team | name | 383 |
| Tournament | name | 1,053 |
| Season | year | 49 |

### Edge Types (12)

| Edge | From → To | Properties |
|---|---|---|
| PLAYED_FOR | Player → Team | |
| BATTED_IN | Player → Match | runs, balls, fours, sixes, strike_rate, innings_num |
| BOWLED_IN | Player → Match | overs, maidens, runs_conceded, wickets, economy, innings_num |
| DISMISSED | Player (bowler) → Player (batsman) | kind, over, match_file_id |
| FIELDED_DISMISSAL | Player (fielder) → Player (batsman) | kind, over, match_file_id |
| COMPETED_IN | Team → Match | |
| WON | Team → Match | by_runs, by_wickets |
| WON_TOSS | Team → Match | decision |
| HOSTED_AT | Match → Venue | |
| PART_OF | Match → Tournament | match_number, group |
| IN_SEASON | Match → Season | |
| PLAYER_OF_MATCH | Player → Match | |

## Quick Start

### Option A: Pre-built Snapshot (Recommended)

A pre-built `.sgsnap` snapshot of the full dataset is available for instant import:

| | |
|---|---|
| **Download** | [cricket.sgsnap](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/cricket.sgsnap) (21 MB) |
| **Nodes** | 36,619 |
| **Edges** | 1,413,870 |
| **Requires** | Samyama Graph v0.6.1+ |

```bash
# Download snapshot
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/cricket.sgsnap

# Create tenant and import
curl -X POST http://localhost:8080/api/tenants \
  -H 'Content-Type: application/json' \
  -d '{"id":"cricket","name":"Cricket KG"}'

curl -X POST http://localhost:8080/api/tenants/cricket/snapshot/import \
  -F "file=@cricket.sgsnap"
```

### Option B: Load from Source

```bash
# Clone
git clone https://github.com/samyama-ai/cricket-kg.git
cd cricket-kg

# Install (requires Samyama Python SDK)
pip install -e ".[dev]"

# Download Cricsheet data
mkdir -p data && cd data
curl -LO https://cricsheet.org/downloads/all_json.zip
unzip -q all_json.zip -d json
cd ..

# Load 100 matches (quick test)
python -m etl.loader --data-dir data/json --max-matches 100

# Load 500 T20 matches
python -m etl.loader --data-dir data/json --max-matches 500 --match-type T20 --gender male

# Load everything (21,324 matches — ~24 minutes)
python -m etl.loader --data-dir data/json

# Run tests
pytest tests/ -v
```

## Example Queries

All results below are from the **full 21,324-match dataset** (all formats combined).

### Top run scorers

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
RETURN p.name AS player, sum(b.runs) AS runs, count(b) AS innings,
       sum(b.balls) AS balls, sum(b.fours) AS fours, sum(b.sixes) AS sixes
ORDER BY runs DESC LIMIT 10
```

| Player | Runs | Inn | Balls | 4s | 6s |
|---|---|---|---|---|---|
| V Kohli | 36,500 | 878 | 41,620 | 3,512 | 607 |
| KC Sangakkara | 30,118 | 728 | 40,130 | 3,166 | 278 |
| DA Warner | 27,903 | 740 | 28,598 | 3,032 | 613 |
| RG Sharma | 26,691 | 795 | 27,992 | 2,537 | 937 |
| AB de Villiers | 26,272 | 688 | 30,003 | 2,449 | 662 |
| JE Root | 26,013 | 611 | 37,631 | 2,580 | 157 |
| KS Williamson | 24,569 | 629 | 34,708 | 2,504 | 270 |
| HM Amla | 22,873 | 567 | 34,412 | 2,573 | 152 |
| JM Vince | 22,784 | 677 | 24,636 | 2,834 | 364 |
| CH Gayle | 22,638 | 694 | 21,259 | 2,125 | 1,243 |

### Top wicket takers

```cypher
MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)
RETURN p.name AS player, sum(b.wickets) AS wickets,
       sum(b.runs_conceded) AS runs, count(b) AS innings
ORDER BY wickets DESC LIMIT 10
```

| Player | Wickets | Inn | Runs |
|---|---|---|---|
| JM Anderson | 1,112 | 622 | 30,378 |
| R Ashwin | 987 | 604 | 27,122 |
| SCJ Broad | 957 | 555 | 27,857 |
| TG Southee | 917 | 606 | 27,414 |
| Shakib Al Hasan | 890 | 679 | 24,614 |
| MA Starc | 882 | 476 | 23,049 |
| TA Boult | 878 | 542 | 23,303 |
| DW Steyn | 874 | 490 | 20,814 |
| MJ Henry | 810 | 416 | 19,491 |
| DJ Bravo | 808 | 671 | 22,073 |

### Dismissal network: who gets whom out?

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
RETURN bowler.name AS bowler, batsman.name AS batsman,
       count(d) AS times, collect(d.kind) AS how
ORDER BY times DESC LIMIT 10
```

| Bowler | Batsman | Times |
|---|---|---|
| SCJ Broad | DA Warner | 20 |
| R Ashwin | BA Stokes | 17 |
| R Ashwin | DA Warner | 17 |
| MA Starc | JM Bairstow | 17 |
| RA Jadeja | SPD Smith | 16 |
| PJ Cummins | JE Root | 16 |
| Saeed Ajmal | DPMD Jayawardene | 16 |
| K Rabada | RG Sharma | 16 |
| MA Starc | BA Stokes | 16 |
| ML Schutt | DN Wyatt | 15 |

### Multi-hop: Australian bowlers vs Indian batsmen

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)-[:PLAYED_FOR]->(t:Team),
      (bowler)-[:PLAYED_FOR]->(bt:Team)
WHERE t.name = "India" AND bt.name = "Australia"
RETURN bowler.name AS bowler, count(d) AS dismissals,
       collect(DISTINCT batsman.name) AS victims
ORDER BY dismissals DESC LIMIT 10
```

| Bowler | Dismissals |
|---|---|
| PJ Cummins | 124 |
| MA Starc | 119 |
| NM Lyon | 119 |
| MG Johnson | 118 |
| JR Hazlewood | 97 |
| B Lee | 86 |
| SR Watson | 79 |
| A Gardner | 78 |
| A Sutherland | 65 |
| A Zampa | 55 |

### Most sixes

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
RETURN p.name AS player, sum(b.sixes) AS sixes, sum(b.runs) AS runs
ORDER BY sixes DESC LIMIT 10
```

| Player | Sixes | Runs |
|---|---|---|
| CH Gayle | 1,243 | 22,638 |
| KA Pollard | 948 | 14,876 |
| RG Sharma | 937 | 26,691 |
| N Pooran | 824 | 12,621 |
| AD Russell | 802 | 10,108 |
| JC Buttler | 774 | 21,686 |
| GJ Maxwell | 711 | 15,542 |
| BB McCullum | 665 | 18,908 |
| Q de Kock | 665 | 21,849 |
| AB de Villiers | 662 | 26,272 |

### Top fielders (catches)

```cypher
MATCH (fielder:Player)-[d:FIELDED_DISMISSAL]->(batsman:Player)
WHERE d.kind = "caught"
RETURN fielder.name AS fielder, count(d) AS catches
ORDER BY catches DESC LIMIT 10
```

| Fielder | Catches |
|---|---|
| MS Dhoni | 752 |
| Q de Kock | 718 |
| JC Buttler | 656 |
| JA Simpson | 631 |
| AB de Villiers | 593 |
| JM Bairstow | 565 |
| KC Sangakkara | 559 |
| BC Brown | 552 |
| OB Cox | 524 |
| BT Foakes | 512 |

### Player of match awards

```cypher
MATCH (p:Player)-[:PLAYER_OF_MATCH]->(m:Match)
RETURN p.name AS player, count(m) AS awards
ORDER BY awards DESC LIMIT 10
```

| Player | Awards |
|---|---|
| V Kohli | 88 |
| AB de Villiers | 71 |
| CH Gayle | 69 |
| Shakib Al Hasan | 66 |
| Q de Kock | 66 |
| RG Sharma | 63 |
| DA Warner | 61 |
| SFM Devine | 59 |
| HK Matthews | 56 |
| SR Watson | 55 |

### Most successful teams

```cypher
MATCH (t:Team)-[w:WON]->(m:Match)
RETURN t.name AS team, count(w) AS wins
ORDER BY wins DESC LIMIT 10
```

| Team | Wins |
|---|---|
| India | 761 |
| Australia | 758 |
| England | 665 |
| South Africa | 597 |
| Pakistan | 520 |
| New Zealand | 506 |
| Sri Lanka | 462 |
| West Indies | 369 |
| Bangladesh | 296 |
| Ireland | 223 |

### Tournaments

```cypher
MATCH (m:Match)-[:PART_OF]->(t:Tournament)
RETURN t.name AS tournament, count(m) AS matches
ORDER BY matches DESC LIMIT 10
```

| Tournament | Matches |
|---|---|
| Indian Premier League | 1,164 |
| Vitality Blast | 834 |
| Syed Mushtaq Ali Trophy | 668 |
| Big Bash League | 654 |
| County Championship | 618 |
| Royal London One-Day Cup | 582 |
| Women's Big Bash League | 517 |
| NatWest T20 Blast | 482 |
| Bangladesh Premier League | 459 |
| Specsavers County Championship | 457 |

### Top venues

```cypher
MATCH (m:Match)-[:HOSTED_AT]->(v:Venue)
RETURN v.name AS venue, v.city AS city, count(m) AS matches
ORDER BY matches DESC LIMIT 10
```

| Venue | City | Matches |
|---|---|---|
| Dubai International Cricket Stadium | — | 371 |
| Shere Bangla National Stadium, Mirpur | Dhaka | 343 |
| Edgbaston, Birmingham | Birmingham | 262 |
| County Ground | Bristol | 260 |
| The Rose Bowl, Southampton | Southampton | 246 |
| Kennington Oval, London | London | 222 |
| Adelaide Oval | — | 217 |
| Trent Bridge, Nottingham | Nottingham | 216 |
| Harare Sports Club | — | 214 |
| County Ground, Chelmsford | Chelmsford | 205 |

## Graph Statistics

```
Total nodes:    36,619
Total edges: 1,372,313

Player          12,933
Match           21,324
Venue              877
Team               383
Tournament       1,053
Season              49
```

### Matches by format

| Format | Matches |
|---|---|
| T20 | 13,069 |
| ODI | 3,098 |
| MDM (Multi-day domestic) | 2,085 |
| ODM (One-day domestic) | 1,852 |
| Test | 900 |
| IT20 (International T20) | 320 |

## 100 Queries: From SQL to Graph

We maintain a **[100-query showcase](docs/100-queries.md)** organized in 5 progressive levels that demonstrate where relational databases hit their ceiling and where graph databases take over:

| Level | Name | SQL Equivalent | Queries |
|-------|------|----------------|---------|
| **1** | Foundation | Single table, GROUP BY | 1--15 |
| **2** | Relational Joins | 2-table JOIN | 16--35 |
| **3** | Multi-hop Traversals | 3--5 JOINs, self-joins **— SQL slows here** | 36--60 |
| **4** | Path & Pattern Analytics | Recursive CTEs **— SQL breaks here** | 61--80 |
| **5** | Network Intelligence | **Impossible in SQL** | 81--100 |

### The inflection point

**Levels 1--2**: Both RDBMS and graph perform well. Choose based on your stack.

**Level 3**: Graph databases start winning. Each hop follows a pointer instead of scanning a hash table. Queries stay readable while SQL accumulates JOINs.

**Level 4**: RDBMS queries become fragile. Adding one more hop requires restructuring the entire query. Graph queries simply extend the pattern.

**Level 5**: RDBMS cannot express these queries at all. Dismissal chains, triangle detection, network centrality, variable-length path traversal — these are native graph operations.

### Highlights

**Dismissal chains** (Level 5) — A dismissed B who dismissed C:
```cypher
MATCH (a:Player)-[:DISMISSED]->(b:Player)-[:DISMISSED]->(c:Player)
WHERE a <> c
RETURN a.name, b.name, c.name, count(*) AS strength
ORDER BY strength DESC LIMIT 10
```

**Mutual dismissal pairs** — Bowlers who've dismissed each other:
```cypher
MATCH (a:Player)-[:DISMISSED]->(b:Player)-[:DISMISSED]->(a)
WHERE a.name < b.name
RETURN a.name, b.name
LIMIT 20
```

**Team overlap network** — Players shared between rival teams:
```cypher
MATCH (t1:Team)<-[:PLAYED_FOR]-(p:Player)-[:PLAYED_FOR]->(t2:Team)
WHERE t1.name < t2.name
WITH t1, t2, count(p) AS shared, collect(p.name) AS players
WHERE shared >= 5
RETURN t1.name, t2.name, shared, players[0..5]
ORDER BY shared DESC
```

**Full player profile** (7-hop aggregation across entire graph):
```cypher
MATCH (p:Player {name: 'V Kohli'})
OPTIONAL MATCH (p)-[bat:BATTED_IN]->(m1:Match)
OPTIONAL MATCH (p)-[bowl:BOWLED_IN]->(m2:Match)
OPTIONAL MATCH (p)-[:PLAYED_FOR]->(team:Team)
OPTIONAL MATCH (p)-[:PLAYER_OF_MATCH]->(m3:Match)
OPTIONAL MATCH (p)-[d:DISMISSED]->(victim:Player)
RETURN sum(bat.runs) AS runs, count(bat) AS innings,
       max(bat.runs) AS highest, sum(bat.sixes) AS sixes,
       sum(bowl.wickets) AS wickets,
       collect(DISTINCT team.name) AS teams,
       count(DISTINCT m3) AS pom_awards, count(d) AS dismissals
```

See the **[full 100-query showcase](docs/100-queries.md)** for all queries with explanations.

## Performance

ETL loads **21,324 matches in ~24 minutes** on a MacBook Pro (embedded mode, no server):

- **31 matches/s** at start, **13 matches/s** at 21K (rate declines as graph grows)
- Batch Cypher execution: groups node CREATEs and edge MATCH+CREATEs
- Property indexes on Player.cricsheet_id, Match.file_id, Team.name, Venue.name, Tournament.name, Season.year
- Python-side deduplication (Registry class) avoids redundant MERGE checks

## Project Structure

```
cricket-kg/
├── etl/
│   └── loader.py              # Main ETL: JSON → Samyama graph (~400 lines)
├── scripts/
│   └── run_queries.py         # Run all showcase queries against loaded graph
├── mcp_server/                # MCP tools (planned)
│   └── tools/
├── tests/
│   └── test_loader.py         # 23 tests: all node types, edges, stats, multi-hop
├── scenarios/                 # Evaluation scenarios (planned)
├── data/
│   └── json/                  # Cricsheet JSON files (21,325 — gitignored)
├── pyproject.toml
└── README.md
```

## Tests

```bash
$ pytest tests/ -v
========================== 23 passed in 0.77s ==========================
```

Tests cover: match creation, team/player/venue/tournament nodes, COMPETED_IN, WON, WON_TOSS, PLAYED_FOR, BATTED_IN (with run/ball/boundary counts), BOWLED_IN (with wickets/maidens), DISMISSED (bowled, caught), FIELDED_DISMISSAL, PLAYER_OF_MATCH, PART_OF, and multi-hop queries (dismissal network, tournament players).

## Roadmap

- [x] Load all 21,324 matches (full dataset — 24 min)
- [ ] MCP server tools (or auto-generated via [SK-12](https://github.com/samyama-ai/samyama-graph))
- [ ] Vector embeddings for player performance profiles (similarity search)
- [ ] Graph algorithms: PageRank (partnership importance), community detection (player clusters)
- [ ] Optimization: NSGA-II team selection (batting avg × SR × bowling economy × experience)
- [ ] 40 evaluation scenarios
- [ ] Women's cricket analysis

## Related

- [Samyama Graph Database](https://github.com/samyama-ai/samyama-graph) — High-performance graph DB with OpenCypher, vector search, and optimization
- [Cricsheet.org](https://cricsheet.org/) — Ball-by-ball cricket data (CC-BY-4.0)
- [Clinical Trials KG](https://github.com/samyama-ai/clinicaltrials-kg) — Same architecture pattern for clinical research

## License

Apache 2.0. Data from Cricsheet.org is CC-BY-4.0.
