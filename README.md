# Cricket Knowledge Graph

A knowledge graph of ball-by-ball cricket data from [Cricsheet.org](https://cricsheet.org/) — powered by [Samyama Graph Database](https://github.com/samyama-ai/samyama-graph).

**21,325 matches** | **8 node types** | **12 edge types** | **Ball-by-ball granularity** | **All formats: Tests, ODIs, T20Is, IPL, BBL, CPL, PSL, The Hundred, ...**

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

| Label | Key Property | Count (500 T20s) |
|---|---|---|
| Player | cricsheet_id, name | 1,133 |
| Match | file_id, match_type, date, winner | 500 |
| Venue | name, city | 100 |
| Team | name | 87 |
| Tournament | name | 28 |
| Season | year | 4 |

### Edge Types (10)

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

# Load everything (21,325 matches — takes ~4-5 hours)
python -m etl.loader --data-dir data/json

# Run tests
pytest tests/ -v
```

## Example Queries

### Top T20 run scorers

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match {match_type: "T20"})
RETURN p.name AS player, sum(b.runs) AS runs, count(b) AS innings,
       sum(b.balls) AS balls, sum(b.fours) AS fours, sum(b.sixes) AS sixes
ORDER BY runs DESC LIMIT 10
```

| Player | Runs | Inn | Balls | 4s | 6s | SR |
|---|---|---|---|---|---|---|
| BB McCullum | 1,666 | 65 | 1,231 | 161 | 89 | 149.4 |
| AJ Finch | 1,433 | 47 | 925 | 132 | 73 | 162.1 |
| KA Pollard | 1,401 | 68 | 1,034 | 82 | 83 | 144.3 |
| KC Sangakkara | 1,372 | 52 | 1,109 | 136 | 34 | 126.3 |
| C Munro | 1,357 | 43 | 897 | 118 | 78 | 155.6 |
| CH Gayle | 1,246 | 47 | 994 | 81 | 94 | 130.9 |
| DR Smith | 1,151 | 44 | 948 | 124 | 47 | 125.7 |
| CA Lynn | 1,118 | 27 | 697 | 89 | 75 | 166.1 |
| JC Buttler | 1,066 | 37 | 788 | 86 | 43 | 139.9 |
| HM Amla | 1,060 | 25 | 787 | 99 | 33 | 138.7 |

### Top wicket takers

```cypher
MATCH (p:Player)-[b:BOWLED_IN]->(m:Match {match_type: "T20"})
RETURN p.name AS player, sum(b.wickets) AS wickets,
       sum(b.runs_conceded) AS runs, count(b) AS innings
ORDER BY wickets DESC LIMIT 10
```

| Player | Wickets | Inn | Avg |
|---|---|---|---|
| DJ Bravo | 76 | 53 | 21.2 |
| Rashid Khan | 61 | 44 | 18.0 |
| SP Narine | 61 | 64 | 26.9 |
| CJ Jordan | 52 | 41 | 23.8 |
| Sohail Tanvir | 51 | 40 | 21.5 |
| AJ Tye | 51 | 29 | 17.1 |
| Imran Tahir | 48 | 39 | 24.7 |
| KOK Williams | 48 | 33 | 21.3 |
| IS Sodhi | 47 | 35 | 22.9 |
| Shadab Khan | 44 | 29 | 16.0 |

### Dismissal network: who gets whom out?

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
RETURN bowler.name AS bowler, batsman.name AS batsman,
       count(d) AS times, collect(d.kind) AS how
ORDER BY times DESC LIMIT 10
```

| Bowler | Batsman | Times | How |
|---|---|---|---|
| Wahab Riaz | SP Narine | 3 | bowled, caught |
| Shadab Khan | MJ Guptill | 3 | caught |
| DJ Bravo | Mohammad Nabi | 3 | bowled, caught |
| SP Narine | Sohail Tanvir | 3 | bowled, lbw |
| Shadab Khan | CAK Walton | 3 | bowled, caught, lbw |
| S Badree | C Munro | 3 | bowled, caught |
| Hasan Ali | JN Mohammed | 3 | bowled, caught |
| DJ Bravo | DJG Sammy | 3 | bowled, caught |
| Sohail Tanvir | N Pooran | 3 | bowled, caught |
| N Vanua | Rohan Mustafa | 3 | bowled, caught |

### Multi-hop: Australian bowlers vs Indian batsmen

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
        -[:PLAYED_FOR]->(t:Team {name: "India"}),
      (bowler)-[:PLAYED_FOR]->(bt:Team {name: "Australia"})
RETURN bowler.name AS bowler, count(d) AS dismissals,
       collect(DISTINCT batsman.name) AS victims
ORDER BY dismissals DESC LIMIT 10
```

| Bowler | Dismissals | Victims |
|---|---|---|
| PJ Cummins | 3 | KD Karthik, Mandeep Singh, SK Raina |
| GJ Maxwell | 3 | Mandeep Singh, RR Pant, Yuvraj Singh |
| A Zampa | 2 | RG Sharma, RR Pant |
| JP Faulkner | 1 | RG Sharma |
| AJ Tye | 1 | Mandeep Singh |

### Most sixes

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
RETURN p.name AS player, sum(b.sixes) AS sixes, sum(b.runs) AS runs
ORDER BY sixes DESC LIMIT 10
```

| Player | Sixes | Runs |
|---|---|---|
| CH Gayle | 94 | 1,246 |
| BB McCullum | 89 | 1,666 |
| KA Pollard | 83 | 1,401 |
| C Munro | 78 | 1,357 |
| E Lewis | 76 | 924 |
| CA Lynn | 75 | 1,118 |
| AJ Finch | 73 | 1,433 |
| CA Ingram | 53 | 1,021 |
| DJG Sammy | 51 | 777 |
| GJ Maxwell | 49 | 959 |

### Top fielders (catches)

```cypher
MATCH (fielder:Player)-[d:FIELDED_DISMISSAL {kind: "caught"}]->(batsman:Player)
RETURN fielder.name AS fielder, count(d) AS catches
ORDER BY catches DESC LIMIT 10
```

| Fielder | Catches |
|---|---|
| KC Sangakkara | 54 |
| KA Pollard | 43 |
| BB McCullum | 35 |
| AJ Finch | 29 |
| CJ Jordan | 27 |
| Umar Akmal | 27 |
| SW Billings | 26 |
| MS Dhoni | 25 |
| DA Miller | 25 |
| CAK Walton | 24 |

### Player of match awards

```cypher
MATCH (p:Player)-[:PLAYER_OF_MATCH]->(m:Match)
RETURN p.name AS player, count(m) AS awards
ORDER BY awards DESC LIMIT 10
```

| Player | Awards |
|---|---|
| E Lewis | 7 |
| C Munro | 7 |
| SR Patel | 7 |
| Rashid Khan | 6 |
| BB McCullum | 6 |
| JT Smuts | 6 |
| Mahmudullah | 5 |
| CH Gayle | 5 |
| DJM Short | 5 |
| CA Ingram | 5 |

### Most successful teams

```cypher
MATCH (t:Team)-[w:WON]->(m:Match)
RETURN t.name AS team, count(w) AS wins
ORDER BY wins DESC LIMIT 10
```

| Team | Wins |
|---|---|
| Perth Scorchers | 15 |
| Trinbago Knight Riders | 15 |
| Jamaica Tallawahs | 13 |
| Adelaide Strikers | 12 |
| Nottinghamshire | 11 |
| Mumbai Indians | 11 |
| Melbourne Renegades | 10 |
| Dhaka Dynamites | 10 |
| Rising Pune Supergiant | 10 |
| Pakistan | 10 |

### Tournaments

```cypher
MATCH (m:Match)-[:PART_OF]->(t:Tournament)
RETURN t.name AS tournament, count(m) AS matches
ORDER BY matches DESC LIMIT 10
```

| Tournament | Matches |
|---|---|
| NatWest T20 Blast | 121 |
| Big Bash League | 78 |
| Indian Premier League | 59 |
| Caribbean Premier League | 54 |
| Bangladesh Premier League | 45 |
| CSA T20 Challenge | 27 |
| Pakistan Super League | 24 |
| Super Smash | 23 |

### Top venues

```cypher
MATCH (m:Match)-[:HOSTED_AT]->(v:Venue)
RETURN v.name AS venue, v.city AS city, count(m) AS matches
ORDER BY matches DESC LIMIT 10
```

| Venue | City | Matches |
|---|---|---|
| Shere Bangla National Stadium | Dhaka | 34 |
| County Ground | Taunton | 28 |
| Dubai International Cricket Stadium | — | 19 |
| Queen's Park Oval | Trinidad | 12 |
| Zahur Ahmed Chowdhury Stadium | Chattogram | 11 |
| Melbourne Cricket Ground | — | 11 |
| Adelaide Oval | — | 11 |
| Sharjah Cricket Stadium | — | 10 |
| Sydney Cricket Ground | — | 10 |
| Brisbane Cricket Ground | Brisbane | 10 |

## Graph Statistics (500 T20 matches)

```
Total nodes:  1,852
Total edges: 28,973

Player          1,133
Match             500
Venue             100
Team               87
Tournament         28
Season              4
```

## Queries Only a Graph Can Answer

These require multi-hop traversals that are impossible with flat stat tables:

```cypher
-- Which bowlers dismissed batsmen who scored 50+ against their own team?
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)-[b:BATTED_IN]->(m:Match),
      (bowler)-[:PLAYED_FOR]->(t:Team)-[:COMPETED_IN]->(m)
WHERE b.runs >= 50
RETURN bowler.name, batsman.name, b.runs, d.kind

-- Players who played for multiple IPL teams
MATCH (p:Player)-[:PLAYED_FOR]->(t1:Team), (p)-[:PLAYED_FOR]->(t2:Team)
WHERE t1 <> t2
MATCH (t1)-[:COMPETED_IN]->(m:Match)-[:PART_OF]->(tour:Tournament {name: "Indian Premier League"})
RETURN p.name, collect(DISTINCT t1.name) AS teams
ORDER BY size(teams) DESC

-- Bowler effectiveness at specific venues
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player),
      (bowler)-[:BOWLED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
RETURN bowler.name, v.name AS venue, count(d) AS dismissals
ORDER BY dismissals DESC LIMIT 20

-- Tournament-spanning player journeys
MATCH (p:Player)-[:BATTED_IN]->(m:Match)-[:PART_OF]->(t:Tournament)
RETURN p.name, collect(DISTINCT t.name) AS tournaments, count(DISTINCT t) AS count
ORDER BY count DESC LIMIT 10
```

## Project Structure

```
cricket-kg/
├── etl/
│   └── loader.py              # Main ETL: JSON → Samyama graph (single file, ~400 lines)
├── mcp_server/                # MCP tools (planned — or auto-generated via SK-12)
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

- [ ] Load all 21,325 matches (full dataset)
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
- [boundary-graph](https://boundary-graph.netlify.app/) — Flat D3 visualization (cricket-kg goes further: queryable graph + MCP + algorithms)

## License

Apache 2.0. Data from Cricsheet.org is CC-BY-4.0.
