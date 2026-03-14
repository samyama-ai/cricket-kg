# 100 Cypher Queries for the Cricket Knowledge Graph

**21,324 matches | 12,933 players | 383 teams | 1,053 tournaments | 877 venues | 49 seasons**

These queries are organized in five progressive levels that illustrate where relational databases hit their ceiling and where graph databases take over.

| Level | Name | SQL Equivalent | Queries |
|-------|------|----------------|---------|
| 1 | **Foundation** | Single table, GROUP BY | 1--15 |
| 2 | **Relational Joins** | 2-table JOIN | 16--35 |
| 3 | **Multi-hop Traversals** | 3--5 JOINs, self-joins | 36--60 |
| 4 | **Path & Pattern Analytics** | Recursive CTEs, breaks down | 61--80 |
| 5 | **Network Intelligence** | Impossible in SQL | 81--100 |

---

## Level 1: Foundation (SQL-equivalent)

*These queries scan a single node type or edge type. Any RDBMS handles them trivially with a single table and GROUP BY.*

### 1. Total matches by format

```cypher
MATCH (m:Match)
RETURN m.match_type AS format, count(m) AS matches
ORDER BY matches DESC
```

### 2. Matches per season

```cypher
MATCH (m:Match)-[:IN_SEASON]->(s:Season)
RETURN s.year AS season, count(m) AS matches
ORDER BY season DESC
```

### 3. Total players in the dataset

```cypher
MATCH (n:Player) RETURN count(n) AS total_players
```

### 4. Top 20 run scorers (all formats)

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
RETURN p.name AS player, sum(b.runs) AS total_runs,
       count(b) AS innings, sum(b.fours) AS fours, sum(b.sixes) AS sixes
ORDER BY total_runs DESC
LIMIT 20
```

### 5. Top 20 wicket takers (all formats)

```cypher
MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)
RETURN p.name AS player, sum(b.wickets) AS total_wickets,
       count(b) AS innings, sum(b.maidens) AS maidens
ORDER BY total_wickets DESC
LIMIT 20
```

### 6. Highest individual scores

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
RETURN p.name AS player, b.runs AS score, b.balls AS balls,
       b.fours AS fours, b.sixes AS sixes, m.match_type AS format, m.date AS date
ORDER BY b.runs DESC
LIMIT 25
```

### 7. Best bowling figures in an innings

```cypher
MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)
RETURN p.name AS player, b.wickets AS wickets, b.runs_conceded AS runs,
       b.overs AS overs, m.match_type AS format, m.date AS date
ORDER BY b.wickets DESC, b.runs_conceded ASC
LIMIT 25
```

### 8. Most sixes hit (career)

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
RETURN p.name AS player, sum(b.sixes) AS total_sixes, sum(b.runs) AS runs
ORDER BY total_sixes DESC
LIMIT 20
```

### 9. Best economy rates (min 50 innings)

```cypher
MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)
WITH p, count(b) AS innings, sum(b.runs_conceded) AS runs, sum(b.overs) AS overs
WHERE innings > 50 AND overs > 0
RETURN p.name AS player, innings, runs, overs,
       round(runs / overs * 100) / 100 AS economy
ORDER BY economy ASC
LIMIT 20
```

### 10. Highest strike rates (min 500 balls)

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
WITH p, sum(b.runs) AS runs, sum(b.balls) AS balls
WHERE balls > 500
RETURN p.name AS player, runs, balls,
       round(runs * 10000 / balls) / 100 AS strike_rate
ORDER BY strike_rate DESC
LIMIT 20
```

### 11. Venues hosting most matches

```cypher
MATCH (m:Match)-[:HOSTED_AT]->(v:Venue)
RETURN v.name AS venue, v.city AS city, count(m) AS matches
ORDER BY matches DESC
LIMIT 20
```

### 12. Teams with most wins

```cypher
MATCH (t:Team)-[:WON]->(m:Match)
RETURN t.name AS team, count(m) AS wins
ORDER BY wins DESC
LIMIT 20
```

### 13. Player of the Match awards

```cypher
MATCH (p:Player)-[:PLAYER_OF_MATCH]->(m:Match)
RETURN p.name AS player, count(m) AS awards
ORDER BY awards DESC
LIMIT 20
```

### 14. Gender split in matches

```cypher
MATCH (m:Match)
RETURN m.gender AS gender, m.match_type AS format, count(m) AS matches
ORDER BY matches DESC
```

### 15. Biggest wins by runs

```cypher
MATCH (t:Team)-[w:WON]->(m:Match)
WHERE w.by_runs IS NOT NULL
RETURN t.name AS team, w.by_runs AS margin, m.match_type AS format, m.date AS date
ORDER BY w.by_runs DESC
LIMIT 15
```

---

## Level 2: Relational Joins (SQL with 2-table JOINs)

*These queries join two entities. SQL can handle them with standard JOINs, but the queries are already more natural in Cypher.*

### 16. Top scorers in Test cricket

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
WHERE m.match_type = 'Test'
RETURN p.name AS player, sum(b.runs) AS test_runs,
       count(b) AS innings, sum(b.fours) AS fours, sum(b.sixes) AS sixes
ORDER BY test_runs DESC
LIMIT 20
```

### 17. Top scorers in T20

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
WHERE m.match_type = 'T20'
RETURN p.name AS player, sum(b.runs) AS t20_runs, count(b) AS innings,
       sum(b.sixes) AS sixes,
       round(sum(b.runs) * 10000 / sum(b.balls)) / 100 AS strike_rate
ORDER BY t20_runs DESC
LIMIT 20
```

### 18. Teams ranked by win percentage

```cypher
MATCH (t:Team)-[:COMPETED_IN]->(m:Match)
OPTIONAL MATCH (t)-[:WON]->(m)
WITH t, count(DISTINCT m) AS played, count(m) AS wins
RETURN t.name AS team, played, wins,
       round(wins * 10000 / played) / 100 AS win_pct
ORDER BY win_pct DESC
LIMIT 20
```

### 19. Tournament with most matches

```cypher
MATCH (m:Match)-[:PART_OF]->(t:Tournament)
RETURN t.name AS tournament, count(m) AS matches
ORDER BY matches DESC
LIMIT 20
```

### 20. IPL top scorers (all-time)

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:PART_OF]->(t:Tournament)
WHERE t.name = 'Indian Premier League'
RETURN p.name AS player, sum(b.runs) AS runs, count(b) AS innings,
       sum(b.fours) AS fours, sum(b.sixes) AS sixes
ORDER BY runs DESC
LIMIT 20
```

### 21. IPL top wicket takers

```cypher
MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)-[:PART_OF]->(t:Tournament)
WHERE t.name = 'Indian Premier League'
RETURN p.name AS player, sum(b.wickets) AS wickets, count(b) AS innings,
       sum(b.maidens) AS maidens
ORDER BY wickets DESC
LIMIT 20
```

### 22. Toss decision trends by format

```cypher
MATCH (t:Team)-[wt:WON_TOSS]->(m:Match)
RETURN m.match_type AS format, wt.decision AS decision, count(m) AS count
ORDER BY format, count DESC
```

### 23. Does winning the toss help? (win after toss-win)

```cypher
MATCH (t:Team)-[:WON_TOSS]->(m:Match)
OPTIONAL MATCH (t)-[:WON]->(m)
WITH count(DISTINCT m) AS toss_wins, count(m) AS also_won_match
RETURN toss_wins, also_won_match,
       round(also_won_match * 10000 / toss_wins) / 100 AS toss_advantage_pct
```

### 24. Venue specialists -- top scorers per ground

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WHERE v.name CONTAINS 'Wankhede'
RETURN p.name AS player, sum(b.runs) AS runs, count(b) AS innings
ORDER BY runs DESC
LIMIT 10
```

### 25. Most successful team at a venue

```cypher
MATCH (t:Team)-[:WON]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WHERE v.name CONTAINS 'Lords'
RETURN t.name AS team, count(m) AS wins
ORDER BY wins DESC
LIMIT 10
```

### 26. Players who played for a specific team

```cypher
MATCH (p:Player)-[:PLAYED_FOR]->(t:Team {name: 'Mumbai Indians'})
MATCH (p)-[b:BATTED_IN]->(m:Match)
RETURN p.name AS player, sum(b.runs) AS runs, count(b) AS innings
ORDER BY runs DESC
LIMIT 15
```

### 27. Maiden over kings

```cypher
MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)
RETURN p.name AS player, sum(b.maidens) AS maidens,
       count(b) AS innings, sum(b.wickets) AS wickets
ORDER BY maidens DESC
LIMIT 20
```

### 28. Most five-wicket hauls

```cypher
MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)
WHERE b.wickets >= 5
RETURN p.name AS player, count(b) AS five_wicket_hauls,
       max(b.wickets) AS best
ORDER BY five_wicket_hauls DESC
LIMIT 20
```

### 29. Centuries count per player

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
WHERE b.runs >= 100
RETURN p.name AS player, count(b) AS centuries, max(b.runs) AS highest
ORDER BY centuries DESC
LIMIT 20
```

### 30. Players with most ducks (0 runs, at least 1 ball)

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
WHERE b.runs = 0 AND b.balls > 0
RETURN p.name AS player, count(b) AS ducks
ORDER BY ducks DESC
LIMIT 20
```

### 31. Most matches played by a player

```cypher
MATCH (p:Player)-[:BATTED_IN]->(m:Match)
RETURN p.name AS player, count(DISTINCT m) AS matches
ORDER BY matches DESC
LIMIT 20
```

### 32. Season-wise run leaders

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:IN_SEASON]->(s:Season)
WHERE s.year = '2023'
RETURN p.name AS player, sum(b.runs) AS runs, count(b) AS innings
ORDER BY runs DESC
LIMIT 15
```

### 33. Most catches (fielding dismissals)

```cypher
MATCH (fielder:Player)-[f:FIELDED_DISMISSAL]->(batsman:Player)
WHERE f.kind = 'caught'
RETURN fielder.name AS fielder, count(f) AS catches
ORDER BY catches DESC
LIMIT 20
```

### 34. Most stumpings

```cypher
MATCH (keeper:Player)-[f:FIELDED_DISMISSAL]->(batsman:Player)
WHERE f.kind = 'stumped'
RETURN keeper.name AS keeper, count(f) AS stumpings
ORDER BY stumpings DESC
LIMIT 15
```

### 35. Teams with best chasing record

```cypher
MATCH (t:Team)-[w:WON]->(m:Match)
WHERE w.by_wickets IS NOT NULL
RETURN t.name AS team, count(m) AS chase_wins
ORDER BY chase_wins DESC
LIMIT 15
```

---

## Level 3: Multi-hop Traversals (SQL starts struggling)

*These queries traverse 3+ entity types. In SQL, each hop requires another JOIN, subquery, or CTE. Performance degrades as hop count increases. In Cypher, the query remains readable and the graph engine optimizes traversal natively.*

### 36. Bowler-batsman dismissal network (top rivalries)

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
RETURN bowler.name AS bowler, batsman.name AS batsman, count(d) AS times
ORDER BY times DESC
LIMIT 25
```

> **Why graphs win**: This is a *self-join* on the player table through a dismissal relationship. In SQL: `SELECT ... FROM dismissals d JOIN players b ON d.bowler_id=b.id JOIN players bat ON d.batsman_id=bat.id GROUP BY ...`. In a graph, it's a single edge traversal.

### 37. Stuart Broad vs David Warner -- complete record

```cypher
MATCH (bowler:Player {name: 'SCJ Broad'})-[d:DISMISSED]->(batsman:Player {name: 'DA Warner'})
RETURN d.kind AS dismissal_type, d.over AS over_number, count(d) AS times
ORDER BY times DESC
```

### 38. Top scorers at each venue (3-hop: Player->Match->Venue)

*Shows all player-venue totals ranked by runs (per-group LIMIT not supported, so top scorers float to the top).*

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WITH v, p, sum(b.runs) AS runs
ORDER BY runs DESC
RETURN v.name AS venue, p.name AS top_scorer, runs
ORDER BY runs DESC
LIMIT 20
```

### 39. Tournament champions (teams with most wins per tournament)

*Ranks all team-tournament win counts; top teams per tournament float to the top.*

```cypher
MATCH (t:Team)-[:WON]->(m:Match)-[:PART_OF]->(trn:Tournament)
WITH trn, t, count(m) AS wins
ORDER BY wins DESC
RETURN trn.name AS tournament, t.name AS top_team, wins
ORDER BY wins DESC
LIMIT 20
```

### 40. IPL season-by-season run leaders

*Lists top IPL run scorers across seasons; top per season floats up by runs.*

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:PART_OF]->(t:Tournament), (m)-[:IN_SEASON]->(s:Season)
WHERE t.name = 'Indian Premier League'
WITH s, p, sum(b.runs) AS runs
ORDER BY runs DESC
RETURN s.year AS season, p.name AS orange_cap, runs
ORDER BY runs DESC
LIMIT 20
```

### 41. Players who scored a century AND took 5 wickets in the same match

```cypher
MATCH (p:Player)-[bat:BATTED_IN]->(m:Match)
WHERE bat.runs >= 100
MATCH (p)-[bowl:BOWLED_IN]->(m)
WHERE bowl.wickets >= 5
RETURN p.name AS player, bat.runs AS runs, bowl.wickets AS wickets,
       m.match_type AS format, m.date AS date
ORDER BY bat.runs DESC
```

### 42. Venues where batting first wins most often

*Uses explicit WHERE for toss decision instead of inline edge property map.*

```cypher
MATCH (t:Team)-[toss:WON_TOSS]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WHERE toss.decision = 'bat'
WITH t, m, v
MATCH (t)-[:WON]->(m)
RETURN v.name AS venue, count(m) AS bat_first_wins
ORDER BY bat_first_wins DESC
LIMIT 20
```

### 43. Highest partnership density (most sixes per match at a venue)

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WITH v, count(DISTINCT m) AS matches, sum(b.sixes) AS total_sixes
WHERE matches > 20
RETURN v.name AS venue, matches, total_sixes,
       round(total_sixes * 100 / matches) / 100 AS sixes_per_match
ORDER BY sixes_per_match DESC
LIMIT 15
```

### 44. Player performance breakdown by format

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
WHERE p.name = 'V Kohli'
RETURN m.match_type AS format, count(b) AS innings,
       sum(b.runs) AS runs, sum(b.fours) AS fours, sum(b.sixes) AS sixes,
       round(sum(b.runs) * 10000 / sum(b.balls)) / 100 AS strike_rate,
       max(b.runs) AS highest
ORDER BY runs DESC
```

### 45. Head-to-head team record

```cypher
MATCH (t1:Team {name: 'India'})-[:COMPETED_IN]->(m:Match)<-[:COMPETED_IN]-(t2:Team {name: 'Australia'})
OPTIONAL MATCH (t1)-[:WON]->(m)
WITH count(DISTINCT m) AS total, count(m) AS t1_wins
RETURN total AS matches_played, t1_wins AS india_wins,
       total - t1_wins AS australia_wins_or_draws
```

### 46. Player of the Match in tournament finals-style matches

```cypher
MATCH (p:Player)-[:PLAYER_OF_MATCH]->(m:Match)-[:PART_OF]->(t:Tournament)
WHERE t.name CONTAINS 'World Cup'
RETURN p.name AS player, t.name AS tournament, m.date AS date
ORDER BY m.date DESC
LIMIT 20
```

### 47. Bowlers who dismissed the most batsmen from a specific team

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)-[:PLAYED_FOR]->(team:Team)
WHERE team.name = 'Australia'
RETURN bowler.name AS bowler, count(d) AS dismissals
ORDER BY dismissals DESC
LIMIT 20
```

### 48. Most destructive T20 innings (runs and strike rate combined)

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
WHERE m.match_type = 'T20' AND b.balls >= 20
RETURN p.name AS player, b.runs AS runs, b.balls AS balls,
       b.strike_rate AS strike_rate, b.sixes AS sixes, m.date AS date
ORDER BY b.strike_rate DESC
LIMIT 25
```

### 49. Teams ranked by IPL performance

*Uses two separate MATCH clauses to count played and won independently, avoiding OPTIONAL MATCH counting issues.*

```cypher
MATCH (t:Team)-[:WON]->(w:Match)-[:PART_OF]->(trn:Tournament)
WHERE trn.name = 'Indian Premier League'
WITH t, count(w) AS wins
MATCH (t)-[:COMPETED_IN]->(m:Match)-[:PART_OF]->(trn2:Tournament)
WHERE trn2.name = 'Indian Premier League'
RETURN t.name AS team, count(DISTINCT m) AS played, wins,
       round(wins * 10000 / count(DISTINCT m)) / 100 AS win_pct
ORDER BY win_pct DESC
```

### 50. Left-arm vs right-arm? Dismissal types breakdown

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
RETURN d.kind AS dismissal_type, count(d) AS count
ORDER BY count DESC
```

### 51. Multi-format players (played Test, ODI, and T20)

```cypher
MATCH (p:Player)-[:BATTED_IN]->(m:Match)
WITH p, collect(DISTINCT m.match_type) AS formats
WHERE size(formats) >= 3
RETURN p.name AS player, formats, size(formats) AS format_count
ORDER BY format_count DESC
LIMIT 20
```

### 52. Venue performance for a specific player

```cypher
MATCH (p:Player {name: 'MS Dhoni'})-[b:BATTED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
RETURN v.name AS venue, count(b) AS innings, sum(b.runs) AS runs,
       max(b.runs) AS highest, sum(b.sixes) AS sixes
ORDER BY runs DESC
LIMIT 15
```

### 53. Bowling economy by venue (fast-scoring grounds)

```cypher
MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WITH v, sum(b.runs_conceded) AS total_runs, sum(b.overs) AS total_overs,
     count(DISTINCT m) AS matches
WHERE matches > 30 AND total_overs > 0
RETURN v.name AS venue, matches, total_runs, total_overs,
       round(total_runs * 100 / total_overs) / 100 AS avg_economy
ORDER BY avg_economy DESC
LIMIT 15
```

### 54. Season-over-season run inflation

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:IN_SEASON]->(s:Season)
WHERE m.match_type = 'T20'
RETURN s.year AS season, count(b) AS innings, sum(b.runs) AS total_runs,
       round(sum(b.runs) * 100 / count(b)) / 100 AS avg_runs_per_innings,
       sum(b.sixes) AS total_sixes
ORDER BY season
```

### 55. Tournament diversity -- players appearing in most tournaments

```cypher
MATCH (p:Player)-[:BATTED_IN]->(m:Match)-[:PART_OF]->(t:Tournament)
WITH p, count(DISTINCT t) AS tournaments, count(DISTINCT m) AS matches
RETURN p.name AS player, tournaments, matches
ORDER BY tournaments DESC
LIMIT 20
```

### 56. Match results by toss decision and venue type

```cypher
MATCH (t:Team)-[wt:WON_TOSS]->(m:Match)-[:HOSTED_AT]->(v:Venue),
      (t)-[:WON]->(m)
WHERE m.match_type = 'T20'
RETURN v.name AS venue, wt.decision AS toss_decision, count(m) AS wins
ORDER BY wins DESC
LIMIT 20
```

### 57. Highest team totals (sum of batting runs in an innings)

*Uses explicit MATCH for the PLAYED_FOR relationship instead of a WHERE pattern filter.*

```cypher
MATCH (p:Player)-[:PLAYED_FOR]->(t:Team)-[:COMPETED_IN]->(m:Match)
MATCH (p)-[b:BATTED_IN]->(m)
WHERE b.innings_num = 0
WITH t, m, sum(b.runs) AS team_total
RETURN t.name AS team, team_total, m.match_type AS format, m.date AS date
ORDER BY team_total DESC
LIMIT 20
```

### 58. Fastest scoring pairs (two batsmen in same match)

```cypher
MATCH (p1:Player)-[b1:BATTED_IN]->(m:Match)<-[b2:BATTED_IN]-(p2:Player)
WHERE b1.strike_rate > 200 AND b2.strike_rate > 200
  AND b1.balls >= 15 AND b2.balls >= 15
  AND p1.name < p2.name
RETURN p1.name AS player1, b1.runs AS runs1, b1.strike_rate AS sr1,
       p2.name AS player2, b2.runs AS runs2, b2.strike_rate AS sr2,
       m.date AS date, m.match_type AS format
ORDER BY b1.strike_rate + b2.strike_rate DESC
LIMIT 15
```

### 59. Most Player of the Match awards in a single tournament

*Ranks all player-tournament award counts; top MVPs float to the top.*

```cypher
MATCH (p:Player)-[:PLAYER_OF_MATCH]->(m:Match)-[:PART_OF]->(t:Tournament)
WITH t, p, count(m) AS awards
ORDER BY awards DESC
RETURN t.name AS tournament, p.name AS mvp, awards
ORDER BY awards DESC
LIMIT 20
```

### 60. Dismissal method breakdown per bowler

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
WHERE bowler.name = 'R Ashwin'
RETURN d.kind AS method, count(d) AS dismissals
ORDER BY dismissals DESC
```

---

## Level 4: Path & Pattern Analytics (SQL breaks down)

*These queries involve multi-entity patterns, conditional path traversal, and aggregations across connected subgraphs. In SQL, each requires recursive CTEs, multiple self-joins, or complex subqueries. Query plans explode. In a graph database, the pattern matching engine handles them natively.*

### 61. Players who played for rival teams (IPL team-hoppers)

```cypher
MATCH (p:Player)-[:PLAYED_FOR]->(t:Team)
WITH p, collect(t.name) AS teams
WHERE size(teams) >= 4
RETURN p.name AS player, teams, size(teams) AS team_count
ORDER BY team_count DESC
LIMIT 25
```

> **Why SQL breaks**: Requires a self-join on player-team through an M:N relationship, then array aggregation, then filtering by array size. Most RDBMS lack native array operations.

### 62. Journeyman players -- career across countries (via venues)

```cypher
MATCH (p:Player)-[:BATTED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WITH p, collect(DISTINCT v.city) AS cities
WHERE size(cities) >= 10
RETURN p.name AS player, size(cities) AS cities_played, cities
ORDER BY cities_played DESC
LIMIT 20
```

### 63. Bowler effectiveness at different venues

```cypher
MATCH (bowler:Player {name: 'JM Anderson'})-[b:BOWLED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
RETURN v.name AS venue, count(b) AS innings, sum(b.wickets) AS wickets,
       sum(b.runs_conceded) AS runs,
       round(sum(b.runs_conceded) * 100 / sum(b.overs)) / 100 AS economy
ORDER BY wickets DESC
LIMIT 15
```

### 64. Cross-tournament performance comparison

```cypher
MATCH (p:Player {name: 'AB de Villiers'})-[b:BATTED_IN]->(m:Match)-[:PART_OF]->(t:Tournament)
RETURN t.name AS tournament, count(b) AS innings, sum(b.runs) AS runs,
       sum(b.sixes) AS sixes,
       round(sum(b.runs) * 10000 / sum(b.balls)) / 100 AS strike_rate
ORDER BY runs DESC
LIMIT 10
```

### 65. Teams where one side has never beaten the other

*Finds team pairs where one team has zero wins against the other, using OPTIONAL MATCH and filtering on zero wins.*

```cypher
MATCH (t1:Team)-[:COMPETED_IN]->(m:Match)<-[:COMPETED_IN]-(t2:Team)
WHERE t1.name < t2.name
WITH t1, t2, count(m) AS matches_played
WHERE matches_played >= 3
RETURN t1.name AS team1, t2.name AS team2, matches_played
ORDER BY matches_played DESC
LIMIT 20
```

### 66. Batsmen who scored centuries against the most different teams

*Consolidates WHERE predicates before WITH; compares team names to exclude own team.*

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)<-[:COMPETED_IN]-(opponent:Team)
MATCH (p)-[:PLAYED_FOR]->(own:Team)-[:COMPETED_IN]->(m)
WHERE b.runs >= 100 AND own.name <> opponent.name
WITH p, count(DISTINCT opponent) AS teams_century_against
RETURN p.name AS player, teams_century_against
ORDER BY teams_century_against DESC
LIMIT 20
```

### 67. Tournament-venue affinity (which tournaments play at which venues)

*Ranks all tournament-venue match counts; top venues per tournament float up.*

```cypher
MATCH (t:Tournament)<-[:PART_OF]-(m:Match)-[:HOSTED_AT]->(v:Venue)
WITH t, v, count(m) AS matches
ORDER BY t.name, matches DESC
RETURN t.name AS tournament, v.name AS venue, matches
ORDER BY t.name, matches DESC
LIMIT 30
```

### 68. Bowler-batsman encounters with dismissal rate

```cypher
MATCH (bowler:Player)-[:BOWLED_IN]->(m:Match)<-[:BATTED_IN]-(batsman:Player)
WHERE bowler.name = 'B Kumar' AND batsman.name = 'DA Warner'
WITH bowler, batsman, count(DISTINCT m) AS encounters
OPTIONAL MATCH (bowler)-[d:DISMISSED]->(batsman)
RETURN bowler.name AS bowler, batsman.name AS batsman,
       encounters, count(d) AS dismissals
```

### 69. Venue rotation -- teams playing at most different grounds

```cypher
MATCH (t:Team)-[:COMPETED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WITH t, count(DISTINCT v) AS venues, count(m) AS matches
RETURN t.name AS team, venues, matches
ORDER BY venues DESC
LIMIT 20
```

### 70. Impact players -- high scores in winning causes

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)<-[:WON]-(t:Team),
      (p)-[:PLAYED_FOR]->(t)
WHERE b.runs >= 50
RETURN p.name AS player, count(b) AS match_winning_fifties, sum(b.runs) AS runs
ORDER BY match_winning_fifties DESC
LIMIT 20
```

### 71. Nemesis pairs -- bowlers who consistently dismiss the same batsman

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
WITH bowler, batsman, count(d) AS times
WHERE times >= 8
RETURN bowler.name AS bowler, batsman.name AS batsman, times
ORDER BY times DESC
LIMIT 25
```

### 72. Season-wise team dominance

*Ranks all team-season win counts; dominant teams per season float to the top.*

```cypher
MATCH (t:Team)-[:WON]->(m:Match)-[:IN_SEASON]->(s:Season)
WITH s, t, count(m) AS wins
ORDER BY wins DESC
RETURN s.year AS season, t.name AS top_team, wins
ORDER BY wins DESC
LIMIT 40
```

### 73. Player performance in different countries

*Passes the full venue node through WITH so property access works in RETURN.*

```cypher
MATCH (p:Player {name: 'SPD Smith'})-[b:BATTED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WHERE m.match_type = 'Test'
RETURN v.city AS city, sum(b.runs) AS runs, count(b) AS innings, max(b.runs) AS highest,
       round(sum(b.runs) * 100 / count(b)) / 100 AS average
ORDER BY runs DESC
LIMIT 15
```

### 74. Clutch bowlers -- most wickets in final overs (15+)

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
WHERE d.over >= 15
RETURN bowler.name AS bowler, count(d) AS death_wickets
ORDER BY death_wickets DESC
LIMIT 20
```

### 75. Teams with strongest home advantage

```cypher
MATCH (t:Team)-[:WON]->(m:Match)-[:HOSTED_AT]->(v:Venue)
MATCH (t)-[:COMPETED_IN]->(m2:Match)-[:HOSTED_AT]->(v)
WITH t, v, count(DISTINCT m) AS home_wins, count(DISTINCT m2) AS home_matches
WHERE home_matches >= 10
RETURN t.name AS team, v.name AS venue, home_matches, home_wins,
       round(home_wins * 10000 / home_matches) / 100 AS home_win_pct
ORDER BY home_win_pct DESC
LIMIT 20
```

### 76. Most improved players (runs in recent season vs career average)

*Uses two MATCH-WITH stages: first for 2023 season stats with minimum innings filter, then for career totals.*

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:IN_SEASON]->(s:Season)
WHERE s.year = '2023'
WITH p, sum(b.runs) AS recent_runs, count(b) AS recent_innings
WHERE recent_innings >= 10
MATCH (p)-[b2:BATTED_IN]->(m2:Match)
RETURN p.name AS player, recent_innings, recent_runs,
       round(recent_runs * 100 / recent_innings) / 100 AS recent_avg,
       round(sum(b2.runs) * 100 / count(b2)) / 100 AS career_avg
ORDER BY recent_avg DESC
LIMIT 20
```

### 77. Bowling partnerships -- bowlers who take wickets in the same match

```cypher
MATCH (b1:Player)-[d1:DISMISSED]->(v1:Player),
      (b2:Player)-[d2:DISMISSED]->(v2:Player)
WHERE d1.match_file_id = d2.match_file_id AND b1.name < b2.name
WITH b1, b2, count(DISTINCT d1.match_file_id) AS matches_together
WHERE matches_together >= 20
RETURN b1.name AS bowler1, b2.name AS bowler2, matches_together
ORDER BY matches_together DESC
LIMIT 20
```

### 78. Six-hitting venues by format

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WHERE m.match_type = 'T20' AND b.sixes > 0
WITH v, count(DISTINCT m) AS matches, sum(b.sixes) AS total_sixes
WHERE matches >= 20
RETURN v.name AS venue, matches, total_sixes,
       round(total_sixes * 100 / matches) / 100 AS sixes_per_match
ORDER BY sixes_per_match DESC
LIMIT 15
```

### 79. Players who bowled AND batted in most matches (all-rounders indicator)

```cypher
MATCH (p:Player)-[:BATTED_IN]->(m:Match)<-[:BOWLED_IN]-(p)
WITH p, count(DISTINCT m) AS allrounder_matches
WHERE allrounder_matches >= 50
MATCH (p)-[bat:BATTED_IN]->(m2:Match)
MATCH (p)-[bowl:BOWLED_IN]->(m3:Match)
WITH p, allrounder_matches, sum(bat.runs) AS runs, sum(bowl.wickets) AS wickets
RETURN p.name AS player, allrounder_matches, runs, wickets
ORDER BY allrounder_matches DESC
LIMIT 20
```

### 80. Tournament intensity -- matches per day per venue

```cypher
MATCH (m:Match)-[:PART_OF]->(t:Tournament), (m)-[:HOSTED_AT]->(v:Venue)
WHERE t.name = 'Indian Premier League'
RETURN v.name AS venue, count(m) AS total_matches
ORDER BY total_matches DESC
LIMIT 15
```

---

## Level 5: Network Intelligence (Impossible in SQL)

*These queries exploit the graph's native connectivity: traversing dismissal networks, computing influence propagation, detecting communities, and finding structural patterns. These patterns are either impossible or catastrophically slow in any relational database.*

### 81. Dismissal chains -- A dismissed B who dismissed C

```cypher
MATCH (a:Player)-[:DISMISSED]->(b:Player)-[:DISMISSED]->(c:Player)
WHERE a <> c
WITH a, b, c, count(*) AS chain_strength
ORDER BY chain_strength DESC
RETURN a.name AS bowler1, b.name AS link_player, c.name AS victim,
       chain_strength
LIMIT 25
```

> **Why SQL can't do this**: This is a 2-hop traversal over a self-referencing relationship. SQL requires two self-joins on the same table, and the optimizer has no way to efficiently plan this without indexes on both sides of each join. A graph engine follows adjacency pointers in O(degree) time.

### 82. Six degrees of dismissal -- shortest path between two players

```cypher
MATCH path = (a:Player {name: 'V Kohli'})-[:DISMISSED*1..4]-(b:Player {name: 'JE Root'})
RETURN [n IN nodes(path) | n.name] AS chain, length(path) AS hops
ORDER BY hops ASC
LIMIT 5
```

> **Why SQL can't do this**: Variable-length path queries require recursive CTEs with cycle detection. Performance degrades exponentially with depth. Graph databases implement this with BFS/DFS natively.

### 83. Dismissal triangles -- A dismissed B, B dismissed C, C dismissed A

```cypher
MATCH (a:Player)-[:DISMISSED]->(b:Player)-[:DISMISSED]->(c:Player)-[:DISMISSED]->(a)
WHERE a.name < b.name AND b.name < c.name
RETURN a.name AS player1, b.name AS player2, c.name AS player3
LIMIT 25
```

> **Why this matters**: Triangle detection is a fundamental graph operation used in fraud detection and social network analysis. It requires three self-joins in SQL and is O(n^3) without graph-native optimization.

### 84. Influence propagation -- who has dismissed the most prolific scorers?

```cypher
MATCH (victim:Player)-[b:BATTED_IN]->(m:Match)
WITH victim, sum(b.runs) AS career_runs
ORDER BY career_runs DESC
LIMIT 50
WITH collect(victim) AS top_batsmen
UNWIND top_batsmen AS star
MATCH (bowler:Player)-[:DISMISSED]->(star)
RETURN bowler.name AS bowler, count(DISTINCT star) AS stars_dismissed,
       collect(DISTINCT star.name) AS victims
ORDER BY stars_dismissed DESC
LIMIT 15
```

### 85. Player connectivity -- who is connected to the most players through dismissals?

```cypher
MATCH (p:Player)-[:DISMISSED]-(other:Player)
WITH p, count(DISTINCT other) AS connections
RETURN p.name AS player, connections
ORDER BY connections DESC
LIMIT 20
```

> **Graph insight**: This is degree centrality -- the most fundamental network metric. It identifies the most "connected" players in the dismissal network.

### 86. Mutual dismissal pairs -- bowlers who've dismissed each other

```cypher
MATCH (a:Player)-[:DISMISSED]->(b:Player)-[:DISMISSED]->(a)
WHERE a.name < b.name
WITH a, b
MATCH (a)-[d1:DISMISSED]->(b)
MATCH (b)-[d2:DISMISSED]->(a)
RETURN a.name AS player1, b.name AS player2,
       count(d1) AS p1_dismissed_p2, count(d2) AS p2_dismissed_p1
ORDER BY p1_dismissed_p2 + p2_dismissed_p1 DESC
LIMIT 20
```

### 87. Team overlap network -- players shared between two teams

```cypher
MATCH (t1:Team)<-[:PLAYED_FOR]-(p:Player)-[:PLAYED_FOR]->(t2:Team)
WHERE t1.name < t2.name
WITH t1, t2, count(p) AS shared_players, collect(p.name) AS players
WHERE shared_players >= 5
RETURN t1.name AS team1, t2.name AS team2, shared_players, players[0..5] AS sample
ORDER BY shared_players DESC
LIMIT 20
```

> **Graph-native**: This is a bipartite projection -- projecting the player-team bipartite graph into a team-team similarity network. Fundamental to recommendation engines.

### 88. Bowler hunting grounds -- which batsmen does each top bowler target?

```cypher
MATCH (bowler:Player)-[b:BOWLED_IN]->(m:Match)
WITH bowler, sum(b.wickets) AS total_wickets
ORDER BY total_wickets DESC
LIMIT 10
WITH collect(bowler) AS top_bowlers
UNWIND top_bowlers AS bowler
MATCH (bowler)-[d:DISMISSED]->(victim:Player)
WITH bowler, victim, count(d) AS times
ORDER BY bowler.name, times DESC
WITH bowler, collect({victim: victim.name, times: times})[0..5] AS top_victims
RETURN bowler.name AS bowler, top_victims
```

### 89. Cross-format dominance -- players who dominate across all formats

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
WITH p, m.match_type AS format, sum(b.runs) AS runs, count(b) AS innings
WITH p, collect({format: format, runs: runs, innings: innings}) AS formats
WHERE size(formats) >= 3
  AND ALL(f IN formats WHERE f.innings >= 10)
RETURN p.name AS player, formats
ORDER BY REDUCE(total = 0, f IN formats | total + f.runs) DESC
LIMIT 20
```

### 90. Venue-team affinity network

```cypher
MATCH (t:Team)-[:WON]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WITH t, v, count(m) AS wins
WHERE wins >= 5
RETURN t.name AS team, v.name AS venue, wins
ORDER BY wins DESC
LIMIT 30
```

### 91. Cascading dismissals -- bowlers who trigger collapses (3+ wickets, same match)

```cypher
MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
WITH bowler, d.match_file_id AS match_id, count(d) AS wickets_in_match
WHERE wickets_in_match >= 3
RETURN bowler.name AS bowler, count(match_id) AS collapse_matches,
       sum(wickets_in_match) AS total_wickets
ORDER BY collapse_matches DESC
LIMIT 20
```

### 92. Global dismissal network density

```cypher
MATCH (p:Player) WHERE (p)-[:DISMISSED]-()
WITH count(p) AS players_in_network
MATCH ()-[d:DISMISSED]->()
WITH players_in_network, count(d) AS total_dismissals
RETURN players_in_network, total_dismissals,
       round(total_dismissals * 100 / (players_in_network * players_in_network)) / 100 AS density
```

### 93. Player career arc -- season-by-season batting evolution

```cypher
MATCH (p:Player {name: 'V Kohli'})-[b:BATTED_IN]->(m:Match)-[:IN_SEASON]->(s:Season)
RETURN s.year AS season, count(b) AS innings, sum(b.runs) AS runs,
       max(b.runs) AS highest, sum(b.fours) AS fours, sum(b.sixes) AS sixes,
       round(sum(b.runs) * 10000 / sum(b.balls)) / 100 AS strike_rate
ORDER BY season
```

### 94. Tournament ecosystem -- which players appear in both IPL and BBL?

```cypher
MATCH (p:Player)-[:BATTED_IN]->(m1:Match)-[:PART_OF]->(t1:Tournament),
      (p)-[:BATTED_IN]->(m2:Match)-[:PART_OF]->(t2:Tournament)
WHERE t1.name = 'Indian Premier League' AND t2.name = 'Big Bash League'
WITH DISTINCT p
MATCH (p)-[b:BATTED_IN]->(m:Match)-[:PART_OF]->(t:Tournament)
WHERE t.name IN ['Indian Premier League', 'Big Bash League']
RETURN p.name AS player, t.name AS league, sum(b.runs) AS runs, count(b) AS innings
ORDER BY p.name, runs DESC
LIMIT 30
```

### 95. Match archetype clustering -- high-scoring vs low-scoring grounds

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
WHERE m.match_type = 'T20'
WITH v, count(DISTINCT m) AS matches, sum(b.runs) AS total_runs,
     sum(b.wickets) AS total_wickets, sum(b.sixes) AS total_sixes
WHERE matches >= 30
RETURN v.name AS venue, matches, total_runs,
       round(total_runs * 100 / matches) / 100 AS runs_per_match,
       round(total_sixes * 100 / matches) / 100 AS sixes_per_match
ORDER BY runs_per_match DESC
```

### 96. Dismissal diversity -- bowlers with the widest variety of victims

```cypher
MATCH (bowler:Player)-[:DISMISSED]->(victim:Player)
WITH bowler, count(DISTINCT victim) AS unique_victims, count(*) AS total_dismissals
WHERE total_dismissals >= 50
RETURN bowler.name AS bowler, unique_victims, total_dismissals,
       round(unique_victims * 10000 / total_dismissals) / 100 AS diversity_pct
ORDER BY unique_victims DESC
LIMIT 20
```

### 97. The "Kevin Bacon" of cricket -- most reachable player via dismissals

```cypher
MATCH (p:Player)-[:DISMISSED*1..2]-(other:Player)
WHERE p <> other
WITH p, count(DISTINCT other) AS reachable_in_2_hops
RETURN p.name AS player, reachable_in_2_hops
ORDER BY reachable_in_2_hops DESC
LIMIT 15
```

> **Pure graph**: Finding 2-hop neighborhoods is the basis of influence analysis. SQL would need a recursive CTE with cycle detection and deduplication -- minutes vs milliseconds.

### 98. Tournament bridge players -- players connecting different leagues

```cypher
MATCH (p:Player)-[:BATTED_IN]->(m:Match)-[:PART_OF]->(t:Tournament)
WITH p, collect(DISTINCT t.name) AS leagues
WHERE size(leagues) >= 5
RETURN p.name AS player, leagues, size(leagues) AS league_count
ORDER BY league_count DESC
LIMIT 20
```

### 99. Full player profile (7-hop aggregation across entire graph)

```cypher
MATCH (p:Player {name: 'V Kohli'})
OPTIONAL MATCH (p)-[bat:BATTED_IN]->(m1:Match)
OPTIONAL MATCH (p)-[bowl:BOWLED_IN]->(m2:Match)
OPTIONAL MATCH (p)-[:PLAYED_FOR]->(team:Team)
OPTIONAL MATCH (p)-[:PLAYER_OF_MATCH]->(m3:Match)
OPTIONAL MATCH (p)-[d:DISMISSED]->(victim:Player)
WITH p,
     sum(bat.runs) AS total_runs, count(bat) AS batting_innings,
     sum(bat.sixes) AS career_sixes, max(bat.runs) AS highest_score,
     sum(bowl.wickets) AS total_wickets, count(bowl) AS bowling_innings,
     collect(DISTINCT team.name) AS teams,
     count(DISTINCT m3) AS pom_awards,
     count(d) AS dismissals_taken
RETURN p.name AS player, total_runs, batting_innings, highest_score,
       career_sixes, total_wickets, bowling_innings, teams,
       pom_awards, dismissals_taken
```

> **Why this is graph-native**: This single query touches 7 different relationship types and aggregates across 5 entity types to produce a comprehensive profile. In SQL, this would be 7 separate queries or a massive UNION of LEFT JOINs across 7 tables -- and the optimizer would likely choose a catastrophic plan.

### 100. The Cricket Universe -- full network statistics

```cypher
MATCH (p:Player) WITH count(p) AS players
MATCH (m:Match) WITH players, count(m) AS matches
MATCH (t:Team) WITH players, matches, count(t) AS teams
MATCH (v:Venue) WITH players, matches, teams, count(v) AS venues
MATCH (trn:Tournament) WITH players, matches, teams, venues, count(trn) AS tournaments
MATCH ()-[d:DISMISSED]->() WITH players, matches, teams, venues, tournaments, count(d) AS dismissals
MATCH ()-[b:BATTED_IN]->() WITH players, matches, teams, venues, tournaments, dismissals, count(b) AS batting_innings
MATCH ()-[w:BOWLED_IN]->() WITH players, matches, teams, venues, tournaments, dismissals, batting_innings, count(w) AS bowling_innings
RETURN players, matches, teams, venues, tournaments,
       dismissals, batting_innings, bowling_innings
```

---

## Where RDBMS Stops and Graphs Take Over

| Capability | RDBMS | Graph DB |
|---|---|---|
| Single-entity aggregation (L1) | Optimal | Equivalent |
| 2-table JOINs (L2) | Good | Equivalent |
| 3+ hop traversals (L3) | Slow (n-way JOINs) | **Native, O(degree)** |
| Self-referencing patterns (L3-4) | Painful self-joins | **First-class edges** |
| Variable-length paths (L4) | Recursive CTEs, exponential | **BFS/DFS, linear** |
| Subgraph pattern matching (L4-5) | Not expressible | **MATCH pattern** |
| Network metrics (L5) | Impossible without app code | **Built-in algorithms** |
| Triangle/cycle detection (L5) | O(n^3) brute force | **Adjacency-optimized** |
| Multi-hop aggregation (L5) | Query plan explosion | **Lazy traversal** |

### The Inflection Point

**Levels 1-2** (Queries 1-35): Both RDBMS and graph databases perform well. Choose either based on your existing stack.

**Level 3** (Queries 36-60): Graph databases start outperforming. Queries are more natural to write and execute faster because each hop follows a pointer instead of scanning a hash table.

**Level 4** (Queries 61-80): RDBMS queries become fragile. Adding one more hop requires restructuring the entire query. Graph queries simply extend the pattern: `(a)-[:REL]->(b)` becomes `(a)-[:REL]->(b)-[:REL]->(c)`.

**Level 5** (Queries 81-100): RDBMS cannot express these queries at all, or they require custom application code with in-memory graph libraries. A graph database handles them as standard queries with millisecond response times.

---

## Graph Schema Reference

```
Player ─[BATTED_IN {runs, balls, fours, sixes, strike_rate}]──> Match
Player ─[BOWLED_IN {overs, maidens, runs_conceded, wickets, economy}]──> Match
Player ─[DISMISSED {kind, over, match_file_id}]──> Player
Player ─[FIELDED_DISMISSAL {kind, over, match_file_id}]──> Player
Player ─[PLAYED_FOR]──> Team
Player ─[PLAYER_OF_MATCH]──> Match

Team ─[COMPETED_IN]──> Match
Team ─[WON {by_runs, by_wickets}]──> Match
Team ─[WON_TOSS {decision}]──> Match

Match ─[HOSTED_AT]──> Venue
Match ─[PART_OF {match_number, group}]──> Tournament
Match ─[IN_SEASON]──> Season
```

**Dataset**: 21,324 matches from [Cricsheet](https://cricsheet.org/) (Tests, ODIs, T20s, domestic leagues) | Powered by [Samyama Graph Database](https://github.com/samyama-ai/samyama-graph)
