"""Run all README showcase queries against a loaded cricket-kg graph."""

import sys
import time
sys.path.insert(0, ".")
from samyama import SamyamaClient
from etl.loader import load_cricket

GRAPH = "default"


def q(client, cypher):
    r = client.query_readonly(cypher, GRAPH)
    return [dict(zip(r.columns, row)) for row in r.records]


def run_all(client):
    print("=" * 70)
    print("CRICKET-KG FULL DATASET QUERIES")
    print("=" * 70)

    # Graph stats
    print("\n## Graph Statistics\n")
    for label in ["Player", "Match", "Venue", "Team", "Tournament", "Season"]:
        rows = q(client, f"MATCH (n:{label}) RETURN count(n) AS c")
        print(f"  {label:20s} {rows[0]['c']:>8,}")

    total_nodes = q(client, "MATCH (n) RETURN count(n) AS c")[0]["c"]
    total_edges = q(client, "MATCH ()-[r]->() RETURN count(r) AS c")[0]["c"]
    print(f"\n  {'Total nodes':20s} {total_nodes:>8,}")
    print(f"  {'Total edges':20s} {total_edges:>8,}")

    # Top run scorers (all formats)
    print("\n## Top Run Scorers (all formats)\n")
    rows = q(client, """
        MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
        RETURN p.name AS player, sum(b.runs) AS runs, count(b) AS innings,
               sum(b.balls) AS balls, sum(b.fours) AS fours, sum(b.sixes) AS sixes
        ORDER BY runs DESC LIMIT 10
    """)
    print(f"  {'Player':25s} {'Runs':>8s} {'Inn':>6s} {'Balls':>8s} {'4s':>6s} {'6s':>6s}")
    for r in rows:
        print(f"  {str(r['player']):25s} {r['runs']:>8,} {r['innings']:>6,} {r['balls']:>8,} {r['fours']:>6,} {r['sixes']:>6,}")

    # Top wicket takers (all formats)
    print("\n## Top Wicket Takers (all formats)\n")
    rows = q(client, """
        MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)
        RETURN p.name AS player, sum(b.wickets) AS wickets,
               sum(b.runs_conceded) AS runs, count(b) AS innings
        ORDER BY wickets DESC LIMIT 10
    """)
    print(f"  {'Player':25s} {'Wkts':>6s} {'Inn':>6s} {'Runs':>8s}")
    for r in rows:
        print(f"  {str(r['player']):25s} {r['wickets']:>6,} {r['innings']:>6,} {r['runs']:>8,}")

    # Dismissal network
    print("\n## Dismissal Network: who gets whom out?\n")
    rows = q(client, """
        MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
        RETURN bowler.name AS bowler, batsman.name AS batsman,
               count(d) AS times, collect(d.kind) AS how
        ORDER BY times DESC LIMIT 10
    """)
    print(f"  {'Bowler':25s} {'Batsman':25s} {'Times':>6s}")
    for r in rows:
        print(f"  {str(r['bowler']):25s} {str(r['batsman']):25s} {r['times']:>6,}")

    # Multi-hop: Aus bowlers vs Indian batsmen
    print("\n## Multi-hop: Australian bowlers vs Indian batsmen\n")
    rows = q(client, """
        MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)-[:PLAYED_FOR]->(t:Team),
              (bowler)-[:PLAYED_FOR]->(bt:Team)
        WHERE t.name = "India" AND bt.name = "Australia"
        RETURN bowler.name AS bowler, count(d) AS dismissals,
               collect(DISTINCT batsman.name) AS victims
        ORDER BY dismissals DESC LIMIT 10
    """)
    print(f"  {'Bowler':25s} {'Dismissals':>10s}")
    for r in rows:
        print(f"  {str(r['bowler']):25s} {r['dismissals']:>10,}")

    # Most sixes
    print("\n## Most Sixes (all formats)\n")
    rows = q(client, """
        MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
        RETURN p.name AS player, sum(b.sixes) AS sixes, sum(b.runs) AS runs
        ORDER BY sixes DESC LIMIT 10
    """)
    print(f"  {'Player':25s} {'Sixes':>6s} {'Runs':>8s}")
    for r in rows:
        print(f"  {str(r['player']):25s} {r['sixes']:>6,} {r['runs']:>8,}")

    # Top fielders
    print("\n## Top Fielders (catches)\n")
    rows = q(client, """
        MATCH (fielder:Player)-[d:FIELDED_DISMISSAL]->(batsman:Player)
        WHERE d.kind = "caught"
        RETURN fielder.name AS fielder, count(d) AS catches
        ORDER BY catches DESC LIMIT 10
    """)
    print(f"  {'Fielder':25s} {'Catches':>8s}")
    for r in rows:
        print(f"  {str(r['fielder']):25s} {r['catches']:>8,}")

    # Player of match
    print("\n## Player of Match Awards\n")
    rows = q(client, """
        MATCH (p:Player)-[:PLAYER_OF_MATCH]->(m:Match)
        RETURN p.name AS player, count(m) AS awards
        ORDER BY awards DESC LIMIT 10
    """)
    print(f"  {'Player':25s} {'Awards':>7s}")
    for r in rows:
        print(f"  {str(r['player']):25s} {r['awards']:>7,}")

    # Most successful teams
    print("\n## Most Successful Teams\n")
    rows = q(client, """
        MATCH (t:Team)-[w:WON]->(m:Match)
        RETURN t.name AS team, count(w) AS wins
        ORDER BY wins DESC LIMIT 10
    """)
    print(f"  {'Team':30s} {'Wins':>6s}")
    for r in rows:
        print(f"  {str(r['team']):30s} {r['wins']:>6,}")

    # Tournaments
    print("\n## Tournaments\n")
    rows = q(client, """
        MATCH (m:Match)-[:PART_OF]->(t:Tournament)
        RETURN t.name AS tournament, count(m) AS matches
        ORDER BY matches DESC LIMIT 10
    """)
    print(f"  {'Tournament':40s} {'Matches':>8s}")
    for r in rows:
        print(f"  {str(r['tournament']):40s} {r['matches']:>8,}")

    # Top venues
    print("\n## Top Venues\n")
    rows = q(client, """
        MATCH (m:Match)-[:HOSTED_AT]->(v:Venue)
        RETURN v.name AS venue, v.city AS city, count(m) AS matches
        ORDER BY matches DESC LIMIT 10
    """)
    print(f"  {'Venue':40s} {'City':15s} {'Matches':>8s}")
    for r in rows:
        city = str(r.get("city", "—") or "—")
        print(f"  {str(r['venue']):40s} {city:15s} {r['matches']:>8,}")

    # Match format breakdown
    print("\n## Matches by Format\n")
    rows = q(client, """
        MATCH (m:Match)
        RETURN m.match_type AS format, count(m) AS matches
        ORDER BY matches DESC
    """)
    print(f"  {'Format':20s} {'Matches':>8s}")
    for r in rows:
        print(f"  {str(r['format']):20s} {r['matches']:>8,}")

    # ---- Graph-Only Queries (multi-hop, impossible with flat tables) ----

    print("\n" + "=" * 70)
    print("QUERIES ONLY A GRAPH CAN ANSWER")
    print("=" * 70)

    # 1. Bowlers who dismissed batsmen who scored 50+ against their own team
    print("\n## Bowlers who dismissed 50+ scorers playing against their team\n")
    rows = q(client, """
        MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)-[b:BATTED_IN]->(m:Match),
              (bowler)-[:PLAYED_FOR]->(t:Team)-[:COMPETED_IN]->(m)
        WHERE b.runs >= 50
        RETURN bowler.name AS bowler, batsman.name AS batsman, b.runs AS runs, d.kind AS how
        ORDER BY runs DESC LIMIT 15
    """)
    print(f"  {'Bowler':25s} {'Batsman':25s} {'Runs':>6s} {'How':15s}")
    for r in rows:
        print(f"  {str(r['bowler']):25s} {str(r['batsman']):25s} {r['runs']:>6,.0f} {str(r['how']):15s}")

    # 2. Bowler effectiveness at specific venues
    print("\n## Bowler effectiveness at specific venues\n")
    rows = q(client, """
        MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player),
              (bowler)-[:BOWLED_IN]->(m:Match)-[:HOSTED_AT]->(v:Venue)
        RETURN bowler.name AS bowler, v.name AS venue, count(d) AS dismissals
        ORDER BY dismissals DESC LIMIT 15
    """)
    print(f"  {'Bowler':25s} {'Venue':40s} {'Dismissals':>10s}")
    for r in rows:
        print(f"  {str(r['bowler']):25s} {str(r['venue']):40s} {r['dismissals']:>10,}")

    # 3. Tournament-spanning player journeys
    print("\n## Tournament-spanning player journeys\n")
    rows = q(client, """
        MATCH (p:Player)-[:BATTED_IN]->(m:Match)-[:PART_OF]->(t:Tournament)
        RETURN p.name AS player, collect(DISTINCT t.name) AS tournaments, count(DISTINCT t) AS count
        ORDER BY count DESC LIMIT 10
    """)
    print(f"  {'Player':25s} {'Tournaments':>12s}")
    for r in rows:
        print(f"  {str(r['player']):25s} {r['count']:>12,}")

    print("\n" + "=" * 70)
    print("DONE")


if __name__ == "__main__":
    # Load data first, then run queries
    data_dir = sys.argv[1] if len(sys.argv) > 1 else None

    if data_dir:
        print(f"Loading data from {data_dir}...")
        c = SamyamaClient.embedded()
        stats = load_cricket(c, data_dir=data_dir)
        print(f"\nLoad complete: {stats}")
        run_all(c)
    else:
        print("Usage: python scripts/run_queries.py <data-dir>")
        print("  or import and call run_all(client) with a pre-loaded client")
