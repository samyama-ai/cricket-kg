"""Verify all numbers cited in the LinkedIn post."""

import sys
import gc
sys.path.insert(0, ".")
from samyama import SamyamaClient
from etl.loader import load_cricket

GRAPH = "default"


def q(client, cypher):
    r = client.query_readonly(cypher, GRAPH)
    return [dict(zip(r.columns, row)) for row in r.records]


def verify(client):
    print("=" * 60, flush=True)
    print("VERIFYING LINKEDIN POST NUMBERS", flush=True)
    print("=" * 60, flush=True)

    # 1. Graph stats (cited in post)
    print("\n--- GRAPH STATS ---", flush=True)
    for label in ["Player", "Match", "Venue", "Team", "Tournament", "Season"]:
        rows = q(client, f"MATCH (n:{label}) RETURN count(n) AS c")
        print(f"  {label}: {rows[0]['c']:,}", flush=True)
    gc.collect()

    total_edges = q(client, "MATCH ()-[r]->() RETURN count(r) AS c")[0]["c"]
    print(f"  Total edges: {total_edges:,}", flush=True)
    print(f"  POST SAYS: 12,933 players, 383 teams, 877 venues, 1,053 tournaments, 1.37M edges", flush=True)
    gc.collect()

    # 2. Dismissal network: Broad→Warner, Ashwin→Warner, Ashwin→Stokes
    print("\n--- DISMISSAL NETWORK ---", flush=True)
    pairs = [
        ("SCJ Broad", "DA Warner"),
        ("R Ashwin", "DA Warner"),
        ("R Ashwin", "BA Stokes"),
    ]
    for bowler, batsman in pairs:
        rows = q(client, f"""
            MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
            WHERE bowler.name = "{bowler}" AND batsman.name = "{batsman}"
            RETURN count(d) AS times
        """)
        count = rows[0]["times"] if rows else 0
        print(f"  {bowler} → {batsman}: {count} dismissals", flush=True)
    print(f"  POST SAYS: Broad→Warner: 20, Ashwin→Warner: 17, Ashwin→Stokes: 17", flush=True)
    gc.collect()

    # 3. Aus bowlers vs Indian batsmen: Cummins, Starc, Lyon
    print("\n--- AUS BOWLERS VS INDIAN BATSMEN ---", flush=True)
    for bowler_name in ["PJ Cummins", "MA Starc", "NM Lyon"]:
        rows = q(client, f"""
            MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)-[:PLAYED_FOR]->(t:Team),
                  (bowler)-[:PLAYED_FOR]->(bt:Team)
            WHERE bowler.name = "{bowler_name}" AND t.name = "India" AND bt.name = "Australia"
            RETURN count(d) AS dismissals
        """)
        count = rows[0]["dismissals"] if rows else 0
        print(f"  {bowler_name}: {count} dismissals of Indian batsmen", flush=True)
        gc.collect()
    print(f"  POST SAYS: Cummins: 124, Starc: 119, Lyon: 119", flush=True)

    # 4. Most sixes: Gayle, Pollard, Rohit, Pooran, Russell
    print("\n--- MOST SIXES ---", flush=True)
    for name in ["CH Gayle", "KA Pollard", "RG Sharma", "N Pooran", "AD Russell"]:
        rows = q(client, f"""
            MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
            WHERE p.name = "{name}"
            RETURN sum(b.sixes) AS sixes
        """)
        sixes = rows[0]["sixes"] if rows else 0
        print(f"  {name}: {sixes:,.0f} sixes", flush=True)
        gc.collect()
    print(f"  POST SAYS: Gayle: 1,243, Pollard: 948, Rohit: 937, Pooran: 824, Russell: 802", flush=True)

    print("\n" + "=" * 60, flush=True)
    print("VERIFICATION COMPLETE", flush=True)


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "data/json"
    print(f"Loading data from {data_dir}...", flush=True)
    c = SamyamaClient.embedded()
    stats = load_cricket(c, data_dir=data_dir)
    print(f"\nLoad complete: {stats}", flush=True)
    verify(c)
