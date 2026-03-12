"""
Cricket Knowledge Graph ETL Loader
===================================
Loads ball-by-ball cricket data from Cricsheet JSON files into a Samyama
property graph. Creates Player, Team, Match, Venue, Tournament, Season nodes
and relationship edges for batting, bowling, dismissals, and partnerships.

Data source: https://cricsheet.org/downloads/all_json.zip
License: CC-BY-4.0

Usage:
    python -m etl.loader --data-dir data/json --max-matches 100
    python -m etl.loader --data-dir data/json --match-type T20 --gender male
"""

import json
import glob
import time
import os
import sys
from pathlib import Path
from collections import defaultdict
from samyama import SamyamaClient

GRAPH = "default"


# ---------------------------------------------------------------------------
# Cypher helpers
# ---------------------------------------------------------------------------

def _escape(value) -> str:
    if value is None:
        return ""
    return str(value).replace('"', '').replace("\n", " ").replace("\r", "")


def _q(val) -> str:
    return f'"{_escape(val)}"'


def _prop_str(props: dict) -> str:
    parts = []
    for key, val in props.items():
        if val is None:
            continue
        if isinstance(val, (int, float)):
            parts.append(f"{key}: {val}")
        else:
            parts.append(f'{key}: {_q(val)}')
    return "{" + ", ".join(parts) + "}"


def _create_node(client, label, props):
    client.query(f"CREATE (n:{label} {_prop_str(props)})", GRAPH)


def _match_to_where(var_name, match_str):
    """Convert 'prop: "value"' to 'var.prop = "value"' for WHERE clause.

    Index scans only trigger with WHERE, not inline MATCH properties.
    """
    return f"{var_name}.{match_str.replace(': ', ' = ', 1)}"


def _create_edge(client, src_label, src_match, rel, tgt_label, tgt_match, props=None):
    prop_part = f" {_prop_str(props)}" if props else ""
    q = (f"MATCH (a:{src_label}), (b:{tgt_label}) "
         f"WHERE {_match_to_where('a', src_match)} AND {_match_to_where('b', tgt_match)} "
         f"CREATE (a)-[:{rel}{prop_part}]->(b)")
    client.query(q, GRAPH)


def _batch_create_nodes(client, nodes):
    """Create multiple nodes in a single CREATE query.

    Each node is a tuple: (label, props_dict)
    """
    if not nodes:
        return
    parts = [f"(:{label} {_prop_str(props)})" for label, props in nodes]
    client.query(f"CREATE {', '.join(parts)}", GRAPH)


def _batch_create_edges(client, edges):
    """Create multiple edges in a single MATCH ... WHERE ... CREATE query.

    Each edge is a tuple:
        (src_label, src_match_str, rel_type, tgt_label, tgt_match_str, props_or_None)

    Uses WHERE clause (not inline properties) to trigger index scans.
    Deduplicates MATCH patterns so each unique node is matched once.
    """
    if not edges:
        return
    var_map = {}
    match_parts = []
    where_parts = []
    create_parts = []

    for src_label, src_match, rel, tgt_label, tgt_match, props in edges:
        src_key = (src_label, src_match)
        tgt_key = (tgt_label, tgt_match)

        if src_key not in var_map:
            vname = f"n{len(var_map)}"
            var_map[src_key] = vname
            match_parts.append(f"({vname}:{src_label})")
            where_parts.append(_match_to_where(vname, src_match))

        if tgt_key not in var_map:
            vname = f"n{len(var_map)}"
            var_map[tgt_key] = vname
            match_parts.append(f"({vname}:{tgt_label})")
            where_parts.append(_match_to_where(vname, tgt_match))

        src_var = var_map[src_key]
        tgt_var = var_map[tgt_key]
        prop_part = f" {_prop_str(props)}" if props else ""
        create_parts.append(f"({src_var})-[:{rel}{prop_part}]->({tgt_var})")

    q = (f"MATCH {', '.join(match_parts)} "
         f"WHERE {' AND '.join(where_parts)} "
         f"CREATE {', '.join(create_parts)}")
    client.query(q, GRAPH)


# ---------------------------------------------------------------------------
# Node dedup tracking (Registry checks, no queries)
# ---------------------------------------------------------------------------

class Registry:
    """Tracks created entities to avoid duplicate node CREATEs."""
    def __init__(self):
        self.players: set[str] = set()       # cricsheet_id
        self.teams: set[str] = set()         # name.lower()
        self.venues: set[str] = set()        # name.lower()
        self.tournaments: set[str] = set()   # name.lower()
        self.seasons: set[str] = set()       # year string
        self.player_teams: set[str] = set()  # "player_id|team" edge dedup


def _check_player(reg, name, cricsheet_id):
    """Return (label, props) if new, else None."""
    if cricsheet_id in reg.players:
        return None
    reg.players.add(cricsheet_id)
    return ("Player", {"cricsheet_id": cricsheet_id, "name": name})


def _check_team(reg, name):
    key = name.lower()
    if key in reg.teams:
        return None
    reg.teams.add(key)
    return ("Team", {"name": name})


def _check_venue(reg, venue, city=None):
    key = venue.lower()
    if key in reg.venues:
        return None
    reg.venues.add(key)
    props = {"name": venue}
    if city:
        props["city"] = city
    return ("Venue", props)


def _check_tournament(reg, name):
    key = name.lower()
    if key in reg.tournaments:
        return None
    reg.tournaments.add(key)
    return ("Tournament", {"name": name})


def _check_season(reg, season):
    key = str(season)
    if key in reg.seasons:
        return None
    reg.seasons.add(key)
    return ("Season", {"year": season})


# ---------------------------------------------------------------------------
# Match ingestion (batched nodes + batched edges = 2 queries per match)
# ---------------------------------------------------------------------------

def _ingest_match(client, data, file_id, reg, counts):
    info = data.get("info", {})
    innings_data = data.get("innings", [])

    teams = info.get("teams", [])
    if len(teams) < 2:
        return

    match_type = info.get("match_type", "")
    gender = info.get("gender", "")
    season = str(info.get("season", ""))
    venue = info.get("venue", "")
    city = info.get("city", "")
    event = info.get("event", {})
    tournament_name = event.get("name", "") if isinstance(event, dict) else ""
    dates = info.get("dates", [])
    date = dates[0] if dates else ""
    outcome = info.get("outcome", {})
    toss = info.get("toss", {})
    registry = info.get("registry", {}).get("people", {})
    player_of_match = info.get("player_of_match", [])

    # Determine winner
    winner = outcome.get("winner", "")
    win_by_runs = outcome.get("by", {}).get("runs")
    win_by_wickets = outcome.get("by", {}).get("wickets")
    result = outcome.get("result", "")  # "draw", "tie", "no result"

    # --- Phase 1: Collect new nodes, batch CREATE in one query ---

    new_nodes = []

    for team in teams:
        node = _check_team(reg, team)
        if node:
            new_nodes.append(node)

    if venue:
        node = _check_venue(reg, venue, city)
        if node:
            new_nodes.append(node)

    if tournament_name:
        node = _check_tournament(reg, tournament_name)
        if node:
            new_nodes.append(node)

    if season:
        node = _check_season(reg, season)
        if node:
            new_nodes.append(node)

    players_by_team = info.get("players", {})
    for team, player_list in players_by_team.items():
        for pname in player_list:
            pid = registry.get(pname, "")
            if pid:
                node = _check_player(reg, pname, pid)
                if node:
                    new_nodes.append(node)

    # Match node (always new)
    match_props = {
        "file_id": file_id,
        "match_type": match_type,
        "gender": gender,
        "date": date,
        "season": season,
        "winner": winner or result or None,
    }
    if win_by_runs is not None:
        match_props["win_by_runs"] = win_by_runs
    if win_by_wickets is not None:
        match_props["win_by_wickets"] = win_by_wickets

    new_nodes.append(("Match", match_props))

    _batch_create_nodes(client, new_nodes)
    counts["matches"] += 1
    mm = f"file_id: {_q(file_id)}"

    # --- Phase 2: Collect ALL edges, batch CREATE in one query ---

    edges = []

    # Teams COMPETED_IN
    for team in teams:
        edges.append(("Team", f"name: {_q(team)}", "COMPETED_IN", "Match", mm, None))

    # Winner edge
    if winner:
        win_props = {}
        if win_by_runs is not None:
            win_props["by_runs"] = win_by_runs
        if win_by_wickets is not None:
            win_props["by_wickets"] = win_by_wickets
        edges.append(("Team", f"name: {_q(winner)}", "WON", "Match", mm, win_props or None))

    # Toss
    toss_winner = toss.get("winner", "")
    toss_decision = toss.get("decision", "")
    if toss_winner:
        edges.append(("Team", f"name: {_q(toss_winner)}", "WON_TOSS", "Match", mm,
                       {"decision": toss_decision} if toss_decision else None))

    # Venue
    if venue:
        edges.append(("Match", mm, "HOSTED_AT", "Venue", f"name: {_q(venue)}", None))

    # Tournament
    if tournament_name:
        event_props = {}
        mn = event.get("match_number")
        if mn is not None:
            event_props["match_number"] = mn
        grp = event.get("group")
        if grp:
            event_props["group"] = grp
        edges.append(("Match", mm, "PART_OF", "Tournament", f"name: {_q(tournament_name)}",
                       event_props or None))

    # Season
    if season:
        edges.append(("Match", mm, "IN_SEASON", "Season", f"year: {_q(season)}", None))

    # PLAYED_FOR (with dedup via registry)
    for team, player_list in players_by_team.items():
        for pname in player_list:
            pid = registry.get(pname, "")
            if pid:
                key = f"{pid}|{team.lower()}"
                if key not in reg.player_teams:
                    edges.append(("Player", f"cricsheet_id: {_q(pid)}", "PLAYED_FOR",
                                  "Team", f"name: {_q(team)}", None))
                    reg.player_teams.add(key)

    # Player of match
    for pname in player_of_match:
        pid = registry.get(pname, "")
        if pid:
            edges.append(("Player", f"cricsheet_id: {_q(pid)}", "PLAYER_OF_MATCH",
                          "Match", mm, None))

    # Innings batting/bowling/dismissal edges
    dismissals = _collect_innings_edges(edges, innings_data, registry, file_id)

    # Execute all edges in ONE batch query
    if edges:
        _batch_create_edges(client, edges)

    counts["edges"] += len(edges)
    counts["dismissals"] += dismissals


def _collect_innings_edges(edges, innings_data, registry, file_id):
    """Aggregate ball-by-ball stats and append edges to the list. Returns dismissal count."""
    mm = f"file_id: {_q(file_id)}"
    total_dismissals = 0

    for inn_idx, innings in enumerate(innings_data):
        overs = innings.get("overs", [])
        is_super_over = innings.get("super_over", False)

        # Aggregate batting and bowling stats per innings
        batting: dict[str, dict] = defaultdict(lambda: {"runs": 0, "balls": 0, "fours": 0, "sixes": 0})
        bowling: dict[str, dict] = defaultdict(lambda: {"balls": 0, "runs": 0, "wickets": 0, "maidens": 0})
        dismissals: list[dict] = []

        for over_data in overs:
            over_num = over_data.get("over", 0)
            deliveries = over_data.get("deliveries", [])
            bowler_runs_this_over = 0
            bowler_this_over = None

            for dlv in deliveries:
                batter = dlv.get("batter", "")
                bowler = dlv.get("bowler", "")
                runs = dlv.get("runs", {})
                batter_runs = runs.get("batter", 0)
                total_runs = runs.get("total", 0)
                extras = dlv.get("extras", {})
                is_wide = "wides" in extras
                is_noball = "noballs" in extras

                # Batting stats (wides don't count as balls faced)
                if batter:
                    bat = batting[batter]
                    bat["runs"] += batter_runs
                    if not is_wide:
                        bat["balls"] += 1
                    if batter_runs == 4:
                        bat["fours"] += 1
                    elif batter_runs == 6:
                        bat["sixes"] += 1

                # Bowling stats (wides and noballs count as runs but not legal deliveries)
                if bowler:
                    bowl = bowling[bowler]
                    extras_against = sum(extras.values()) if extras else 0
                    bowl["runs"] += batter_runs + extras_against
                    if not is_wide and not is_noball:
                        bowl["balls"] += 1
                    bowler_this_over = bowler
                    bowler_runs_this_over += total_runs

                # Wickets
                for w in dlv.get("wickets", []):
                    player_out = w.get("player_out", "")
                    kind = w.get("kind", "")
                    fielders = w.get("fielders", [])
                    fielder_names = [f.get("name", "") for f in fielders if f.get("name")]

                    if kind in ("bowled", "caught", "caught and bowled", "lbw",
                                "stumped", "hit wicket"):
                        bowling[bowler]["wickets"] += 1

                    dismissals.append({
                        "player_out": player_out,
                        "bowler": bowler,
                        "kind": kind,
                        "fielders": fielder_names,
                        "over": over_num,
                        "innings": inn_idx,
                    })

            # Maiden check
            if bowler_this_over and bowler_runs_this_over == 0:
                bowling[bowler_this_over]["maidens"] += 1

        # BATTED_IN edges
        for batter_name, stats in batting.items():
            pid = registry.get(batter_name, "")
            if not pid or stats["balls"] == 0:
                continue
            sr = round(stats["runs"] * 100.0 / stats["balls"], 2) if stats["balls"] > 0 else 0
            edge_props = {
                "runs": stats["runs"],
                "balls": stats["balls"],
                "fours": stats["fours"],
                "sixes": stats["sixes"],
                "strike_rate": sr,
                "innings_num": inn_idx,
            }
            if is_super_over:
                edge_props["super_over"] = 1
            edges.append(("Player", f"cricsheet_id: {_q(pid)}",
                          "BATTED_IN", "Match", mm, edge_props))

        # BOWLED_IN edges
        for bowler_name, stats in bowling.items():
            pid = registry.get(bowler_name, "")
            if not pid or stats["balls"] == 0:
                continue
            overs_bowled = stats["balls"] // 6 + (stats["balls"] % 6) / 10
            economy = round(stats["runs"] / (stats["balls"] / 6), 2) if stats["balls"] > 0 else 0
            edge_props = {
                "overs": round(overs_bowled, 1),
                "maidens": stats["maidens"],
                "runs_conceded": stats["runs"],
                "wickets": stats["wickets"],
                "economy": economy,
                "innings_num": inn_idx,
            }
            edges.append(("Player", f"cricsheet_id: {_q(pid)}",
                          "BOWLED_IN", "Match", mm, edge_props))

        # DISMISSED edges
        for d in dismissals:
            out_pid = registry.get(d["player_out"], "")
            bowler_pid = registry.get(d["bowler"], "")
            if not out_pid:
                continue

            # Bowler dismissed batsman
            if bowler_pid and d["kind"] in ("bowled", "caught", "caught and bowled",
                                             "lbw", "stumped", "hit wicket"):
                edges.append(("Player", f"cricsheet_id: {_q(bowler_pid)}",
                              "DISMISSED", "Player", f"cricsheet_id: {_q(out_pid)}",
                              {"kind": d["kind"], "over": d["over"],
                               "match_file_id": file_id}))
                total_dismissals += 1

            # Fielder involvement (caught, stumped, run out)
            for fname in d["fielders"]:
                fpid = registry.get(fname, "")
                if fpid and fpid != bowler_pid:
                    edges.append(("Player", f"cricsheet_id: {_q(fpid)}",
                                  "FIELDED_DISMISSAL", "Player", f"cricsheet_id: {_q(out_pid)}",
                                  {"kind": d["kind"], "over": d["over"],
                                   "match_file_id": file_id}))

    return total_dismissals


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def load_cricket(
    client: SamyamaClient,
    data_dir: str = "data/json",
    max_matches: int = 0,
    match_type: str = "",
    gender: str = "",
) -> dict:
    """
    Load cricket matches from Cricsheet JSON files into Samyama.

    Args:
        client: SamyamaClient instance.
        data_dir: Directory containing Cricsheet JSON files.
        max_matches: Max matches to load (0 = all).
        match_type: Filter by match type (Test, ODI, T20, IT20, MDM, ODM).
        gender: Filter by gender (male, female).

    Returns:
        Dict with counts of all created entities.
    """
    # Create indexes for O(1) MATCH lookups (vs O(n) label scan)
    indexes = [
        ("Player", "cricsheet_id"),
        ("Match", "file_id"),
        ("Team", "name"),
        ("Venue", "name"),
        ("Tournament", "name"),
        ("Season", "year"),
    ]
    for label, prop in indexes:
        try:
            client.query(f"CREATE INDEX ON :{label}({prop})", GRAPH)
        except Exception:
            pass  # Index may already exist
    print(f"Created {len(indexes)} indexes", flush=True)

    files = sorted(glob.glob(os.path.join(data_dir, "*.json")))
    print(f"Found {len(files)} JSON files in {data_dir}", flush=True)

    reg = Registry()
    counts = {"matches": 0, "edges": 0, "dismissals": 0, "skipped": 0, "errors": 0}
    t0 = time.time()
    loaded = 0

    for i, fpath in enumerate(files):
        if max_matches > 0 and loaded >= max_matches:
            break

        file_id = Path(fpath).stem

        try:
            with open(fpath) as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as exc:
            print(f"  ERROR reading {fpath}: {exc}", flush=True)
            counts["errors"] += 1
            continue

        info = data.get("info", {})

        # Apply filters
        if match_type and info.get("match_type", "") != match_type:
            counts["skipped"] += 1
            continue
        if gender and info.get("gender", "") != gender:
            counts["skipped"] += 1
            continue

        try:
            _ingest_match(client, data, file_id, reg, counts)
            loaded += 1
        except Exception as exc:
            print(f"  ERROR ingesting {file_id}: {exc}", flush=True)
            counts["errors"] += 1
            continue

        if loaded % 500 == 0:
            elapsed = time.time() - t0
            rate = loaded / elapsed if elapsed > 0 else 0
            print(f"  [{loaded}/{max_matches or len(files)}] "
                  f"{elapsed:.0f}s ({rate:.0f} matches/s) — "
                  f"{len(reg.players)} players, {counts['edges']} edges",
                  flush=True)

    elapsed = time.time() - t0
    counts["players"] = len(reg.players)
    counts["teams"] = len(reg.teams)
    counts["venues"] = len(reg.venues)
    counts["tournaments"] = len(reg.tournaments)
    counts["seasons"] = len(reg.seasons)

    print(f"\n{'='*60}", flush=True)
    print(f"Cricket KG load complete in {elapsed:.1f}s", flush=True)
    print(f"{'='*60}", flush=True)
    for k, v in counts.items():
        print(f"  {k:<17s} {v}", flush=True)
    return counts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Load Cricsheet data into Samyama")
    ap.add_argument("--data-dir", default="data/json", help="Path to JSON files")
    ap.add_argument("--max-matches", type=int, default=0, help="Max matches (0=all)")
    ap.add_argument("--match-type", default="", help="Filter: Test, ODI, T20, IT20")
    ap.add_argument("--gender", default="", help="Filter: male, female")
    ap.add_argument("--url", default=None, help="Samyama server URL (omit for embedded)")
    args = ap.parse_args()

    c = SamyamaClient.connect(args.url) if args.url else SamyamaClient.embedded()
    load_cricket(c, data_dir=args.data_dir, max_matches=args.max_matches,
                 match_type=args.match_type, gender=args.gender)
