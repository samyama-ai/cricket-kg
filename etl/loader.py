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
from pathlib import Path
from collections import defaultdict
from samyama import SamyamaClient

GRAPH = "default"


# ---------------------------------------------------------------------------
# Cypher helpers (match clinicaltrials-kg patterns)
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


def _merge_node(client, label, props):
    client.query(f"MERGE (n:{label} {_prop_str(props)})", GRAPH)


def _create_edge(client, src_label, src_match, rel, tgt_label, tgt_match, props=None):
    prop_part = f" {_prop_str(props)}" if props else ""
    q = (f"MATCH (a:{src_label} {{{src_match}}}), (b:{tgt_label} {{{tgt_match}}}) "
         f"CREATE (a)-[:{rel}{prop_part}]->(b)")
    client.query(q, GRAPH)


# ---------------------------------------------------------------------------
# Node creation (with dedup tracking)
# ---------------------------------------------------------------------------

class Registry:
    """Tracks created nodes to avoid redundant MERGEs."""
    def __init__(self):
        self.players: set[str] = set()       # cricsheet_id
        self.teams: set[str] = set()         # name.lower()
        self.venues: set[str] = set()        # name.lower()
        self.tournaments: set[str] = set()   # name.lower()
        self.seasons: set[str] = set()       # year string
        self.player_teams: set[str] = set()  # "player_id|team" edge dedup


def _ensure_player(client, reg, name, cricsheet_id):
    if cricsheet_id in reg.players:
        return
    _merge_node(client, "Player", {"cricsheet_id": cricsheet_id, "name": name})
    reg.players.add(cricsheet_id)


def _ensure_team(client, reg, name):
    key = name.lower()
    if key in reg.teams:
        return
    _merge_node(client, "Team", {"name": name})
    reg.teams.add(key)


def _ensure_venue(client, reg, venue, city=None):
    key = venue.lower()
    if key in reg.venues:
        return
    props = {"name": venue}
    if city:
        props["city"] = city
    _merge_node(client, "Venue", props)
    reg.venues.add(key)


def _ensure_tournament(client, reg, name):
    key = name.lower()
    if key in reg.tournaments:
        return
    _merge_node(client, "Tournament", {"name": name})
    reg.tournaments.add(key)


def _ensure_season(client, reg, season):
    key = str(season)
    if key in reg.seasons:
        return
    _merge_node(client, "Season", {"year": season})
    reg.seasons.add(key)


def _ensure_played_for(client, reg, cricsheet_id, team_name):
    key = f"{cricsheet_id}|{team_name.lower()}"
    if key in reg.player_teams:
        return
    _create_edge(client, "Player", f"cricsheet_id: {_q(cricsheet_id)}",
                 "PLAYED_FOR", "Team", f"name: {_q(team_name)}")
    reg.player_teams.add(key)


# ---------------------------------------------------------------------------
# Match ingestion
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

    # Match node
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

    _merge_node(client, "Match", match_props)
    counts["matches"] += 1
    mm = f"file_id: {_q(file_id)}"

    # Teams
    for team in teams:
        _ensure_team(client, reg, team)
        _create_edge(client, "Team", f"name: {_q(team)}", "COMPETED_IN", "Match", mm)
        counts["edges"] += 1

    # Winner edge
    if winner:
        win_props = {}
        if win_by_runs is not None:
            win_props["by_runs"] = win_by_runs
        if win_by_wickets is not None:
            win_props["by_wickets"] = win_by_wickets
        _create_edge(client, "Team", f"name: {_q(winner)}", "WON", "Match", mm, win_props or None)
        counts["edges"] += 1

    # Toss
    toss_winner = toss.get("winner", "")
    toss_decision = toss.get("decision", "")
    if toss_winner:
        _create_edge(client, "Team", f"name: {_q(toss_winner)}", "WON_TOSS", "Match", mm,
                     {"decision": toss_decision} if toss_decision else None)
        counts["edges"] += 1

    # Venue
    if venue:
        _ensure_venue(client, reg, venue, city)
        _create_edge(client, "Match", mm, "HOSTED_AT", "Venue", f"name: {_q(venue)}")
        counts["edges"] += 1

    # Tournament
    if tournament_name:
        _ensure_tournament(client, reg, tournament_name)
        event_props = {}
        mn = event.get("match_number")
        if mn is not None:
            event_props["match_number"] = mn
        grp = event.get("group")
        if grp:
            event_props["group"] = grp
        _create_edge(client, "Match", mm, "PART_OF", "Tournament", f"name: {_q(tournament_name)}",
                     event_props or None)
        counts["edges"] += 1

    # Season
    if season:
        _ensure_season(client, reg, season)
        _create_edge(client, "Match", mm, "IN_SEASON", "Season", f"year: {_q(season)}")
        counts["edges"] += 1

    # Players — register all and link to teams
    players_by_team = info.get("players", {})
    for team, player_list in players_by_team.items():
        for pname in player_list:
            pid = registry.get(pname, "")
            if pid:
                _ensure_player(client, reg, pname, pid)
                _ensure_played_for(client, reg, pid, team)

    # Player of match
    for pname in player_of_match:
        pid = registry.get(pname, "")
        if pid:
            _create_edge(client, "Player", f"cricsheet_id: {_q(pid)}",
                         "PLAYER_OF_MATCH", "Match", mm)
            counts["edges"] += 1

    # Process innings for batting/bowling stats and dismissals
    _process_innings(client, innings_data, registry, file_id, reg, counts)


def _process_innings(client, innings_data, registry, file_id, reg, counts):
    mm = f"file_id: {_q(file_id)}"

    for inn_idx, innings in enumerate(innings_data):
        team = innings.get("team", "")
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

        # Create BATTED_IN edges with aggregated stats
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
            _create_edge(client, "Player", f"cricsheet_id: {_q(pid)}",
                         "BATTED_IN", "Match", mm, edge_props)
            counts["edges"] += 1

        # Create BOWLED_IN edges with aggregated stats
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
            _create_edge(client, "Player", f"cricsheet_id: {_q(pid)}",
                         "BOWLED_IN", "Match", mm, edge_props)
            counts["edges"] += 1

        # Create DISMISSED edges
        for d in dismissals:
            out_pid = registry.get(d["player_out"], "")
            bowler_pid = registry.get(d["bowler"], "")
            if not out_pid:
                continue

            # Bowler dismissed batsman
            if bowler_pid and d["kind"] in ("bowled", "caught", "caught and bowled",
                                             "lbw", "stumped", "hit wicket"):
                _create_edge(client, "Player", f"cricsheet_id: {_q(bowler_pid)}",
                             "DISMISSED", "Player", f"cricsheet_id: {_q(out_pid)}",
                             {"kind": d["kind"], "over": d["over"],
                              "match_file_id": file_id})
                counts["dismissals"] += 1
                counts["edges"] += 1

            # Fielder involvement (caught, stumped, run out)
            for fname in d["fielders"]:
                fpid = registry.get(fname, "")
                if fpid and fpid != bowler_pid:
                    _create_edge(client, "Player", f"cricsheet_id: {_q(fpid)}",
                                 "FIELDED_DISMISSAL", "Player", f"cricsheet_id: {_q(out_pid)}",
                                 {"kind": d["kind"], "over": d["over"],
                                  "match_file_id": file_id})
                    counts["edges"] += 1


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
    files = sorted(glob.glob(os.path.join(data_dir, "*.json")))
    print(f"Found {len(files)} JSON files in {data_dir}")

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
            print(f"  ERROR reading {fpath}: {exc}")
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
            print(f"  ERROR ingesting {file_id}: {exc}")
            counts["errors"] += 1
            continue

        if loaded % 100 == 0:
            elapsed = time.time() - t0
            print(f"  [{loaded}/{max_matches or len(files)}] "
                  f"{elapsed:.0f}s — {len(reg.players)} players, "
                  f"{counts['matches']} matches, {counts['edges']} edges")

    elapsed = time.time() - t0
    counts["players"] = len(reg.players)
    counts["teams"] = len(reg.teams)
    counts["venues"] = len(reg.venues)
    counts["tournaments"] = len(reg.tournaments)
    counts["seasons"] = len(reg.seasons)

    print(f"\n{'='*60}")
    print(f"Cricket KG load complete in {elapsed:.1f}s")
    print(f"{'='*60}")
    for k, v in counts.items():
        print(f"  {k:<17s} {v}")
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
