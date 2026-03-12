"""Tests for the cricket ETL loader against embedded Samyama."""

import json
import pytest
import tempfile
import os
from pathlib import Path
from samyama import SamyamaClient
from etl.loader import load_cricket


SAMPLE_MATCH = {
    "meta": {"data_version": "1.1.0", "created": "2024-01-01", "revision": 1},
    "info": {
        "teams": ["India", "Australia"],
        "players": {
            "India": ["V Kohli", "R Sharma", "JJ Bumrah", "RA Jadeja"],
            "Australia": ["SPD Smith", "DA Warner", "MA Starc", "PJ Cummins"],
        },
        "registry": {
            "people": {
                "V Kohli": "vk18",
                "R Sharma": "rs45",
                "JJ Bumrah": "jb93",
                "RA Jadeja": "rj08",
                "SPD Smith": "ss49",
                "DA Warner": "dw31",
                "MA Starc": "ms56",
                "PJ Cummins": "pc30",
            }
        },
        "outcome": {"winner": "India", "by": {"runs": 36}},
        "toss": {"winner": "Australia", "decision": "bat"},
        "dates": ["2024-01-15"],
        "gender": "male",
        "match_type": "Test",
        "season": "2024",
        "venue": "Melbourne Cricket Ground",
        "city": "Melbourne",
        "event": {"name": "Border-Gavaskar Trophy", "match_number": 3},
        "player_of_match": ["V Kohli"],
        "balls_per_over": 6,
        "team_type": "international",
    },
    "innings": [
        {
            "team": "Australia",
            "overs": [
                {
                    "over": 0,
                    "deliveries": [
                        {"batter": "DA Warner", "bowler": "JJ Bumrah", "non_striker": "SPD Smith",
                         "runs": {"batter": 4, "extras": 0, "total": 4}},
                        {"batter": "DA Warner", "bowler": "JJ Bumrah", "non_striker": "SPD Smith",
                         "runs": {"batter": 0, "extras": 0, "total": 0}},
                        {"batter": "DA Warner", "bowler": "JJ Bumrah", "non_striker": "SPD Smith",
                         "runs": {"batter": 0, "extras": 0, "total": 0},
                         "wickets": [{"player_out": "DA Warner", "kind": "bowled", "fielders": []}]},
                        {"batter": "MA Starc", "bowler": "JJ Bumrah", "non_striker": "SPD Smith",
                         "runs": {"batter": 0, "extras": 0, "total": 0}},
                        {"batter": "MA Starc", "bowler": "JJ Bumrah", "non_striker": "SPD Smith",
                         "runs": {"batter": 0, "extras": 0, "total": 0}},
                        {"batter": "MA Starc", "bowler": "JJ Bumrah", "non_striker": "SPD Smith",
                         "runs": {"batter": 0, "extras": 0, "total": 0}},
                    ],
                },
                {
                    "over": 1,
                    "deliveries": [
                        {"batter": "SPD Smith", "bowler": "RA Jadeja", "non_striker": "MA Starc",
                         "runs": {"batter": 1, "extras": 0, "total": 1}},
                        {"batter": "MA Starc", "bowler": "RA Jadeja", "non_striker": "SPD Smith",
                         "runs": {"batter": 6, "extras": 0, "total": 6}},
                        {"batter": "MA Starc", "bowler": "RA Jadeja", "non_striker": "SPD Smith",
                         "runs": {"batter": 0, "extras": 0, "total": 0},
                         "wickets": [{"player_out": "MA Starc", "kind": "caught",
                                      "fielders": [{"name": "V Kohli"}]}]},
                        {"batter": "PJ Cummins", "bowler": "RA Jadeja", "non_striker": "SPD Smith",
                         "runs": {"batter": 0, "extras": 0, "total": 0}},
                        {"batter": "PJ Cummins", "bowler": "RA Jadeja", "non_striker": "SPD Smith",
                         "runs": {"batter": 0, "extras": 0, "total": 0}},
                        {"batter": "PJ Cummins", "bowler": "RA Jadeja", "non_striker": "SPD Smith",
                         "runs": {"batter": 0, "extras": 1, "total": 1},
                         "extras": {"legbyes": 1}},
                    ],
                },
            ],
        },
        {
            "team": "India",
            "overs": [
                {
                    "over": 0,
                    "deliveries": [
                        {"batter": "R Sharma", "bowler": "MA Starc", "non_striker": "V Kohli",
                         "runs": {"batter": 4, "extras": 0, "total": 4}},
                        {"batter": "R Sharma", "bowler": "MA Starc", "non_striker": "V Kohli",
                         "runs": {"batter": 1, "extras": 0, "total": 1}},
                        {"batter": "V Kohli", "bowler": "MA Starc", "non_striker": "R Sharma",
                         "runs": {"batter": 4, "extras": 0, "total": 4}},
                        {"batter": "V Kohli", "bowler": "MA Starc", "non_striker": "R Sharma",
                         "runs": {"batter": 6, "extras": 0, "total": 6}},
                        {"batter": "V Kohli", "bowler": "MA Starc", "non_striker": "R Sharma",
                         "runs": {"batter": 0, "extras": 0, "total": 0}},
                        {"batter": "V Kohli", "bowler": "MA Starc", "non_striker": "R Sharma",
                         "runs": {"batter": 1, "extras": 0, "total": 1}},
                    ],
                },
            ],
        },
    ],
}


@pytest.fixture(scope="module")
def loaded_graph():
    """Load a single test match into an embedded graph."""
    c = SamyamaClient.embedded()
    with tempfile.TemporaryDirectory() as tmpdir:
        fpath = os.path.join(tmpdir, "test_match.json")
        with open(fpath, "w") as f:
            json.dump(SAMPLE_MATCH, f)
        stats = load_cricket(c, data_dir=tmpdir, max_matches=1)
    return c, stats


def _q(client, cypher):
    r = client.query_readonly(cypher, "default")
    return [dict(zip(r.columns, row)) for row in r.records]


class TestMatchLoading:
    def test_match_created(self, loaded_graph):
        c, stats = loaded_graph
        assert stats["matches"] == 1

    def test_match_properties(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, 'MATCH (m:Match) RETURN m.file_id, m.match_type, m.gender, m.winner, m.win_by_runs')
        assert len(rows) == 1
        m = rows[0]
        assert m["m.match_type"] == "Test"
        assert m["m.gender"] == "male"
        assert m["m.winner"] == "India"
        assert m["m.win_by_runs"] == 36


class TestTeams:
    def test_teams_created(self, loaded_graph):
        c, stats = loaded_graph
        assert stats["teams"] == 2
        teams = _q(c, "MATCH (t:Team) RETURN t.name ORDER BY t.name")
        names = [r["t.name"] for r in teams]
        assert "Australia" in names
        assert "India" in names

    def test_competed_in(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, "MATCH (t:Team)-[:COMPETED_IN]->(m:Match) RETURN t.name ORDER BY t.name")
        assert len(rows) == 2

    def test_winner_edge(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, "MATCH (t:Team)-[w:WON]->(m:Match) RETURN t.name, w.by_runs")
        assert len(rows) == 1
        assert rows[0]["t.name"] == "India"
        assert rows[0]["w.by_runs"] == 36

    def test_toss_edge(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, "MATCH (t:Team)-[w:WON_TOSS]->(m:Match) RETURN t.name, w.decision")
        assert len(rows) == 1
        assert rows[0]["t.name"] == "Australia"
        assert rows[0]["w.decision"] == "bat"


class TestPlayers:
    def test_players_created(self, loaded_graph):
        c, stats = loaded_graph
        assert stats["players"] == 8

    def test_played_for_edges(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (p:Player)-[:PLAYED_FOR]->(t:Team {name: "India"})
            RETURN p.name ORDER BY p.name
        """)
        names = [r["p.name"] for r in rows]
        assert "V Kohli" in names
        assert "JJ Bumrah" in names
        assert len(names) == 4

    def test_player_of_match(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, "MATCH (p:Player)-[:PLAYER_OF_MATCH]->(m:Match) RETURN p.name")
        assert len(rows) == 1
        assert rows[0]["p.name"] == "V Kohli"


class TestVenue:
    def test_venue_created(self, loaded_graph):
        c, stats = loaded_graph
        assert stats["venues"] == 1
        rows = _q(c, "MATCH (v:Venue) RETURN v.name, v.city")
        assert rows[0]["v.name"] == "Melbourne Cricket Ground"
        assert rows[0]["v.city"] == "Melbourne"

    def test_hosted_at_edge(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, "MATCH (m:Match)-[:HOSTED_AT]->(v:Venue) RETURN v.name")
        assert len(rows) == 1


class TestTournament:
    def test_tournament_created(self, loaded_graph):
        c, stats = loaded_graph
        assert stats["tournaments"] == 1
        rows = _q(c, "MATCH (t:Tournament) RETURN t.name")
        assert rows[0]["t.name"] == "Border-Gavaskar Trophy"

    def test_part_of_edge(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, "MATCH (m:Match)-[p:PART_OF]->(t:Tournament) RETURN t.name, p.match_number")
        assert len(rows) == 1
        assert rows[0]["p.match_number"] == 3


class TestBatting:
    def test_batted_in_edges(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (p:Player)-[b:BATTED_IN]->(m:Match)
            RETURN p.name, b.runs, b.balls, b.fours, b.sixes
            ORDER BY b.runs DESC
        """)
        assert len(rows) >= 4  # At least 4 batsmen across 2 innings

        # Kohli: 4+6+0+1 = 11 runs, 4 balls
        kohli = [r for r in rows if r["p.name"] == "V Kohli"]
        assert len(kohli) == 1
        assert kohli[0]["b.runs"] == 11
        assert kohli[0]["b.balls"] == 4
        assert kohli[0]["b.fours"] == 1
        assert kohli[0]["b.sixes"] == 1

    def test_warner_batting(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (p:Player {name: "DA Warner"})-[b:BATTED_IN]->(m:Match)
            RETURN b.runs, b.balls
        """)
        assert len(rows) == 1
        assert rows[0]["b.runs"] == 4  # 4+0+0 (got out third ball)
        assert rows[0]["b.balls"] == 3


class TestBowling:
    def test_bowled_in_edges(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (p:Player)-[b:BOWLED_IN]->(m:Match)
            RETURN p.name, b.wickets, b.runs_conceded, b.economy
            ORDER BY b.wickets DESC
        """)
        assert len(rows) >= 2  # At least Bumrah, Jadeja, Starc

    def test_bumrah_bowling(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (p:Player {name: "JJ Bumrah"})-[b:BOWLED_IN]->(m:Match)
            RETURN b.wickets, b.runs_conceded, b.maidens
        """)
        assert len(rows) == 1
        assert rows[0]["b.wickets"] == 1  # Warner bowled
        assert rows[0]["b.runs_conceded"] == 4  # Warner's 4
        assert rows[0]["b.maidens"] == 0  # 4 runs in the over


class TestDismissals:
    def test_dismissal_count(self, loaded_graph):
        c, stats = loaded_graph
        assert stats["dismissals"] == 2  # Warner bowled, Starc caught

    def test_bumrah_dismissed_warner(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
            WHERE bowler.name = "JJ Bumrah" AND batsman.name = "DA Warner"
            RETURN d.kind
        """)
        assert len(rows) == 1
        assert rows[0]["d.kind"] == "bowled"

    def test_jadeja_dismissed_starc(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
            WHERE bowler.name = "RA Jadeja" AND batsman.name = "MA Starc"
            RETURN d.kind
        """)
        assert len(rows) == 1
        assert rows[0]["d.kind"] == "caught"

    def test_fielder_edge(self, loaded_graph):
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (fielder:Player)-[d:FIELDED_DISMISSAL]->(batsman:Player)
            RETURN fielder.name, batsman.name, d.kind
        """)
        assert len(rows) == 1
        assert rows[0]["fielder.name"] == "V Kohli"
        assert rows[0]["batsman.name"] == "MA Starc"
        assert rows[0]["d.kind"] == "caught"


class TestMultiHopQueries:
    def test_dismissal_network(self, loaded_graph):
        """Multi-hop: who dismissed players who played for Australia?"""
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (bowler:Player)-[:DISMISSED]->(batsman:Player)-[:PLAYED_FOR]->(t:Team {name: "Australia"})
            RETURN bowler.name, batsman.name
            ORDER BY bowler.name
        """)
        assert len(rows) == 2
        bowlers = {r["bowler.name"] for r in rows}
        assert "JJ Bumrah" in bowlers
        assert "RA Jadeja" in bowlers

    def test_tournament_players(self, loaded_graph):
        """Multi-hop: players in Border-Gavaskar Trophy matches."""
        c, _ = loaded_graph
        rows = _q(c, """
            MATCH (p:Player)-[:BATTED_IN]->(m:Match)-[:PART_OF]->(t:Tournament {name: "Border-Gavaskar Trophy"})
            RETURN DISTINCT p.name
            ORDER BY p.name
        """)
        assert len(rows) >= 4
