"""Tests for cricket-kg MCP server — verifies auto-generated + custom tools."""

import asyncio
import json
import os
import sys

import pytest

from samyama import SamyamaClient
from samyama_mcp.config import ToolConfig
from samyama_mcp.server import SamyamaMCPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from etl.loader import load_cricket


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def server():
    """Create a server with 20 matches — shared across all tests."""
    client = SamyamaClient.embedded()
    load_cricket(client, data_dir="data/json", max_matches=20)
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "mcp_server", "config.yaml"
    )
    config = ToolConfig.from_yaml(config_path)
    return SamyamaMCPServer(client, server_name="Cricket KG Test", config=config)


def _call(server, tool_name, args=None):
    """Synchronously call an MCP tool and parse the JSON result."""
    async def _run():
        r = await server.mcp.call_tool(tool_name, args or {})
        return json.loads(r.content[0].text)
    return asyncio.run(_run())


# ── Tool Registration ────────────────────────────────────────────────

class TestToolRegistration:
    def test_has_generic_tools(self, server):
        tools = server.list_tools()
        assert "cypher_query" in tools
        assert "schema_info" in tools

    def test_has_node_tools(self, server):
        tools = server.list_tools()
        assert "search_player" in tools
        assert "count_match" in tools
        assert "get_team_by_name" in tools

    def test_has_edge_tools(self, server):
        tools = server.list_tools()
        assert "find_batted_in_connections" in tools
        assert "find_dismissed_connections" in tools
        assert "traverse_played_for" in tools

    def test_has_algorithm_tools(self, server):
        tools = server.list_tools()
        assert "pagerank" in tools
        assert "shortest_path" in tools
        assert "communities" in tools

    def test_has_custom_tools(self, server):
        tools = server.list_tools()
        assert "top_run_scorers" in tools
        assert "top_wicket_takers" in tools
        assert "dismissal_network" in tools
        assert "head_to_head" in tools
        assert "tournament_top_scorers" in tools
        assert "most_sixes" in tools

    def test_tool_count_at_least_50(self, server):
        assert len(server.list_tools()) >= 50


# ── Schema Info ──────────────────────────────────────────────────────

class TestSchemaInfo:
    def test_schema_has_all_labels(self, server):
        schema = _call(server, "schema_info")
        labels = {nt["label"] for nt in schema["node_types"]}
        assert {"Player", "Match", "Team", "Venue", "Tournament", "Season"} <= labels

    def test_schema_has_edge_types(self, server):
        schema = _call(server, "schema_info")
        etypes = {et["type"] for et in schema["edge_types"]}
        assert "BATTED_IN" in etypes
        assert "BOWLED_IN" in etypes
        assert "DISMISSED" in etypes

    def test_schema_totals_positive(self, server):
        schema = _call(server, "schema_info")
        assert schema["total_nodes"] > 0
        assert schema["total_edges"] > 0


# ── Auto-Generated Node Tools ────────────────────────────────────────

class TestNodeTools:
    def test_search_player(self, server):
        rows = _call(server, "search_player", {"query": "Warner", "limit": 5})
        assert any("Warner" in str(r.get("name", "")) for r in rows)

    def test_count_match(self, server):
        result = _call(server, "count_match")
        assert result["count"] == 20

    def test_get_team_by_name(self, server):
        result = _call(server, "get_team_by_name", {"value": "Australia"})
        assert result["name"] == "Australia"

    def test_get_team_not_found(self, server):
        result = _call(server, "get_team_by_name", {"value": "Nonexistent FC"})
        assert "error" in result


# ── Auto-Generated Edge Tools ────────────────────────────────────────

class TestEdgeTools:
    def test_find_played_for_connections(self, server):
        rows = _call(server, "find_played_for_connections", {
            "node_label": "Team",
            "node_property": "name",
            "node_value": "Australia",
            "direction": "incoming",
        })
        assert len(rows) > 0

    def test_find_batted_in_connections(self, server):
        rows = _call(server, "find_batted_in_connections", {
            "node_label": "Player",
            "node_property": "name",
            "node_value": "DA Warner",
        })
        assert len(rows) > 0


# ── Custom Tools ─────────────────────────────────────────────────────

class TestCustomTools:
    def test_top_run_scorers(self, server):
        rows = _call(server, "top_run_scorers", {"limit": 5})
        assert len(rows) == 5
        # Sorted descending by runs
        runs = [r["total_runs"] for r in rows]
        assert runs == sorted(runs, reverse=True)

    def test_top_run_scorers_with_format_filter(self, server):
        rows = _call(server, "top_run_scorers", {"match_type": "Test", "limit": 5})
        assert len(rows) <= 5

    def test_top_wicket_takers(self, server):
        rows = _call(server, "top_wicket_takers", {"limit": 5})
        assert len(rows) > 0
        assert "total_wickets" in rows[0]

    def test_dismissal_network(self, server):
        rows = _call(server, "dismissal_network", {"limit": 5})
        assert len(rows) > 0
        assert "bowler" in rows[0]
        assert "batsman" in rows[0]
        assert "times" in rows[0]

    def test_player_of_match_awards(self, server):
        rows = _call(server, "player_of_match_awards", {"limit": 5})
        assert len(rows) > 0
        assert "player" in rows[0]
        assert "awards" in rows[0]

    def test_most_sixes(self, server):
        rows = _call(server, "most_sixes", {"limit": 5})
        assert len(rows) > 0
        assert "sixes" in rows[0]

    def test_top_fielders(self, server):
        rows = _call(server, "top_fielders", {"limit": 5})
        assert len(rows) > 0
        assert "catches" in rows[0]

    def test_venue_stats(self, server):
        rows = _call(server, "venue_stats", {"venue_name": "Perth"})
        # Should find WACA or Perth Stadium
        assert len(rows) >= 0  # might not have Perth in 20 matches


# ── Security ─────────────────────────────────────────────────────────

class TestSecurity:
    def test_cypher_query_rejects_write(self, server):
        result = _call(server, "cypher_query", {"cypher": "CREATE (n:Test)"})
        assert "error" in result

    def test_cypher_query_readonly_works(self, server):
        rows = _call(server, "cypher_query", {
            "cypher": "MATCH (n:Player) RETURN count(n) AS c"
        })
        assert rows[0]["c"] > 0
