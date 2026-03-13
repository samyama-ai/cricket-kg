"""Cricket KG MCP Server — auto-generated via samyama-mcp-serve.

Usage:
    # Embedded mode (loads demo data on startup):
    python -m mcp_server.server --max-matches 100

    # Connect to running Samyama server with pre-loaded data:
    python -m mcp_server.server --url http://localhost:8080

    # List all auto-generated + custom tools:
    python -m mcp_server.server --max-matches 50 --list-tools

    # Claude Desktop config (embedded with 100 matches):
    # {"mcpServers": {"cricket-kg": {
    #     "command": "python", "args": ["-m", "mcp_server.server", "--max-matches", "100"]}}}
"""

from __future__ import annotations

import argparse
import os
import sys


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="cricket-kg-mcp",
        description="Cricket Knowledge Graph MCP Server (powered by samyama-mcp-serve)",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Connect to a running Samyama server (skip embedded loading).",
    )
    parser.add_argument(
        "--max-matches",
        type=int,
        default=100,
        help="Number of matches to load in embedded mode (default: 100, 0 = all).",
    )
    parser.add_argument(
        "--data-dir",
        default="data/json",
        help="Path to Cricsheet JSON files (default: data/json).",
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="Print discovered tools and exit.",
    )
    parser.add_argument(
        "--name",
        default="Cricket KG",
        help="MCP server name.",
    )

    args = parser.parse_args(argv)

    from samyama import SamyamaClient

    if args.url:
        client = SamyamaClient.connect(args.url)
    else:
        client = SamyamaClient.embedded()
        _load_data(client, args.data_dir, args.max_matches)

    # Resolve config path relative to this file
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

    from samyama_mcp.config import ToolConfig
    from samyama_mcp.server import SamyamaMCPServer

    config = ToolConfig.from_yaml(config_path)
    server = SamyamaMCPServer(
        client, server_name=args.name, config=config
    )

    if args.list_tools:
        tools = server.list_tools()
        print(f"Cricket KG: {len(tools)} tools\n")
        for name in sorted(tools):
            print(f"  - {name}")
        sys.exit(0)

    server.run()


def _load_data(client, data_dir: str, max_matches: int) -> None:
    """Load cricket data from Cricsheet JSON files."""
    if not os.path.isdir(data_dir):
        print(
            f"Warning: data directory '{data_dir}' not found. "
            f"Starting with empty graph.",
            file=sys.stderr,
        )
        return

    from etl.loader import load_cricket

    stats = load_cricket(client, data_dir=data_dir, max_matches=max_matches)
    print(
        f"Loaded {stats.get('matches_loaded', 0)} matches "
        f"({stats.get('nodes', 0)} nodes, {stats.get('edges', 0)} edges)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
