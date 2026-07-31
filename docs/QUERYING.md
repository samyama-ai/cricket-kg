# Querying the Cricket KG

Three ways to ask the graph questions, once it's loaded into the `cricket` tenant on a running engine
(see [GETTING_STARTED.md](../GETTING_STARTED.md)). All three below were run live and return the same result.

---

## 1. Claude, over MCP (natural language)

```bash
# register this KG's MCP server with Claude Code (once), pointed at the running engine:
claude mcp add cricket -- python -m mcp_server.server --url http://localhost:8080 --graph cricket

# start a new Claude Code session (MCP servers load at session start), then just ask:
#   "which bowler has dismissed Virat Kohli the most?"   → TG Southee
#   "who are the top run scorers of all time?"
```

*(No engine? `python -m mcp_server.server --data-dir data/json --max-matches 500` loads a small graph
in-memory and serves it — good for a quick local demo.)*

## 2. HTTP API (`POST /api/query`)

```bash
curl -s -X POST http://localhost:8080/api/query -H 'Content-Type: application/json' -d '{
  "graph": "cricket",
  "query": "MATCH (b:Player)-[:DISMISSED]->(bat:Player {name:\"V Kohli\"}) RETURN b.name AS bowler, count(*) AS times ORDER BY times DESC LIMIT 3"
}'
```
```json
{"columns":["bowler","times"],"records":[["TG Southee",12],["MM Ali",11],["JR Hazlewood",11]]}
```

## 3. Samyama CLI (Redis wire protocol, `:6379`)

```bash
redis-cli -p 6379 GRAPH.QUERY cricket \
  "MATCH (b:Player)-[:DISMISSED]->(:Player) RETURN b.name, count(*) AS wickets ORDER BY wickets DESC LIMIT 3"
# 1) "JM Anderson" 1140
# 2) "R Ashwin"    1007
# 3) "SCJ Broad"    983
```

---

## More queries
See **[100-queries.md](100-queries.md)** for 100 example Cypher queries, and the
[schema](../README.md#schema) for the node/edge model.
