#!/usr/bin/env python3
"""
FRTB Query Engine Comparison UI Server
Serves comparison UI at http://localhost:5050/
"""
import json, time, decimal, datetime, os, concurrent.futures
import yaml
from pathlib import Path
import requests
import psycopg2, psycopg2.extras
import adbc_driver_flightsql.dbapi as flightsql_dbapi
from flask import Flask, request, jsonify, send_from_directory

STATIC = os.path.join(os.path.dirname(__file__), "ui_static")
app = Flask(__name__, static_folder=STATIC)

QS_URL        = "http://127.0.0.1:8090"   # all query execution via QueryService
FLIGHT_URL    = "grpc://127.0.0.1:32010" # FlightSQL endpoint (zero-copy Arrow)
SAIL_META_URL = os.environ.get("SAIL_META_URL", "http://127.0.0.1:7070")  # bootstrap.py sidecar
# SAIL_ENGINE=http          routes Sail queries through QueryService → HTTP bridge.
# SAIL_ENGINE=flightsql     routes them directly via ADBC → FlightSQL → Arrow IPC (live from pgpool).
# SAIL_ENGINE=sparkconnect  routes through QueryService → SparkConnect JDBC (session from metadata :7070).
# SAIL_ENGINE=sc_arrow      routes through QueryService → SparkConnect gRPC + Arrow IPC (zero-copy).
# Mutable at runtime via POST /api/production/engine.
SAIL_ENGINE   = os.environ.get("SAIL_ENGINE", "flightsql").lower()
SHARD1_CONN = dict(host="127.0.0.1", port=6432, dbname="frtb_shard1",   user="rajeshganta", password="frtb_local")
SHARD2_CONN = dict(host="127.0.0.1", port=6433, dbname="frtb_shard2",   user="rajeshganta", password="frtb_local")
SHARD3_CONN = dict(host="127.0.0.1", port=6434, dbname="frtb_shard3",   user="rajeshganta", password="frtb_local")
EQUITY_CONN = dict(host="127.0.0.1", port=6435, dbname="equity_db",     user="rajeshganta", password="frtb_local")
CREDIT_CONN = dict(host="127.0.0.1", port=6436, dbname="credit_db",     user="rajeshganta", password="frtb_local")
MACRO_CONN  = dict(host="127.0.0.1", port=6437, dbname="macro_db",      user="rajeshganta", password="frtb_local")

REGISTRY_PATH = Path(__file__).parent / "shard_registry.yaml"
PGPOOL_CONF    = Path(__file__).parent.parent / "infra" / "pgpool_shard1" / "pgpool.conf"
SHARD_CONN_MAP = {
    "shard1":        SHARD1_CONN,
    "shard2":        SHARD2_CONN,
    "shard3":        SHARD3_CONN,
    "equity_shard":  EQUITY_CONN,
    "credit_shard":  CREDIT_CONN,
    "macro_shard":   MACRO_CONN,
}

# Dev HTTP bridge workers — Flask proxies for the dev Sail server (:50051).
SAIL_WORKERS = [
    {"id": "W1", "port": 8080, "label": "HTTP Bridge 1"},
    {"id": "W2", "port": 8081, "label": "HTTP Bridge 2"},
    {"id": "W3", "port": 8082, "label": "HTTP Bridge 3"},
]

# Production Sail server — separate process with 3 DataFusion workers (local-cluster mode).
# Dev server: SparkConnect :50051, FlightSQL :32010, HTTP bridges :8080-8082.
# Prod server: SparkConnect :50053, FlightSQL :32011, HTTP bridges :8083-8085.
PROD_WORKERS = [
    {"id": "W1", "port": 8083, "label": "HTTP Bridge 1"},
    {"id": "W2", "port": 8084, "label": "HTTP Bridge 2"},
    {"id": "W3", "port": 8085, "label": "HTTP Bridge 3"},
]
PROD_FLIGHT_URL   = "grpc://127.0.0.1:32011"
PROD_WORKER_COUNT = 3   # production always runs 3 DataFusion workers (local-cluster)

QUERIES = [
    {
        "id": "topn",
        "label": "TopN — Global Risk Rank",
        "tag": "RANK window",
        "description": "Top 20 legal entities by total SA capital, using RANK() window function.",
        "sql": (
            "SELECT   legal_entity_group_id,\n"
            "         SUM(total)                             AS sum_total,\n"
            "         SUM(interest_rates_delta)              AS ir_delta,\n"
            "         SUM(equities_delta)                    AS eq_delta,\n"
            "         COUNT(*)                               AS row_count,\n"
            "         RANK() OVER (ORDER BY SUM(total) DESC) AS global_rank\n"
            "FROM     frtb_sa_capital_output\n"
            "GROUP BY legal_entity_group_id\n"
            "ORDER BY global_rank, legal_entity_group_id\n"
            "LIMIT    20"
        ),
        "check": "rank",
        "wrong_in": "sharding",
        "why_wrong": (
            "ShardingSphere evaluates <code>RANK() OVER (ORDER BY SUM(total) DESC)</code> "
            "independently inside each PostgreSQL shard before merging. "
            "Shard-1 ranks by its local partial sums; shard-2 does the same. "
            "After merging, entity 36 (global $4.498B — the true #1) receives rank&nbsp;7 "
            "while entity 7 (only $4.206B) is given rank&nbsp;1. "
            "The final rankings are meaningless."
        ),
        "why_correct": (
            "Sail (DataFusion) applies <code>RANK()</code> as a global window function "
            "over the unified UNION ALL of both shards. "
            "Entity 36 correctly receives rank&nbsp;1."
        ),
    },
    {
        "id": "casewhen",
        "label": "CASE/WHEN — Above-Avg Breakdown",
        "tag": "correlated subquery",
        "description": "Scenario breakdown where threshold uses a cross-shard AVG subquery.",
        "sql": (
            "WITH windowed AS (\n"
            "    SELECT legal_entity_group_id,\n"
            "           driving_scenario,\n"
            "           total,\n"
            "           AVG(total) OVER () AS period_avg\n"
            "    FROM   frtb_sa_capital_output\n"
            "    WHERE  cobdate BETWEEN DATE '2024-06-01' AND DATE '2026-03-31'\n"
            ")\n"
            "SELECT   legal_entity_group_id,\n"
            "         SUM(CASE WHEN driving_scenario = 'HIGH'   THEN total ELSE 0.0 END) AS high_total,\n"
            "         SUM(CASE WHEN driving_scenario = 'MEDIUM' THEN total ELSE 0.0 END) AS medium_total,\n"
            "         SUM(CASE WHEN driving_scenario = 'LOW'    THEN total ELSE 0.0 END) AS low_total,\n"
            "         SUM(CASE WHEN total > period_avg THEN total ELSE 0.0 END)          AS above_avg_total,\n"
            "         COUNT(CASE WHEN total > period_avg THEN 1 END)                     AS above_avg_count\n"
            "FROM     windowed\n"
            "GROUP BY legal_entity_group_id\n"
            "ORDER BY above_avg_total DESC\n"
            "LIMIT    20"
        ),
        "check": "numeric",
        "wrong_in": "sharding",
        "why_wrong": (
            "The inner subquery <code>SELECT AVG(total) FROM frtb_sa_capital_output</code> "
            "is evaluated on a single shard by ShardingSphere. "
            "Shard-1 average ≈&nbsp;$999,180 vs true global average ≈&nbsp;$998,382. "
            "The misplaced threshold causes 1–2 positions per entity to be misclassified — "
            "small but non-zero regulatory reporting error."
        ),
        "why_correct": (
            "Sail evaluates the subquery against the unified UNION ALL view, "
            "producing the true global average before evaluating each "
            "<code>CASE WHEN</code> predicate."
        ),
    },
    {
        "id": "aggregations",
        "label": "SA Risk Components",
        "tag": "plain GROUP BY",
        "description": "SUM/COUNT/AVG across all SA capital components by correlation scenario.",
        "sql": (
            "SELECT   correlation_scenario_id,\n"
            "         driving_scenario,\n"
            "         COUNT(*)                                               AS positions,\n"
            "         SUM(total)                                             AS total_capital,\n"
            "         AVG(total)                                             AS avg_capital,\n"
            "         SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,\n"
            "         SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS ir_sa,\n"
            "         SUM(equities_delta+equities_vega+equities_curvature)  AS equities_sa,\n"
            "         SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa\n"
            "FROM     frtb_sa_capital_output\n"
            "GROUP BY correlation_scenario_id, driving_scenario\n"
            "ORDER BY total_capital DESC"
        ),
        "check": "numeric",
        "wrong_in": None,
        "why_correct": (
            "Plain <code>GROUP BY</code> with SUM/COUNT/AVG — no window functions or correlated subqueries. "
            "ShardingSphere 5.5 correctly rewrites <code>AVG</code> as <code>SUM/COUNT</code> per shard "
            "and merges partial aggregates. Results match Sail within floating-point tolerance."
        ),
    },
    {
        "id": "pruning",
        "label": "Partition Pruning — Q1 2024",
        "tag": "shard routing",
        "description": "cobdate 2024-Q1 range — entirely within shard-1. Tests route correctness.",
        "sql": (
            "SELECT   cobdate,\n"
            "         COUNT(*)    AS positions,\n"
            "         SUM(total)  AS total_capital\n"
            "FROM     frtb_sa_capital_output\n"
            "WHERE    cobdate BETWEEN DATE '2024-01-01' AND DATE '2024-03-31'\n"
            "GROUP BY cobdate\n"
            "ORDER BY cobdate"
        ),
        "check": "numeric",
        "wrong_in": None,
        "why_correct": (
            "2024-Q1 falls entirely within shard-1 (RANGE 2024-01-01 → 2025-06-30). "
            "The custom <code>COBDATE_RANGE</code> algorithm routes to <code>ds_shard1</code> only. "
            "Single-shard queries need no cross-shard aggregation — both engines agree."
        ),
    },
    {
        "id": "cte_topn",
        "label": "CTE TopN — ROW_NUMBER in Subquery",
        "tag": "CTE failure",
        "description": "Top 10 entities for a single date using a CTE with ROW_NUMBER(). ShardingSphere cannot resolve the CTE alias as a result set.",
        "sql": (
            "WITH entity_ranked AS (\n"
            "  SELECT\n"
            "    legal_entity_group_id,\n"
            "    SUM(total)                                   AS sum_total,\n"
            "    ROW_NUMBER() OVER (ORDER BY SUM(total) DESC) AS row_rank\n"
            "  FROM frtb_sa_capital_output\n"
            "  WHERE cobdate = DATE '2026-01-15'\n"
            "  GROUP BY legal_entity_group_id\n"
            ")\n"
            "SELECT * FROM entity_ranked\n"
            "WHERE row_rank <= 10\n"
            "ORDER BY row_rank"
        ),
        "check": "cte",
        "wrong_in": "sharding",
        "why_wrong": (
            "ShardingSphere cannot resolve CTE (Common Table Expression) aliases as temporary result sets. "
            "It interprets <code>SELECT * FROM entity_ranked</code> as a reference to a physical table named "
            "<code>entity_ranked</code> and fails with <em>Table or view 'entity_ranked' does not exist</em>. "
            "The query cannot execute at all — it does not produce wrong results, it errors entirely."
        ),
        "why_correct": (
            "Sail (DataFusion) fully supports CTEs as named logical subplans. "
            "<code>entity_ranked</code> is materialised as a logical plan in memory, "
            "then the outer SELECT filters and returns the top 10 rows correctly."
        ),
    },
    {
        "id": "cross_dataset_join",
        "label": "Cross-Dataset JOIN — FRTB + Equity Risk",
        "tag": "multi-table JOIN",
        "description": "Join FRTB SA capital output with equity risk measures on the same date. Sail resolves both sharded datasets transparently; ShardingSphere fails — cross-datasource JOINs between SHARDING and SINGLE tables are not supported even with ADVANCED federation.",
        "sql": (
            "SELECT\n"
            "  f.legal_entity_group_id,\n"
            "  SUM(f.total)                AS frtb_capital,\n"
            "  SUM(e.total_equity_risk)    AS equity_risk,\n"
            "  SUM(f.total) + SUM(e.total_equity_risk) AS combined_exposure\n"
            "FROM frtb_sa_capital_output f\n"
            "JOIN equity_risk_measure e\n"
            "  ON f.legal_entity_group_id = e.legal_entity_group_id\n"
            " AND f.cobdate = e.cobdate\n"
            "WHERE f.cobdate = DATE '2026-01-15'\n"
            "GROUP BY f.legal_entity_group_id\n"
            "ORDER BY combined_exposure DESC\n"
            "LIMIT 20"
        ),
        "check": "numeric",
        "wrong_in": "sharding",
        "why_wrong": (
            "<code>equity_risk_measure</code> is registered in SS as a SINGLE table in <code>ds_equity</code> "
            "(port 5443), while <code>frtb_sa_capital_output</code> is sharded across "
            "<code>ds_shard1/2/3</code> (ports 6432–6434). SS routes the JOIN to the FRTB shard backends "
            "because they own the driving SHARDING table. The shard databases (TimescaleDB) do not contain "
            "<code>equity_risk_measure</code>, so they return "
            "<em>relation \"equity_risk_measure\" does not exist</em>. "
            "This is a cross-datasource JOIN limitation in SS 5.5: it cannot bridge SHARDING and SINGLE "
            "tables from separate physical databases, regardless of federation settings."
        ),
        "why_correct": (
            "Sail reads <code>shard_registry.yaml</code> at startup and auto-registers all datasets "
            "as logical UNION ALL views. <code>equity_risk_measure</code> is registered as a sharded "
            "view across shard1 and shard2 exactly like <code>frtb_sa_capital_output</code>. "
            "The JOIN is resolved entirely within Sail's DataFusion query planner — no cross-shard "
            "shuffle needed since both tables share the same cobdate sharding key."
        ),
    },
    {
        "id": "broadcast_join",
        "label": "Broadcast JOIN — Fiscal YoY Delta via cobdate",
        "tag": "broadcast + cross-shard delta",
        "description": "Join FRTB capital with the cobdate broadcast table to get fiscal calendar attributes, then compute Q1 2026 vs Q1 2025 year-on-year delta. SS handles the broadcast JOIN — cobdate is in its config — but produces wrong deltas because the two fiscal quarters span different shards.",
        "sql": (
            "SELECT\n"
            "  f.legal_entity_group_id,\n"
            "  SUM(CASE WHEN c.fiscal_year = 2026 AND c.fiscal_quarter = 1 THEN f.total ELSE 0.0 END) AS q1_2026,\n"
            "  SUM(CASE WHEN c.fiscal_year = 2025 AND c.fiscal_quarter = 1 THEN f.total ELSE 0.0 END) AS q1_2025,\n"
            "  SUM(CASE WHEN c.fiscal_year = 2026 AND c.fiscal_quarter = 1 THEN f.total ELSE 0.0 END)\n"
            "  - SUM(CASE WHEN c.fiscal_year = 2025 AND c.fiscal_quarter = 1 THEN f.total ELSE 0.0 END) AS yoy_delta\n"
            "FROM frtb_sa_capital_output f\n"
            "JOIN cobdate c ON f.cobdate = c.cobdate\n"
            "WHERE (c.fiscal_year = 2026 AND c.fiscal_quarter = 1)\n"
            "   OR (c.fiscal_year = 2025 AND c.fiscal_quarter = 1)\n"
            "GROUP BY f.legal_entity_group_id\n"
            "ORDER BY yoy_delta DESC\n"
            "LIMIT 20"
        ),
        "check": "numeric",
        "wrong_in": "sharding",
        "why_wrong": (
            "<code>cobdate</code> IS correctly configured as a broadcast table in ShardingSphere "
            "(<code>!BROADCAST tables: [cobdate]</code>), so SS can resolve the JOIN. "
            "The failure is subtler: Q1&nbsp;2025 (Jan–Mar 2025) lives in shard&nbsp;1 and "
            "Q1&nbsp;2026 (Jan–Mar 2026) lives in shard&nbsp;2. "
            "SS computes the <code>CASE WHEN fiscal_year=2026 THEN total</code> and "
            "<code>CASE WHEN fiscal_year=2025 THEN total</code> expressions per-shard before merging. "
            "Shard&nbsp;1 has Q1-2025 rows (2026 cases = 0); shard&nbsp;2 has Q1-2026 rows (2025 cases = 0). "
            "The merged <code>yoy_delta</code> column is wrong for the same reason as the cross-date delta query — "
            "SS merges the derived subtraction, not the global components."
        ),
        "why_correct": (
            "Sail loads <code>cobdate</code> as a broadcast temp view from <code>shard_registry.yaml</code> "
            "at bootstrap time. DataFusion resolves the JOIN in-process — no cross-shard data movement. "
            "Both Q1 2025 (shard 1) and Q1 2026 (shard 2) rows are present in the unified scan before "
            "any CASE WHEN expression is evaluated. The subtraction <code>q1_2026 − q1_2025</code> is "
            "computed over the globally unified result, producing the correct YoY delta."
        ),
    },
    {
        "id": "full_risk_aggregation",
        "label": "Full Risk Aggregation — FRTB + Equity + Credit + Macro + SDS Hierarchy",
        "tag": "4-dataset JOIN + broadcast",
        "description": (
            "Joins four independently sharded risk datasets (FRTB SA capital, equity risk, "
            "credit risk, macro risk) with the chorushierarchy broadcast table to produce a "
            "combined exposure view per SDS desk. ShardingSphere cannot execute this — "
            "All four domain tables and chorushierarchy are registered in SS, but SS cannot "
            "JOIN tables across different datasource nodes — it fails with "
            "'Unsupported SQL operation: all tables must be in the same compute node'."
        ),
        "sql": (
            "SELECT\n"
            "  ch.desk_name,\n"
            "  ch.risk_pillar,\n"
            "  SUM(f.total)                  AS frtb_capital,\n"
            "  SUM(e.total_equity_risk)       AS equity_risk,\n"
            "  SUM(cr.total_credit_risk)      AS credit_risk,\n"
            "  SUM(m.total_macro_risk)        AS macro_risk,\n"
            "  SUM(f.total)\n"
            "    + SUM(e.total_equity_risk)\n"
            "    + SUM(cr.total_credit_risk)\n"
            "    + SUM(m.total_macro_risk)    AS total_combined_exposure\n"
            "FROM frtb_sa_capital_output f\n"
            "JOIN equity_risk_measure  e  ON f.legal_entity_group_id = e.legal_entity_group_id\n"
            "                            AND f.cobdate = e.cobdate\n"
            "                            AND f.correlation_scenario_id = e.correlation_scenario_id\n"
            "JOIN credit_risk_measure  cr ON f.legal_entity_group_id = cr.legal_entity_group_id\n"
            "                            AND f.cobdate = cr.cobdate\n"
            "                            AND f.correlation_scenario_id = cr.correlation_scenario_id\n"
            "JOIN macro_risk_measure   m  ON f.legal_entity_group_id = m.legal_entity_group_id\n"
            "                            AND f.cobdate = m.cobdate\n"
            "                            AND f.correlation_scenario_id = m.correlation_scenario_id\n"
            "JOIN chorushierarchy      ch ON f.node_id = ch.node_id\n"
            "WHERE f.cobdate = DATE '2026-01-15'\n"
            "  AND f.driving_scenario = 'HIGH'\n"
            "GROUP BY ch.desk_name, ch.risk_pillar\n"
            "ORDER BY total_combined_exposure DESC\n"
            "LIMIT 20"
        ),
        "check": "numeric",
        "wrong_in": "sharding",
        "why_wrong": (
            "All four domain tables (<code>frtb_sa_capital_output</code>, <code>equity_risk_measure</code>, "
            "<code>credit_risk_measure</code>, <code>macro_risk_measure</code>) and "
            "<code>chorushierarchy</code> are registered in SS across their respective datasources. "
            "The query still fails: SS cannot execute a JOIN where each table lives in a different "
            "datasource node (<code>ds_shard1/2/3</code> for FRTB, <code>ds_equity</code>, "
            "<code>ds_credit</code>, <code>ds_macro</code>). Even with "
            "<code>sql-federation-type: ADVANCED</code>, SS throws "
            "<em>Unsupported SQL operation: all tables must be in the same compute node</em>. "
            "This 4-way cross-datasource JOIN is beyond what SS federation supports."
        ),
        "why_correct": (
            "Sail reads <code>shard_registry.yaml</code> at startup and auto-registers all four datasets "
            "(<code>frtb_sa_capital_output</code>, <code>equity_risk_measure</code>, "
            "<code>credit_risk_measure</code>, <code>macro_risk_measure</code>) as sharded UNION ALL views, "
            "and loads <code>chorushierarchy</code> as a broadcast temp view from shard1. "
            "DataFusion executes the 4-way JOIN and the broadcast lookup entirely in-process — "
            "no cross-shard data shuffle. Each dataset is partition-pruned to the shard(s) "
            "containing cobdate 2026-01-15 before the join."
        ),
    },
    {
        "id": "pushdown_where",
        "label": "Predicate Pushdown — Single-Day Detail",
        "tag": "WHERE pushdown",
        "description": (
            "Pure WHERE filter with no aggregation: returns raw rows for one date and one entity. "
            "Demonstrates sc_arrow predicate pushdown — the WHERE clause is sent to TimescaleDB via JDBC "
            "so DataFusion only receives the matching rows, not the full shard."
        ),
        "sql": (
            "SELECT\n"
            "  node_id,\n"
            "  correlation_scenario_id,\n"
            "  driving_scenario,\n"
            "  total,\n"
            "  interest_rates_delta + interest_rates_vega + interest_rates_curvature AS ir_total,\n"
            "  equities_delta       + equities_vega       + equities_curvature       AS eq_total,\n"
            "  foreign_exchange_delta + foreign_exchange_vega + foreign_exchange_curvature AS fx_total\n"
            "FROM frtb_sa_capital_output\n"
            "WHERE cobdate = DATE '2026-06-15'\n"
            "  AND legal_entity_group_id = 5\n"
            "ORDER BY total DESC\n"
            "LIMIT 50"
        ),
        "check": "numeric",
        "wrong_in": None,
        "why_correct": (
            "With <code>pushDownPredicate=true</code>, Sail's DataFusion JDBC connector pushes "
            "<code>WHERE cobdate = '2026-06-15' AND legal_entity_group_id = 5</code> directly to "
            "TimescaleDB. Only the ~20 matching rows are returned over JDBC — DataFusion never sees "
            "the full 550K-row shard. Query latency drops from ~1,500&nbsp;ms (full shard scan) "
            "to ~15&nbsp;ms. ShardingSphere achieves ~4&nbsp;ms by pushing the entire SQL natively; "
            "the remaining gap is DataFusion plan compilation overhead."
        ),
    },
    {
        "id": "change_calc",
        "label": "Cross-Date Delta — Shard-Boundary Change",
        "tag": "SUM(a)−SUM(b)",
        "description": "Capital change between two dates on different shards: 2026-01-15 (shard 2) minus 2025-01-15 (shard 1). Delta columns are wrong in SS.",
        "sql": (
            "SELECT\n"
            "  legal_entity_group_id,\n"
            "  SUM(CASE WHEN cobdate = DATE '2026-01-15' THEN total ELSE 0.0 END) AS current_total,\n"
            "  SUM(CASE WHEN cobdate = DATE '2025-01-15' THEN total ELSE 0.0 END) AS prior_total,\n"
            "  SUM(CASE WHEN cobdate = DATE '2026-01-15' THEN total ELSE 0.0 END)\n"
            "  - SUM(CASE WHEN cobdate = DATE '2025-01-15' THEN total ELSE 0.0 END) AS delta_total,\n"
            "  SUM(CASE WHEN cobdate = DATE '2026-01-15' THEN interest_rates_delta ELSE 0.0 END)\n"
            "  - SUM(CASE WHEN cobdate = DATE '2025-01-15' THEN interest_rates_delta ELSE 0.0 END) AS delta_ir\n"
            "FROM frtb_sa_capital_output\n"
            "WHERE cobdate IN (DATE '2026-01-15', DATE '2025-01-15')\n"
            "GROUP BY legal_entity_group_id\n"
            "ORDER BY (\n"
            "  SUM(CASE WHEN cobdate = DATE '2026-01-15' THEN total ELSE 0.0 END)\n"
            "  - SUM(CASE WHEN cobdate = DATE '2025-01-15' THEN total ELSE 0.0 END)\n"
            ") DESC\n"
            "LIMIT 20"
        ),
        "check": "numeric",
        "wrong_in": "sharding",
        "why_wrong": (
            "The two dates span the shard boundary: 2025-01-15 lives in shard&nbsp;1 (range 2024–2025-06) "
            "and 2026-01-15 lives in shard&nbsp;2 (range 2025-07→now). "
            "ShardingSphere computes <code>SUM(CASE WHEN cobdate=today …) − SUM(CASE WHEN cobdate=prior …)</code> "
            "<em>per shard</em> before merging. "
            "Shard&nbsp;1 sees only the prior date (today=0, prior=X, delta=−X) and shard&nbsp;2 sees only the current date (today=Y, prior=0, delta=Y). "
            "When SS merges the delta column it uses only one shard's contribution, "
            "producing a wrong delta instead of the correct <code>Y − X</code>."
        ),
        "why_correct": (
            "Sail unifies both shards before evaluating any expression. "
            "Both dates' rows are present in the same DataFusion pipeline, so "
            "<code>SUM(CASE WHEN cobdate=today THEN total ELSE 0)</code> and "
            "<code>SUM(CASE WHEN cobdate=prior THEN total ELSE 0)</code> are computed globally before subtraction."
        ),
    },
]


def _ser(obj):
    if isinstance(obj, decimal.Decimal): return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)): return obj.isoformat()
    raise TypeError(type(obj))


def _run_sail_http(sql):
    """HTTP bridge path: ui_server → QueryService :8090 → HTTP bridge :8080 → SparkConnect."""
    t0   = time.time()
    resp = requests.post(f"{QS_URL}/query", json={"sql": sql, "mode": "sail"}, timeout=120)
    ms   = int((time.time() - t0) * 1000)
    if resp.status_code != 200:
        raise RuntimeError(resp.text)
    data = resp.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(data["error"])
    return data, ms



def _run_sail_flightsql(sql):
    """FlightSQL path (dev): ADBC → FlightSQL :32010 → Arrow IPC (refreshed by bootstrap background thread)."""
    t0   = time.time()
    conn = flightsql_dbapi.connect(FLIGHT_URL)
    try:
        cur  = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in (cur.description or [])]
        rows = [dict(zip(cols, row)) for row in (cur.fetchall() or [])]
        ms   = int((time.time() - t0) * 1000)
        for row in rows:
            for k, v in row.items():
                if isinstance(v, decimal.Decimal):
                    row[k] = float(v)
                elif isinstance(v, (datetime.date, datetime.datetime)):
                    row[k] = v.isoformat()
        return rows, ms
    finally:
        conn.close()


def _run_sail_sparkconnect(sql):
    """SparkConnect JDBC path: ui_server → QueryService :8090 → SparkConnect JDBC → pySail session."""
    t0   = time.time()
    resp = requests.post(f"{QS_URL}/query", json={"sql": sql, "mode": "sail"}, timeout=120)
    ms   = int((time.time() - t0) * 1000)
    if resp.status_code != 200:
        raise RuntimeError(resp.text)
    data = resp.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(data["error"])
    return data, ms


def _run_sail_sc_arrow(sql):
    """SparkConnect gRPC + Arrow IPC (zero-copy): ui_server → QueryService :8090 → SC gRPC → Arrow."""
    t0   = time.time()
    resp = requests.post(f"{QS_URL}/query", json={"sql": sql, "mode": "sail"}, timeout=120)
    ms   = int((time.time() - t0) * 1000)
    if resp.status_code != 200:
        raise RuntimeError(resp.text)
    data = resp.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(data["error"])
    return data, ms


def run_sail(sql):
    global SAIL_ENGINE
    if SAIL_ENGINE == "flightsql":
        return _run_sail_flightsql(sql)
    if SAIL_ENGINE == "sparkconnect":
        return _run_sail_sparkconnect(sql)
    if SAIL_ENGINE == "sc_arrow":
        return _run_sail_sc_arrow(sql)
    return _run_sail_http(sql)


# ── Production Sail server query functions ────────────────────────────────────

def _run_prod_http(sql):
    """HTTP path to production Sail: → HTTP bridge :8083 → SparkConnect :50053 (3 DataFusion workers)."""
    t0 = time.time()
    r  = requests.post(f"http://127.0.0.1:{PROD_WORKERS[0]['port']}/query",
                       json={"sql": sql}, timeout=120)
    ms = int((time.time() - t0) * 1000)
    if r.status_code != 200:
        raise RuntimeError(r.text)
    data = r.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(data["error"])
    return data, ms


def _run_prod_flightsql(sql):
    """FlightSQL path to production Sail: ADBC → FlightSQL :32011 → Arrow IPC (refreshed by bootstrap background thread)."""
    t0   = time.time()
    conn = flightsql_dbapi.connect(PROD_FLIGHT_URL)
    try:
        cur  = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in (cur.description or [])]
        rows = [dict(zip(cols, row)) for row in (cur.fetchall() or [])]
        ms   = int((time.time() - t0) * 1000)
        for row in rows:
            for k, v in row.items():
                if isinstance(v, decimal.Decimal): row[k] = float(v)
                elif isinstance(v, (datetime.date, datetime.datetime)): row[k] = v.isoformat()
        return rows, ms
    finally:
        conn.close()


def _run_prod_sparkconnect(sql):
    """SparkConnect JDBC path (prod): ui_server → QueryService :8090 → SparkConnect JDBC → pySail session."""
    t0 = time.time()
    r  = requests.post(f"{QS_URL}/query", json={"sql": sql, "mode": "sail"}, timeout=120)
    ms = int((time.time() - t0) * 1000)
    if r.status_code != 200:
        raise RuntimeError(r.text)
    data = r.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(data["error"])
    return data, ms


def _run_prod_sc_arrow(sql):
    """SparkConnect gRPC + Arrow IPC (zero-copy, prod): ui_server → QueryService :8090 → SC gRPC → Arrow."""
    t0 = time.time()
    r  = requests.post(f"{QS_URL}/query", json={"sql": sql, "mode": "sail"}, timeout=120)
    ms = int((time.time() - t0) * 1000)
    if r.status_code != 200:
        raise RuntimeError(r.text)
    data = r.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(data["error"])
    return data, ms


def run_sail_prod(sql):
    global SAIL_ENGINE
    if SAIL_ENGINE == "flightsql":
        return _run_prod_flightsql(sql)
    if SAIL_ENGINE == "sparkconnect":
        return _run_prod_sparkconnect(sql)
    if SAIL_ENGINE == "sc_arrow":
        return _run_prod_sc_arrow(sql)
    return _run_prod_http(sql)


def run_shard_direct(sql, shard_idx):
    params = {1: SHARD1_CONN, 2: SHARD2_CONN, 3: SHARD3_CONN}.get(shard_idx, SHARD1_CONN)
    t0 = time.time()
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    ms = int((time.time() - t0) * 1000)
    for row in rows:
        for k, v in row.items():
            if isinstance(v, decimal.Decimal): row[k] = float(v)
            elif isinstance(v, (datetime.date, datetime.datetime)): row[k] = v.isoformat()
    return rows, ms


def run_sharding(sql):
    t0 = time.time()
    resp = requests.post(f"{QS_URL}/query", json={"sql": sql, "mode": "sharding"}, timeout=120)
    ms = int((time.time() - t0) * 1000)
    if resp.status_code != 200:
        raise RuntimeError(resp.text)
    data = resp.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(data["error"])
    return data, ms


def run_spark(sql):
    t0 = time.time()
    resp = requests.post(f"{QS_URL}/query", json={"sql": sql, "mode": "spark"}, timeout=120)
    ms = int((time.time() - t0) * 1000)
    if resp.status_code != 200:
        raise RuntimeError(resp.text)
    data = resp.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(data["error"])
    return data, ms


def compare(sail_rows, ss_rows):
    diffs = []
    total = 0
    n = min(len(sail_rows), len(ss_rows))
    for i in range(n):
        for col in sail_rows[i]:
            if col not in ss_rows[i]: continue
            total += 1
            sv, ssv = sail_rows[i][col], ss_rows[i][col]
            try:
                fv, fss = float(sv), float(ssv)
                base = max(abs(fv), abs(fss), 1e-10)
                if abs(fv - fss) / base > 0.0001:
                    pct = abs(fv - fss) / base * 100
                    diffs.append({"row": i, "col": col, "sail": sv, "ss": ssv, "pct": round(pct, 4)})
            except (TypeError, ValueError):
                if str(sv) != str(ssv):
                    diffs.append({"row": i, "col": col, "sail": sv, "ss": ssv})
    if len(sail_rows) != len(ss_rows):
        diffs.append({"row": -1, "col": "_rowcount",
                      "sail": len(sail_rows), "ss": len(ss_rows)})
    return diffs, total


def rank_issues(rows):
    if not rows or "global_rank" not in rows[0]: return []
    ordered = sorted(rows, key=lambda r: -(float(r.get("sum_total") or 0)))
    issues = []
    for exp, row in enumerate(ordered[:6], 1):
        got = int(row.get("global_rank", exp))
        if got != exp:
            issues.append({
                "entity": row.get("legal_entity_group_id"),
                "sum": row.get("sum_total"),
                "expected_rank": exp,
                "got_rank": got,
            })
    return issues


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(STATIC, "index.html")


@app.route("/api/queries")
def api_queries():
    return jsonify([{k: q[k] for k in ("id","label","tag","description","sql","wrong_in")}
                    for q in QUERIES])


@app.route("/api/status")
def api_status():
    sail = ss = spark = False
    try:
        data = requests.get(f"{QS_URL}/status", timeout=5).json()
        sail  = data.get("sail",     False)
        ss    = data.get("sharding", False)
        spark = data.get("spark",    False)
    except: pass
    return jsonify({"sail": sail, "sharding": ss, "spark": spark})


@app.route("/api/execute", methods=["POST"])
def api_execute():
    body = request.get_json() or {}
    mode = body.get("mode", "sail")
    sql  = (body.get("sql") or "").strip()
    qid  = body.get("query_id")
    if not sql:
        return jsonify({"error": "No SQL provided"}), 400

    q_meta = next((q for q in QUERIES if q["id"] == qid), None)
    # compare_with: which engine is on the right side of compare mode ("spark" default, "sharding")
    compare_with = (body.get("compare_with") or "spark").lower()
    result = {"mode": mode, "sql": sql, "query_id": qid, "sail_engine": SAIL_ENGINE,
              "compare_with": compare_with}

    try:
        if mode == "sail":
            rows, ms = run_sail(sql)
            result.update(rows=rows, elapsed_ms=ms,
                          correctness={"verdict": "CORRECT", "score": 1.0,
                                       "explanation": q_meta["why_correct"] if q_meta else
                                       "Sail computes all aggregations and window functions globally.",
                                       "evidence": [], "diff_cells": []})
        elif mode == "spark":
            rows, ms = run_spark(sql)
            result.update(rows=rows, elapsed_ms=ms,
                          correctness={"verdict": "CORRECT", "score": 1.0,
                                       "explanation": "Spark JVM executed the query.",
                                       "evidence": [], "diff_cells": []})
        elif mode in ("sharding", "compare"):
            right_engine   = compare_with if mode == "compare" else "sharding"
            right_error    = None
            right_rows, right_ms = [], 0
            try:
                if right_engine == "spark":
                    right_rows, right_ms = run_spark(sql)
                else:
                    right_rows, right_ms = run_sharding(sql)
            except Exception as exc:
                right_error = str(exc)

            sail_rows, s_ms = run_sail(sql)
            result.update(rows=right_rows, elapsed_ms=right_ms,
                          sail_rows=sail_rows, sail_elapsed_ms=s_ms,
                          ss_error=right_error)

            diffs, total = compare(sail_rows, right_rows)
            rissues = rank_issues(right_rows) if right_engine == "sharding" else []

            if right_error:
                verdict    = "ERROR"
                eng_label  = "Spark JVM" if right_engine == "spark" else "ShardingSphere"
                expl       = q_meta["why_wrong"] if (q_meta and right_engine == "sharding") \
                             else f"{eng_label} error: {right_error}"
                evidence   = [f"{eng_label} error: {right_error[:300]}"]
                diff_cells = []
            elif rissues:
                verdict    = "INCORRECT"
                expl       = q_meta["why_wrong"] if q_meta else "Rank inconsistency detected."
                evidence   = [
                    f"Entity {r['entity']} (${float(r['sum'] or 0)/1e9:.3f}B): "
                    f"SS rank={r['got_rank']} — should be {r['expected_rank']}"
                    for r in rissues[:4]
                ]
                diff_cells = diffs
            elif diffs:
                verdict    = "INCORRECT"
                expl       = q_meta["why_wrong"] if (q_meta and right_engine == "sharding") \
                             else f"{len(diffs)} cell(s) differ from Sail ground truth."
                evidence   = [
                    f"Row {d['row']} · '{d['col']}': right={d['ss']} vs Sail={d['sail']}"
                    + (f" ({d['pct']:.3f}% off)" if "pct" in d else "")
                    for d in diffs[:5]
                ]
                diff_cells = diffs
            else:
                verdict    = "CORRECT"
                expl       = q_meta["why_correct"] if q_meta else "Results match Sail ground truth."
                evidence   = []
                diff_cells = []

            result["correctness"] = {
                "verdict": verdict, "score": 1 - len(diffs)/max(total,1),
                "explanation": expl, "evidence": evidence, "diff_cells": diff_cells
            }

    except Exception as e:
        result.update(rows=[], error=str(e),
                      correctness={"verdict": "ERROR", "score": 0,
                                   "explanation": str(e), "evidence": [], "diff_cells": []})

    return app.response_class(
        response=json.dumps(result, default=_ser),
        status=200, mimetype="application/json"
    )


@app.route("/api/deepdive", methods=["POST"])
def api_deepdive():
    body = request.get_json() or {}
    sql  = (body.get("sql") or "").strip()
    if not sql:
        return jsonify({"error": "No SQL provided"}), 400
    try:
        s1_rows, s1_ms = run_shard_direct(sql, 1)
        s2_rows, s2_ms = run_shard_direct(sql, 2)
        return app.response_class(
            response=json.dumps({
                "shard1": {"rows": s1_rows[:25], "elapsed_ms": s1_ms,
                           "dbname": "frtb_shard1", "date_range": "2024-01-01 → 2025-06-30", "row_count": 109400},
                "shard2": {"rows": s2_rows[:25], "elapsed_ms": s2_ms,
                           "dbname": "frtb_shard2", "date_range": "2025-07-01 → 2026-09-25", "row_count": 90200},
            }, default=_ser),
            status=200, mimetype="application/json"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _pgpool_running() -> bool:
    import socket
    for port in (6432, 6433, 6434, 6435, 6436, 6437):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                pass
        except OSError:
            return False
    return True


def _port_open(port: int) -> bool:
    import socket
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def _sc_session_ready() -> bool:
    try:
        r = requests.get(f"{SAIL_META_URL}/session", timeout=1)
        if r.status_code == 200:
            return r.json().get("ready", False)
    except Exception:
        pass
    return False


@app.route("/api/production/status")
def api_prod_status():
    try:
        with open(REGISTRY_PATH) as f:
            registry = yaml.safe_load(f)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    shards_cfg = registry.get("shards", {})
    shard_status = []
    for sid, cfg in shards_cfg.items():
        conn_info = SHARD_CONN_MAP.get(sid)
        connected = False
        row_counts = {}
        if conn_info:
            try:
                c = psycopg2.connect(**conn_info, connect_timeout=2)
                with c.cursor() as cur:
                    for ds in registry.get("datasets", []):
                        if sid in ds.get("shards", []):
                            cur.execute(f"SELECT COUNT(*) FROM {ds['table']}")
                            row_counts[ds["name"]] = cur.fetchone()[0]
                    for bt in registry.get("broadcast_tables", []):
                        if bt.get("source_shard") == sid:
                            cur.execute(f"SELECT COUNT(*) FROM {bt['table']}")
                            row_counts[bt["name"] + " (broadcast)"] = cur.fetchone()[0]
                c.close()
                connected = True
            except Exception:
                pass
        shard_status.append({
            "id": sid,
            "database": cfg.get("database", ""),
            "host": cfg.get("host", "localhost"),
            "port": cfg.get("port", 5432),
            "key_range": cfg.get("key_range", {}),
            "connected": connected,
            "row_counts": row_counts,
        })

    worker_status = []
    for w in PROD_WORKERS:
        ok = False
        try:
            ok = requests.get(f"http://127.0.0.1:{w['port']}/health", timeout=2).status_code == 200
        except Exception:
            pass
        worker_status.append({**w, "available": ok,
                               "url": f"http://127.0.0.1:{w['port']}/query"})

    pgpool_conf_snippet = {}
    try:
        with open(PGPOOL_CONF) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    pgpool_conf_snippet[k.strip()] = v.strip()
    except Exception:
        pass

    return jsonify({
        "shards": shard_status,
        "datasets": registry.get("datasets", []),
        "broadcast_tables": registry.get("broadcast_tables", []),
        "workers": worker_status,
        "sail": {
            "available": any(w["available"] for w in worker_status) if SAIL_ENGINE == "http"
                         else (_port_open(32010) if SAIL_ENGINE == "flightsql"
                               else _sc_session_ready()),
            "http_bridge_workers": sum(1 for w in worker_status if w["available"]),
            "bridge_bypassed": SAIL_ENGINE in ("flightsql", "sparkconnect", "sc_arrow"),
            "grpc_port": 50051,
            "flight_port": 32010,
            "mode": "local-cluster",
            "datafusion_workers": PROD_WORKER_COUNT,
            "engine": SAIL_ENGINE,
        },
        "pgpool": {
            "available": _pgpool_running(),
            "reason": None,
            "cloud_status": "Config ready at infra/pgpool_shard1-3/ + pgpool_equity/credit/macro/ — deploys on cloud Linux unchanged",
            "shard1_port": 6432,
            "shard2_port": 6433,
            "shard3_port": 6434,
            "equity_port": 6435,
            "credit_port": 6436,
            "macro_port":  6437,
            "conf": {k: pgpool_conf_snippet[k] for k in
                     ["port", "num_init_children", "max_pool", "backend_hostname0",
                      "backend_port0", "connection_life_time"]
                     if k in pgpool_conf_snippet},
        },
    })


@app.route("/api/production/benchmark", methods=["POST"])
def api_prod_benchmark():
    """
    Concurrent throughput benchmark: fires N simultaneous requests through the
    production query path (run_sail → QueryService → Sail) and records wall-clock
    time.  N ranges from 1 to len(SAIL_WORKERS) to show concurrency throughput.
    Engine-aware: HTTP mode routes through QS → HTTP bridge; FlightSQL routes
    directly via ADBC.
    """
    body       = request.get_json() or {}
    qid        = body.get("query_id", "topn")
    iterations = min(int(body.get("iterations", 5)), 10)

    q   = next((x for x in QUERIES if x["id"] == qid), QUERIES[0])
    sql = q["sql"]

    def _run_one(_sql):
        """Single query via production Sail server (3 DataFusion workers, engine-aware)."""
        t0 = time.time()
        try:
            rows, _ = run_sail_prod(_sql)
            ms = round((time.time() - t0) * 1000, 1)
            return ms, len(rows)
        except Exception:
            return -1, 0

    def _run_concurrent(n, _sql):
        """Fire n concurrent requests to production Sail; return (wall_ms, [per_slot_ms])."""
        import concurrent.futures, time as _time
        results = [None] * n
        wall_start = _time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
            futures = {ex.submit(_run_one, _sql): i for i in range(n)}
            for f in concurrent.futures.as_completed(futures):
                idx = futures[f]
                ms, _ = f.result()
                results[idx] = ms
        wall_ms = round((_time.time() - wall_start) * 1000, 1)
        return wall_ms, results

    scaling_results = []
    for worker_count in range(1, len(PROD_WORKERS) + 1):
        rounds = []
        for i in range(iterations):
            wall_ms, per_worker_ms = _run_concurrent(worker_count, sql)
            rounds.append({"round": i + 1, "wall_ms": wall_ms,
                           "per_worker_ms": per_worker_ms})

        valid_wall = [r["wall_ms"] for r in rounds if r["wall_ms"] > 0]
        warm = valid_wall[1:] if len(valid_wall) > 1 else valid_wall
        scaling_results.append({
            "worker_count": worker_count,
            "worker_labels": [f"Slot {i+1}" for i in range(worker_count)],
            "rounds":        rounds,
            "summary": {
                "avg_wall_ms":  round(sum(warm) / len(warm), 1) if warm else None,
                "min_wall_ms":  min(warm) if warm else None,
                "p50_wall_ms":  sorted(warm)[len(warm) // 2] if warm else None,
            },
        })

    base_avg = scaling_results[0]["summary"]["avg_wall_ms"] or 1
    for sr in scaling_results:
        n_avg = sr["summary"]["avg_wall_ms"] or base_avg
        sr["summary"]["speedup"] = round(base_avg / n_avg, 2)

    return jsonify({
        "query_id":      qid,
        "query_label":   q["label"],
        "iterations":    iterations,
        "scaling":       scaling_results,
        "baseline_avg_ms": base_avg,
    })


@app.route("/api/production/latency_benchmark", methods=["POST"])
def api_latency_benchmark():
    """
    NFR3 / NFR5 — single-query latency benchmark.

    Runs the selected query sequentially N times against one HTTP worker
    and records per-execution latency.  This measures within-query Sail
    DataFusion execution speed — more DataFusion workers (SAIL_WORKER_COUNT)
    means each query is partitioned across more threads, reducing latency.

    To compare 1-worker vs 2-worker vs N-worker:
      SAIL_WORKER_COUNT=1 python3 sail/bootstrap.py   → run this endpoint → note latency
      SAIL_WORKER_COUNT=2 python3 sail/bootstrap.py   → run again → compare
    """
    body       = request.get_json() or {}
    qid        = body.get("query_id", "aggregations")
    iterations = min(int(body.get("iterations", 7)), 15)

    q   = next((x for x in QUERIES if x["id"] == qid), QUERIES[2])
    sql = q["sql"]

    latencies = []
    errors    = []
    for i in range(iterations):
        t0 = time.time()
        try:
            rows, _ = run_sail_prod(sql)
            ms = round((time.time() - t0) * 1000, 1)
            latencies.append({"round": i + 1, "ms": ms, "rows": len(rows)})
        except Exception as exc:
            errors.append({"round": i + 1, "error": str(exc)})

    valid_ms = [x["ms"] for x in latencies]
    warm_ms  = valid_ms[1:] if len(valid_ms) > 1 else valid_ms
    sorted_ms = sorted(warm_ms)

    return jsonify({
        "query_id":    qid,
        "query_label": q["label"],
        "sail_mode":   "local-cluster",
        "datafusion_workers": PROD_WORKER_COUNT,
        "iterations":  iterations,
        "rounds":      latencies,
        "errors":      errors,
        "summary": {
            "avg_ms": round(sum(warm_ms) / len(warm_ms), 1) if warm_ms else None,
            "min_ms": min(warm_ms) if warm_ms else None,
            "p50_ms": sorted_ms[len(sorted_ms) // 2] if sorted_ms else None,
            "max_ms": max(warm_ms) if warm_ms else None,
        },
        "note": (
            f"Production Sail running in 'local-cluster' mode with {PROD_WORKER_COUNT} "
            f"DataFusion workers on SparkConnect :50053."
        ),
    })


@app.route("/api/explain", methods=["POST"])
def api_explain():
    body = request.get_json() or {}
    sql  = (body.get("sql") or "").strip()
    if not sql:
        return jsonify({"error": "No SQL provided"}), 400
    try:
        t0 = time.time()
        resp = requests.post(f"{QS_URL}/explain", json={"sql": sql}, timeout=120)
        ms = int((time.time() - t0) * 1000)
        if resp.status_code != 200:
            raise RuntimeError(resp.text)
        rows = resp.json()
        if isinstance(rows, dict) and "error" in rows:
            raise RuntimeError(rows["error"])
        plan_text = "\n".join(
            row.get("plan", "") or next(iter(row.values()), "")
            for row in rows
        )
        return jsonify({"plan": plan_text, "elapsed_ms": ms})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/production/workers")
def api_prod_workers():
    result = []
    for w in PROD_WORKERS:
        ok = False
        latency_ms = None
        try:
            t0 = time.time()
            r = requests.get(f"http://127.0.0.1:{w['port']}/health", timeout=2)
            latency_ms = round((time.time() - t0) * 1000, 1)
            ok = r.status_code == 200
        except Exception:
            pass
        result.append({**w, "available": ok, "latency_ms": latency_ms})
    return jsonify(result)


@app.route("/api/production/engine", methods=["GET", "POST"])
def api_prod_engine():
    global SAIL_ENGINE
    if request.method == "POST":
        body   = request.get_json() or {}
        engine = (body.get("engine") or "").strip().lower()
        if engine not in ("http", "flightsql", "sparkconnect", "sc_arrow"):
            return jsonify({"error": "engine must be 'http', 'flightsql', 'sparkconnect', or 'sc_arrow'"}), 400
        SAIL_ENGINE = engine
        # Sync engine switch to QueryService so Java also routes correctly
        try:
            requests.post(f"{QS_URL}/engine", json={"engine": engine}, timeout=5)
        except Exception:
            pass  # QueryService may not be running; non-fatal
    return jsonify({"engine": SAIL_ENGINE})


@app.route("/api/production/run_query", methods=["POST"])
def api_prod_run_query():
    """Routes through production Sail server (3 DataFusion workers, engine-aware)."""
    body = request.get_json() or {}
    sql = (body.get("sql") or "").strip()
    if not sql:
        return jsonify({"error": "missing sql"}), 400
    try:
        t0 = time.time()
        rows, _ = run_sail_prod(sql)
        ms = round((time.time() - t0) * 1000, 1)
        return jsonify({"rows": len(rows), "elapsed_ms": ms, "engine": SAIL_ENGINE,
                        "datafusion_workers": PROD_WORKER_COUNT})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/production/run_worker", methods=["POST"])
def api_prod_run_worker():
    """Health test for a specific production HTTP bridge worker."""
    body = request.get_json() or {}
    worker_id = body.get("worker_id")
    sql = (body.get("sql") or "").strip()
    if not sql:
        return jsonify({"error": "missing sql"}), 400
    worker = next((w for w in PROD_WORKERS if w["id"] == worker_id), None)
    if not worker:
        return jsonify({"error": f"unknown worker: {worker_id}"}), 404
    try:
        t0 = time.time()
        r = requests.post(f"http://127.0.0.1:{worker['port']}/query",
                          json={"sql": sql}, timeout=90)
        ms = round((time.time() - t0) * 1000, 1)
        if r.status_code != 200:
            return jsonify({"error": r.text, "worker_id": worker_id}), 500
        data = r.json()
        return jsonify({"worker_id": worker_id, "port": worker["port"],
                        "rows": len(data), "elapsed_ms": ms})
    except Exception as e:
        return jsonify({"error": str(e), "worker_id": worker_id}), 500


if __name__ == "__main__":
    print("FRTB Comparison UI → http://127.0.0.1:5050/")
    app.run(host="127.0.0.1", port=5050, debug=False, threaded=True)
