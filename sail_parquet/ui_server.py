#!/usr/bin/env python3
"""
sail_parquet UI server — Sail (Parquet) vs Spark JVM (Parquet) comparison.
Serves benchmark UI at http://localhost:5052/

Engines:
  Sail  — DataFusion/Rust local mode, reads Parquet via SparkConnect :50056
  Spark — JVM local[4], reads Parquet natively
"""
import json
import time
import decimal
import datetime
import os
import statistics

import requests
from flask import Flask, request, jsonify, send_from_directory

STATIC = os.path.join(os.path.dirname(__file__), "ui_static")
app    = Flask(__name__, static_folder=STATIC)

SAIL_BRIDGE_PORT  = 8097
SPARK_BRIDGE_PORT = 8100
BENCHMARK_RUNS    = 10
QUERY_TIMEOUT     = 300


def _fix(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(type(obj))


def _run_sail(sql: str) -> tuple[list, float]:
    t0 = time.time()
    resp = requests.post(f"http://127.0.0.1:{SAIL_BRIDGE_PORT}/query",
                         json={"sql": sql}, timeout=QUERY_TIMEOUT)
    ms = (time.time() - t0) * 1000
    resp.raise_for_status()
    return resp.json(), ms


def _run_sail_arrow(sql: str) -> tuple[list, float]:
    t0 = time.time()
    resp = requests.post(f"http://127.0.0.1:{SAIL_BRIDGE_PORT}/query_arrow",
                         json={"sql": sql}, timeout=QUERY_TIMEOUT)
    ms = (time.time() - t0) * 1000
    resp.raise_for_status()
    return resp.json(), ms


def _run_sail_disk(sql: str) -> tuple[list, float]:
    t0 = time.time()
    resp = requests.post(f"http://127.0.0.1:{SAIL_BRIDGE_PORT}/query_disk",
                         json={"sql": sql}, timeout=QUERY_TIMEOUT)
    ms = (time.time() - t0) * 1000
    resp.raise_for_status()
    return resp.json(), ms


def _run_spark(sql: str) -> tuple[list, float]:
    t0 = time.time()
    resp = requests.post(f"http://127.0.0.1:{SPARK_BRIDGE_PORT}/query",
                         json={"sql": sql}, timeout=QUERY_TIMEOUT)
    ms = (time.time() - t0) * 1000
    resp.raise_for_status()
    return resp.json(), ms


QUERIES = [
    {
        "id":          "full_agg",
        "label":       "Full Scan — Multi-column SUM",
        "tag":         "SELECT agg",
        "description": "Sum 4 risk column groups across all rows from both shards. Tests raw aggregation throughput and column pruning.",
        "sql": (
            "SELECT\n"
            "  SUM(commodities_delta + commodities_vega + commodities_curvature) AS commodities_sa,\n"
            "  SUM(equities_delta    + equities_vega    + equities_curvature)    AS equities_sa,\n"
            "  SUM(interest_rates_delta + interest_rates_vega + interest_rates_curvature) AS rates_sa,\n"
            "  SUM(foreign_exchange_delta + foreign_exchange_vega + foreign_exchange_curvature) AS fx_sa\n"
            "FROM frtb_sa_capital_output"
        ),
    },
    {
        "id":          "grouped_agg",
        "label":       "Group By — Risk by Entity & Scenario",
        "tag":         "SELECT GROUP BY",
        "description": "Aggregate total capital by legal entity and correlation scenario across both shards.",
        "sql": (
            "SELECT\n"
            "  legal_entity_group_id,\n"
            "  correlation_scenario_id,\n"
            "  driving_scenario,\n"
            "  SUM(total) AS total_capital,\n"
            "  COUNT(*)   AS row_count\n"
            "FROM frtb_sa_capital_output\n"
            "GROUP BY legal_entity_group_id, correlation_scenario_id, driving_scenario\n"
            "ORDER BY total_capital DESC\n"
            "LIMIT 20"
        ),
    },
    {
        "id":          "filtered_agg",
        "label":       "Filtered Agg — Partition Range",
        "tag":         "WHERE + agg",
        "description": "Aggregate over first partition range (legal_entity_group_id < 17, ~1/3 of rows). Tests predicate pushdown and partial scan.",
        "sql": (
            "SELECT\n"
            "  legal_entity_group_id,\n"
            "  SUM(total)                AS total_capital,\n"
            "  SUM(equities_delta)       AS eq_delta,\n"
            "  SUM(interest_rates_delta) AS ir_delta\n"
            "FROM frtb_sa_capital_output\n"
            "WHERE legal_entity_group_id < 17\n"
            "GROUP BY legal_entity_group_id\n"
            "ORDER BY total_capital DESC\n"
            "LIMIT 20"
        ),
    },
    {
        "id":          "join_entity",
        "label":       "JOIN — Capital by Region & Business Unit",
        "tag":         "JOIN entity_hierarchy",
        "description": "JOIN frtb_sa_capital_output with entity_hierarchy to aggregate by region and business unit.",
        "sql": (
            "SELECT\n"
            "  e.region,\n"
            "  e.business_unit,\n"
            "  SUM(f.total)                AS total_capital,\n"
            "  SUM(f.equities_delta)       AS eq_delta,\n"
            "  SUM(f.interest_rates_delta) AS ir_delta,\n"
            "  COUNT(*)                    AS row_count\n"
            "FROM frtb_sa_capital_output f\n"
            "JOIN entity_hierarchy e\n"
            "  ON f.legal_entity_group_id = e.legal_entity_group_id\n"
            "GROUP BY e.region, e.business_unit\n"
            "ORDER BY total_capital DESC"
        ),
    },
    {
        "id":          "join_desk",
        "label":       "JOIN — Capital by Trading Desk",
        "tag":         "JOIN chorushierarchy",
        "description": "JOIN frtb_sa_capital_output with chorushierarchy to aggregate by trading desk.",
        "sql": (
            "SELECT\n"
            "  c.desk_name,\n"
            "  c.risk_pillar,\n"
            "  SUM(f.total)          AS total_capital,\n"
            "  SUM(f.rrao)           AS rrao_charge,\n"
            "  COUNT(DISTINCT f.legal_entity_group_id) AS entities\n"
            "FROM frtb_sa_capital_output f\n"
            "JOIN chorushierarchy c ON f.node_id = c.node_id\n"
            "GROUP BY c.desk_name, c.risk_pillar\n"
            "ORDER BY total_capital DESC\n"
            "LIMIT 20"
        ),
    },
    {
        "id":          "join_calendar",
        "label":       "JOIN — Capital by Fiscal Quarter",
        "tag":         "JOIN cobdate",
        "description": "JOIN with cobdate to group capital by fiscal quarter and year.",
        "sql": (
            "SELECT\n"
            "  d.fiscal_year,\n"
            "  d.fiscal_quarter,\n"
            "  SUM(f.total)                AS total_capital,\n"
            "  SUM(f.commodities_delta)    AS commodities_delta,\n"
            "  COUNT(DISTINCT f.legal_entity_group_id) AS entities\n"
            "FROM frtb_sa_capital_output f\n"
            "JOIN cobdate d ON f.cobdate = d.cobdate\n"
            "GROUP BY d.fiscal_year, d.fiscal_quarter\n"
            "ORDER BY d.fiscal_year, d.fiscal_quarter"
        ),
    },
]

QUERY_MAP = {q["id"]: q for q in QUERIES}


@app.route("/api/benchmark", methods=["POST"])
def api_benchmark():
    body    = request.get_json() or {}
    sql     = (body.get("sql") or "").strip()
    qid     = body.get("query_id", "")
    engines = (body.get("engines") or "both").lower()
    if not sql and qid in QUERY_MAP:
        sql = QUERY_MAP[qid]["sql"]
    if not sql:
        return jsonify({"error": "missing sql or query_id"}), 400

    all_engines = [("sail_arrow", _run_sail_arrow), ("sail_disk", _run_sail_disk), ("spark", _run_spark)]
    run_engines = [e for e in all_engines if engines in ("both", "all") or e[0] == engines]
    results = {}

    for engine_name, run_fn in run_engines:
        timings = []
        last_rows = []
        errors = []

        try:
            run_fn(sql)  # warm-up
        except Exception:
            pass

        for i in range(BENCHMARK_RUNS):
            try:
                rows, ms = run_fn(sql)
                timings.append(round(ms, 1))
                last_rows = rows
            except Exception as e:
                errors.append(f"run {i+1}: {e}")

        if timings:
            sorted_t  = sorted(timings)
            p99_idx   = max(0, int(len(sorted_t) * 0.99) - 1)
            p99       = sorted_t[p99_idx]
            mean_ms   = round(statistics.mean(timings), 1)
            median_ms = round(statistics.median(timings), 1)
        else:
            p99 = mean_ms = median_ms = 0

        results[engine_name] = {
            "runs":       BENCHMARK_RUNS,
            "successful": len(timings),
            "errors":     errors,
            "timings":    timings,
            "p99_ms":     round(p99, 1),
            "mean_ms":    mean_ms,
            "median_ms":  median_ms,
            "min_ms":     round(min(timings), 1) if timings else 0,
            "max_ms":     round(max(timings), 1) if timings else 0,
            "row_count":  len(last_rows),
            "rows":       last_rows[:50],
        }

    return jsonify({"query_id": qid, "sql": sql, "runs": BENCHMARK_RUNS, **results})


@app.route("/api/execute", methods=["POST"])
def api_execute():
    body   = request.get_json() or {}
    sql    = (body.get("sql") or "").strip()
    engine = (body.get("engine") or "sail").lower()
    if not sql:
        return jsonify({"error": "missing sql"}), 400
    try:
        run_fn = {"sail": _run_sail, "sail_arrow": _run_sail_arrow, "sail_disk": _run_sail_disk, "spark": _run_spark}.get(engine, _run_sail)
        rows, ms = run_fn(sql)
        return jsonify({"engine": engine, "elapsed_ms": round(ms, 1),
                        "rows": rows, "row_count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e), "engine": engine}), 500


@app.route("/api/queries", methods=["GET"])
def api_queries():
    return jsonify([{k: q[k] for k in ("id","label","tag","description","sql")}
                    for q in QUERIES])


@app.route("/api/logs", methods=["GET"])
def api_logs():
    since = request.args.get("since", "")
    combined = []
    for name, port in [("sail", SAIL_BRIDGE_PORT), ("spark", SPARK_BRIDGE_PORT)]:
        try:
            r = requests.get(f"http://127.0.0.1:{port}/logs",
                             params={"since": since}, timeout=2)
            if r.ok:
                combined.extend(r.json())
        except Exception:
            pass
    combined.sort(key=lambda x: x["ts"])
    return jsonify(combined)


@app.route("/api/health", methods=["GET"])
def api_health():
    status = {}
    for name, port in [("sail", SAIL_BRIDGE_PORT), ("spark", SPARK_BRIDGE_PORT)]:
        try:
            r = requests.get(f"http://127.0.0.1:{port}/health", timeout=2)
            status[name] = "ok" if r.ok else "error"
        except Exception:
            status[name] = "down"
    return jsonify(status)


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_static(path):
    from flask import make_response
    if path and os.path.exists(os.path.join(STATIC, path)):
        resp = make_response(send_from_directory(STATIC, path))
    else:
        resp = make_response(send_from_directory(STATIC, "index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


if __name__ == "__main__":
    import logging
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    print("Parquet Benchmark UI: http://localhost:5052/")
    app.run(host="0.0.0.0", port=5052, threaded=True)
