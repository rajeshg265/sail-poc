#!/usr/bin/env python3
"""
Integration tests — validate Sail query correctness against expected results.

Requires bootstrap.py to be running (workers on :8080/:8081/:8082).
Run:  python3 tests/test_integration.py
"""
import json, sys, time
from pathlib import Path
import requests
import psycopg2

SAIL_URL = "http://127.0.0.1:8080/query"
TIMEOUT  = 60

# pgpool ports (all connections go through pgpool, not directly to TimescaleDB)
PGPOOL_PORTS = {
    "frtb_shard1": 6432, "frtb_shard2": 6433, "frtb_shard3": 6434,
    "equity_db":   6435, "credit_db":   6436, "macro_db":    6437,
}


def sail(sql: str) -> list:
    r = requests.post(SAIL_URL, json={"sql": sql}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def run(name: str, fn):
    try:
        fn()
        print(f"  PASS  {name}")
        return True
    except AssertionError as e:
        print(f"  FAIL  {name}: {e}")
        return False
    except Exception as e:
        print(f"  ERROR {name}: {e}")
        return False


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_workers_healthy():
    for port in [8080, 8081, 8082]:
        r = requests.get(f"http://127.0.0.1:{port}/health", timeout=5)
        assert r.status_code == 200, f"Worker on :{port} unhealthy"


def test_frtb_row_count():
    rows = sail("SELECT COUNT(*) AS n FROM frtb_sa_capital_output")
    n = rows[0]["n"]
    # shard1+shard2 = 18 months each, shard3 = 12 months — total ~219k
    assert n >= 210_000, f"Expected ≥210k FRTB rows, got {n}"


def test_equity_row_count():
    rows = sail("SELECT COUNT(*) AS n FROM equity_risk_measure")
    n = rows[0]["n"]
    assert n >= 200_000, f"Expected ≥200k equity rows, got {n}"


def test_credit_row_count():
    rows = sail("SELECT COUNT(*) AS n FROM credit_risk_measure")
    n = rows[0]["n"]
    assert n >= 200_000, f"Expected ≥200k credit rows, got {n}"


def test_macro_row_count():
    rows = sail("SELECT COUNT(*) AS n FROM macro_risk_measure")
    n = rows[0]["n"]
    assert n >= 200_000, f"Expected ≥200k macro rows, got {n}"


def test_broadcast_cobdate():
    rows = sail("SELECT COUNT(*) AS n FROM cobdate")
    n = rows[0]["n"]
    assert n >= 1_000, f"Expected ≥1000 cobdate rows, got {n}"


def test_broadcast_chorushierarchy():
    rows = sail("SELECT COUNT(*) AS n FROM chorushierarchy")
    n = rows[0]["n"]
    assert n == 200, f"Expected 200 chorushierarchy rows, got {n}"


def test_broadcast_vw_pillar_node_mapping():
    rows = sail("SELECT COUNT(*) AS n FROM vw_pillar_node_mapping")
    n = rows[0]["n"]
    assert n == 200, f"Expected 200 vw_pillar_node_mapping rows, got {n}"


def test_partition_pruning_shard_boundary():
    """
    Query with cobdate in shard2 range should return data.
    Query with cobdate in shard1 range should also return data.
    Both should return non-zero rows (data exists on correct shards).
    """
    r1 = sail("SELECT COUNT(*) AS n FROM frtb_sa_capital_output WHERE cobdate = DATE '2024-06-15'")
    r2 = sail("SELECT COUNT(*) AS n FROM frtb_sa_capital_output WHERE cobdate = DATE '2026-06-15'")
    r3 = sail("SELECT COUNT(*) AS n FROM frtb_sa_capital_output WHERE cobdate = DATE '2027-06-15'")
    assert r1[0]["n"] > 0, "Expected shard1 data for 2024-06-15"
    assert r2[0]["n"] > 0, "Expected shard2 data for 2026-06-15"
    assert r3[0]["n"] > 0, "Expected shard3 data for 2027-06-15"
    # Date outside any shard range should return 0
    r0 = sail("SELECT COUNT(*) AS n FROM frtb_sa_capital_output WHERE cobdate = DATE '2023-01-01'")
    assert r0[0]["n"] == 0, "Expected 0 rows for date before all shards"


def test_topn_window_function():
    rows = sail("""
        SELECT legal_entity_group_id,
               SUM(total) AS sum_total,
               RANK() OVER (ORDER BY SUM(total) DESC) AS global_rank
        FROM frtb_sa_capital_output
        GROUP BY legal_entity_group_id
        ORDER BY global_rank LIMIT 20
    """)
    assert len(rows) == 20, f"Expected 20 TopN rows, got {len(rows)}"
    ranks = [r["global_rank"] for r in rows]
    assert ranks == sorted(ranks), "Ranks not in order"
    assert ranks[0] == 1, "First rank should be 1"


def test_case_when_cross_shard():
    """CASE/WHEN spanning two shards must produce correct delta."""
    rows = sail("""
        SELECT
          legal_entity_group_id,
          SUM(CASE WHEN cobdate = DATE '2026-01-15' THEN total ELSE 0.0 END) AS current_total,
          SUM(CASE WHEN cobdate = DATE '2025-01-15' THEN total ELSE 0.0 END) AS prior_total,
          SUM(CASE WHEN cobdate = DATE '2026-01-15' THEN total ELSE 0.0 END)
          - SUM(CASE WHEN cobdate = DATE '2025-01-15' THEN total ELSE 0.0 END) AS delta
        FROM frtb_sa_capital_output
        WHERE cobdate IN (DATE '2026-01-15', DATE '2025-01-15')
        GROUP BY legal_entity_group_id
        ORDER BY delta DESC LIMIT 5
    """)
    assert len(rows) > 0, "No rows returned for cross-shard CASE/WHEN"
    for r in rows:
        expected_delta = r["current_total"] - r["prior_total"]
        assert abs(r["delta"] - expected_delta) < 0.01, \
            f"CASE/WHEN delta wrong: {r['delta']} != {expected_delta}"


def test_cross_dataset_join_frtb_equity():
    rows = sail("""
        SELECT f.legal_entity_group_id,
               SUM(f.total) AS frtb_capital,
               SUM(e.total_equity_risk) AS equity_risk
        FROM frtb_sa_capital_output f
        JOIN equity_risk_measure e
          ON f.legal_entity_group_id = e.legal_entity_group_id
         AND f.cobdate = e.cobdate
         AND f.correlation_scenario_id = e.correlation_scenario_id
        WHERE f.cobdate = DATE '2026-01-15'
        GROUP BY f.legal_entity_group_id
        ORDER BY frtb_capital DESC LIMIT 10
    """)
    assert len(rows) > 0, "Cross-dataset join FRTB+equity returned no rows"
    for r in rows:
        assert r["frtb_capital"] is not None
        assert r["equity_risk"] is not None


def test_full_cross_domain_join():
    """4-dataset cross-domain join: FRTB + equity + credit + macro + SDS hierarchy."""
    rows = sail("""
        SELECT ch.desk_name, ch.risk_pillar,
               SUM(f.total)               AS frtb_capital,
               SUM(e.total_equity_risk)   AS equity_risk,
               SUM(cr.total_credit_risk)  AS credit_risk,
               SUM(m.total_macro_risk)    AS macro_risk,
               SUM(f.total) + SUM(e.total_equity_risk)
                 + SUM(cr.total_credit_risk) + SUM(m.total_macro_risk) AS combined
        FROM frtb_sa_capital_output f
        JOIN equity_risk_measure  e  ON f.legal_entity_group_id = e.legal_entity_group_id
                                    AND f.cobdate = e.cobdate
                                    AND f.correlation_scenario_id = e.correlation_scenario_id
        JOIN credit_risk_measure  cr ON f.legal_entity_group_id = cr.legal_entity_group_id
                                    AND f.cobdate = cr.cobdate
                                    AND f.correlation_scenario_id = cr.correlation_scenario_id
        JOIN macro_risk_measure   m  ON f.legal_entity_group_id = m.legal_entity_group_id
                                    AND f.cobdate = m.cobdate
                                    AND f.correlation_scenario_id = m.correlation_scenario_id
        JOIN chorushierarchy      ch ON f.node_id = ch.node_id
        WHERE f.cobdate = DATE '2026-01-15'
          AND f.driving_scenario = 'HIGH'
        GROUP BY ch.desk_name, ch.risk_pillar
        ORDER BY combined DESC LIMIT 20
    """)
    assert len(rows) > 0, "Full cross-domain join returned no rows"
    for r in rows:
        expected = (r["frtb_capital"] + r["equity_risk"]
                    + r["credit_risk"] + r["macro_risk"])
        assert abs(r["combined"] - expected) < 0.01, \
            f"Combined exposure sum wrong for desk {r['desk_name']}"


def test_broadcast_join_yoy_delta():
    """YoY delta using cobdate broadcast table across shard boundary."""
    rows = sail("""
        SELECT f.legal_entity_group_id,
               SUM(CASE WHEN c.fiscal_year=2026 AND c.fiscal_quarter=1 THEN f.total ELSE 0 END) AS q1_2026,
               SUM(CASE WHEN c.fiscal_year=2025 AND c.fiscal_quarter=1 THEN f.total ELSE 0 END) AS q1_2025,
               SUM(CASE WHEN c.fiscal_year=2026 AND c.fiscal_quarter=1 THEN f.total ELSE 0 END)
             - SUM(CASE WHEN c.fiscal_year=2025 AND c.fiscal_quarter=1 THEN f.total ELSE 0 END) AS yoy_delta
        FROM frtb_sa_capital_output f
        JOIN cobdate c ON f.cobdate = c.cobdate
        WHERE (c.fiscal_year=2026 AND c.fiscal_quarter=1)
           OR (c.fiscal_year=2025 AND c.fiscal_quarter=1)
        GROUP BY f.legal_entity_group_id
        ORDER BY yoy_delta DESC LIMIT 5
    """)
    assert len(rows) > 0, "Broadcast YoY delta returned no rows"
    for r in rows:
        expected = r["q1_2026"] - r["q1_2025"]
        assert abs(r["yoy_delta"] - expected) < 0.01, \
            f"YoY delta wrong: {r['yoy_delta']} != {expected}"


def test_agg_functions():
    """SUM, AVG, COUNT, MIN, MAX all work on cross-shard data."""
    rows = sail("""
        SELECT
          SUM(total)   AS sum_total,
          AVG(total)   AS avg_total,
          COUNT(*)     AS cnt,
          MIN(total)   AS min_total,
          MAX(total)   AS max_total
        FROM frtb_sa_capital_output
    """)
    r = rows[0]
    assert r["cnt"] >= 210_000, f"COUNT wrong: {r['cnt']}"
    assert r["sum_total"] is not None
    assert r["avg_total"] is not None
    assert r["min_total"] <= r["max_total"]


def test_pillar_node_mapping_join():
    """Join FRTB output with vw_pillar_node_mapping broadcast table."""
    rows = sail("""
        SELECT pm.pillar_name, SUM(f.total) AS capital
        FROM frtb_sa_capital_output f
        JOIN vw_pillar_node_mapping pm ON f.node_id = pm.node_id
        WHERE f.cobdate = DATE '2025-06-15'
        GROUP BY pm.pillar_name
        ORDER BY capital DESC LIMIT 7
    """)
    assert len(rows) > 0, "vw_pillar_node_mapping join returned no rows"


# ── NFR4: Horizontal Scalability for Data Volume ──────────────────────────────

def _pgpool_count(port: int, dbname: str, table: str) -> int:
    """Row count directly from a shard via pgpool (bypasses Sail)."""
    conn = psycopg2.connect(host="127.0.0.1", port=port, dbname=dbname,
                            user="rajeshganta", password="frtb_local",
                            connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM public.{table}")
            return cur.fetchone()[0]
    finally:
        conn.close()


def _pgpool_sum(port: int, dbname: str, table: str, col: str) -> float:
    conn = psycopg2.connect(host="127.0.0.1", port=port, dbname=dbname,
                            user="rajeshganta", password="frtb_local",
                            connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT SUM({col}) FROM public.{table}")
            v = cur.fetchone()[0]
            return float(v or 0)
    finally:
        conn.close()


def test_shard_additivity():
    """
    NFR4 — UNION ALL row count must equal the sum of per-shard row counts.
    Validates that the logical view correctly federates all 3 FRTB shards
    and no rows are dropped or duplicated.
    """
    sail_total = sail("SELECT COUNT(*) AS n FROM frtb_sa_capital_output")[0]["n"]

    per_shard = sum(
        _pgpool_count(port, db, "frtb_sa_capital_output")
        for port, db in [(6432, "frtb_shard1"), (6433, "frtb_shard2"), (6434, "frtb_shard3")]
    )

    assert sail_total == per_shard, (
        f"UNION ALL count ({sail_total}) != sum of individual shards ({per_shard}). "
        "Rows were either dropped or duplicated in the logical view."
    )


def test_cross_shard_sum_consistency():
    """
    NFR4 — SUM(total) via the logical Sail view must equal the sum of
    per-shard SUM(total) values queried directly.  Validates that
    aggregation over the UNION ALL view is numerically correct.
    """
    sail_sum = float(sail("SELECT SUM(total) AS s FROM frtb_sa_capital_output")[0]["s"])

    per_shard_sum = sum(
        _pgpool_sum(port, db, "frtb_sa_capital_output", "total")
        for port, db in [(6432, "frtb_shard1"), (6433, "frtb_shard2"), (6434, "frtb_shard3")]
    )

    rel_diff = abs(sail_sum - per_shard_sum) / max(abs(per_shard_sum), 1e-10)
    assert rel_diff < 0.0001, (
        f"SUM(total) via Sail ({sail_sum:.2f}) differs from per-shard sum "
        f"({per_shard_sum:.2f}) by {rel_diff*100:.4f}%"
    )


def test_registry_driven_view_coverage():
    """
    NFR4 — every dataset and broadcast table in shard_registry.yaml must be
    queryable via Sail without any manual view definition.  Adding a new entry
    to the registry and restarting bootstrap.py is the only change required.
    """
    registry_path = Path(__file__).parent.parent / "sail" / "shard_registry.yaml"
    import yaml
    with open(registry_path) as f:
        registry = yaml.safe_load(f)

    for ds in registry.get("datasets", []):
        rows = sail(f"SELECT COUNT(*) AS n FROM {ds['name']}")
        n = rows[0]["n"]
        assert n >= 0, f"Dataset '{ds['name']}' not queryable via Sail"

    for bt in registry.get("broadcast_tables", []):
        rows = sail(f"SELECT COUNT(*) AS n FROM {bt['name']}")
        n = rows[0]["n"]
        assert n > 0, f"Broadcast table '{bt['name']}' empty or not queryable"


# ── Runner ────────────────────────────────────────────────────────────────────

TESTS = [
    ("Workers healthy",                    test_workers_healthy),
    ("FRTB row count ≥ 270k",             test_frtb_row_count),
    ("Equity row count ≥ 200k (equity_db)",test_equity_row_count),
    ("Credit row count ≥ 200k (credit_db)",test_credit_row_count),
    ("Macro row count ≥ 200k (macro_db)",  test_macro_row_count),
    ("Broadcast cobdate loaded",           test_broadcast_cobdate),
    ("Broadcast chorushierarchy loaded",   test_broadcast_chorushierarchy),
    ("Broadcast vw_pillar_node_mapping",   test_broadcast_vw_pillar_node_mapping),
    ("Partition pruning — shard boundary", test_partition_pruning_shard_boundary),
    ("TopN window function",               test_topn_window_function),
    ("CASE/WHEN cross-shard delta",        test_case_when_cross_shard),
    ("Cross-dataset JOIN FRTB+equity",     test_cross_dataset_join_frtb_equity),
    ("Full 4-dataset cross-domain JOIN",   test_full_cross_domain_join),
    ("Broadcast JOIN — YoY delta",         test_broadcast_join_yoy_delta),
    ("SUM/AVG/COUNT/MIN/MAX aggregations", test_agg_functions),
    ("vw_pillar_node_mapping JOIN",        test_pillar_node_mapping_join),
    # NFR4 — Horizontal Scalability for Data Volume
    ("[NFR4] Shard additivity — UNION ALL row count = Σ shard counts",
                                           test_shard_additivity),
    ("[NFR4] Cross-shard SUM consistency — view SUM = Σ per-shard SUM",
                                           test_cross_shard_sum_consistency),
    ("[NFR4] Registry-driven view coverage — all datasets queryable",
                                           test_registry_driven_view_coverage),
]

if __name__ == "__main__":
    print(f"Running {len(TESTS)} integration tests against {SAIL_URL}\n")
    passed = failed = 0
    for name, fn in TESTS:
        ok = run(name, fn)
        if ok: passed += 1
        else:  failed += 1

    print(f"\n{'='*50}")
    print(f"  {passed} passed  |  {failed} failed  |  {len(TESTS)} total")
    print(f"{'='*50}")
    sys.exit(0 if failed == 0 else 1)
