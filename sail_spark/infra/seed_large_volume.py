#!/usr/bin/env python3
"""
Reseed all tables to larger volumes (>1M rows for frtb, ~500K for domain tables).

Strategy:
  - TRUNCATE (not DROP) to preserve hypertable structure and indexes
  - Same schema, same ID ranges — existing queries work unchanged
  - frtb_sa_capital_output: 1000 rows/day  → ~1.46M total across 3 shards
  - equity/credit/macro:    extend start to 2018-01-01 → ~550K rows each
  - Reference tables (cobdate, entity_hierarchy, etc.): unchanged
"""
import psycopg2
import random
from datetime import date, timedelta

random.seed(42)

DB_USER = "rajeshganta"
DB_HOST = "127.0.0.1"

SHARD_PORTS = {
    "frtb_shard1": 5440,
    "frtb_shard2": 5441,
    "frtb_shard3": 5442,
    "equity_db":   5443,
    "credit_db":   5444,
    "macro_db":    5445,
}

# frtb_sa_capital_output — sharded by cobdate
FRTB_SHARDS = [
    ("frtb_shard1", date(2024, 1, 1),  date(2025, 6, 30)),
    ("frtb_shard2", date(2025, 7, 1),  date(2026, 12, 31)),
    ("frtb_shard3", date(2027, 1, 1),  date(2027, 12, 31)),
]
FRTB_ROWS_PER_DAY = 1000   # was 150 → now 1000 → ~1.46M total

# Domain databases — single DB covers all dates (no time sharding)
DOMAIN_START = date(2018, 1, 1)    # was 2024-01-01 → extend back for more rows
DOMAIN_END   = date(2027, 12, 31)  # 3653 days × 150 rows/day = ~548K per table

N_ENTITIES = 50   # keep same range (1-50) so GROUP BY queries stay consistent
N_CORR     = 3    # keep same range (1-3)
SCENARIOS  = ["LOW", "MEDIUM", "HIGH"]

NODE_IDS    = [f"NODE_{i:04d}" for i in range(1, 201)]
RISK_COLS   = [
    "interest_rates_delta", "interest_rates_vega", "interest_rates_curvature",
    "equities_delta", "equities_vega", "equities_curvature",
    "foreign_exchange_delta", "foreign_exchange_vega", "foreign_exchange_curvature",
    "commodities_delta", "commodities_vega", "commodities_curvature",
    "csrnonsec_delta", "csrnonsec_vega", "csrnonsec_curvature",
    "csrsecnonctp_delta", "csrsecnonctp_jump_to_default",
    "nonsec_jump_to_default", "rrao",
]  # 19 risk columns + total = 20


def _connect(db_name):
    port = SHARD_PORTS[db_name]
    return psycopg2.connect(host=DB_HOST, port=port, dbname=db_name, user=DB_USER)


def _insert_batched(cur, conn, sql, rows, label):
    total, batch = 0, []
    for row in rows:
        batch.append(row)
        if len(batch) >= 10_000:
            cur.executemany(sql, batch)
            conn.commit()
            total += len(batch)
            batch = []
            print(f"    {label}: {total:,} rows...", end="\r")
    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        total += len(batch)
    print(f"  {label}: {total:,} rows inserted")
    return total


# ── frtb_sa_capital_output ────────────────────────────────────────────────────
# Column order: cobdate, legal_entity_group_id, correlation_scenario_id,
#   driving_scenario, node_id, batch_id, scope_id, hedge_flag,
#   <19 risk cols>, total  — 28 columns total

FRTB_SQL = """INSERT INTO public.frtb_sa_capital_output (
    cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
    node_id, batch_id, scope_id, hedge_flag,
    interest_rates_delta, interest_rates_vega, interest_rates_curvature,
    equities_delta, equities_vega, equities_curvature,
    foreign_exchange_delta, foreign_exchange_vega, foreign_exchange_curvature,
    commodities_delta, commodities_vega, commodities_curvature,
    csrnonsec_delta, csrnonsec_vega, csrnonsec_curvature,
    csrsecnonctp_delta, csrsecnonctp_jump_to_default,
    nonsec_jump_to_default, rrao, total
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

def _gen_frtb_row(cobdate, batch_seq):
    le_id    = random.randint(1, N_ENTITIES)
    corr_id  = random.randint(1, N_CORR)
    node     = random.choice(NODE_IDS)
    sign     = 1 if random.random() > 0.3 else -1
    scale    = random.uniform(1_000, 10_000_000)
    def v(): return sign * random.uniform(0, scale)
    risks = [v() for _ in range(19)]
    return (
        cobdate, le_id, corr_id, random.choice(SCENARIOS),
        node, f"BATCH_{batch_seq:06d}", random.randint(1, 5), random.random() > 0.5,
        *risks, sum(risks),
    )


def reseed_frtb(db_name, start_date, end_date):
    conn = _connect(db_name)
    cur  = conn.cursor()
    cur.execute("TRUNCATE TABLE public.frtb_sa_capital_output")
    conn.commit()

    rows, batch_seq, d = [], 0, start_date
    while d <= end_date:
        for i in range(FRTB_ROWS_PER_DAY):
            rows.append(_gen_frtb_row(d, batch_seq + i))
        batch_seq += FRTB_ROWS_PER_DAY
        d += timedelta(days=1)

    _insert_batched(cur, conn, FRTB_SQL, rows, f"{db_name}/frtb_sa_capital_output")
    cur.close()
    conn.close()


# ── equity_risk_measure ───────────────────────────────────────────────────────

EQUITY_SQL = """INSERT INTO public.equity_risk_measure
    (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
     equity_sensitivity, equity_delta, equity_vega, equity_curvature, total_equity_risk)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

def reseed_equity():
    conn = _connect("equity_db")
    cur  = conn.cursor()
    cur.execute("TRUNCATE TABLE public.equity_risk_measure")
    conn.commit()

    rows, d = [], DOMAIN_START
    while d <= DOMAIN_END:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen  = random.choice(SCENARIOS)
                sign  = 1 if random.random() > 0.3 else -1
                scale = random.uniform(500, 5_000_000)
                s  = sign * random.uniform(0, scale)
                dl = sign * random.uniform(0, scale * 0.8)
                v  = sign * random.uniform(0, scale * 0.5)
                c  = sign * random.uniform(0, scale * 0.3)
                rows.append((d, le, corr, scen, s, dl, v, c, s + dl + v + c))
        d += timedelta(days=1)

    _insert_batched(cur, conn, EQUITY_SQL, rows, "equity_db/equity_risk_measure")
    cur.close()
    conn.close()


# ── credit_risk_measure ───────────────────────────────────────────────────────

CREDIT_SQL = """INSERT INTO public.credit_risk_measure
    (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
     credit_sensitivity, credit_delta, credit_jump_to_default, credit_curvature, total_credit_risk)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

def reseed_credit():
    conn = _connect("credit_db")
    cur  = conn.cursor()
    cur.execute("TRUNCATE TABLE public.credit_risk_measure")
    conn.commit()

    rows, d = [], DOMAIN_START
    while d <= DOMAIN_END:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen  = random.choice(SCENARIOS)
                sign  = 1 if random.random() > 0.25 else -1
                scale = random.uniform(100, 2_000_000)
                s   = sign * random.uniform(0, scale)
                dl  = sign * random.uniform(0, scale * 0.6)
                jtd = abs(random.uniform(0, scale * 0.4))
                c   = sign * random.uniform(0, scale * 0.2)
                rows.append((d, le, corr, scen, s, dl, jtd, c, s + dl + jtd + c))
        d += timedelta(days=1)

    _insert_batched(cur, conn, CREDIT_SQL, rows, "credit_db/credit_risk_measure")
    cur.close()
    conn.close()


# ── macro_risk_measure ────────────────────────────────────────────────────────

MACRO_SQL = """INSERT INTO public.macro_risk_measure
    (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
     ir_sensitivity, fx_sensitivity, commodities_sensitivity, total_macro_risk)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

def reseed_macro():
    conn = _connect("macro_db")
    cur  = conn.cursor()
    cur.execute("TRUNCATE TABLE public.macro_risk_measure")
    conn.commit()

    rows, d = [], DOMAIN_START
    while d <= DOMAIN_END:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen   = random.choice(SCENARIOS)
                sign   = 1 if random.random() > 0.35 else -1
                scale  = random.uniform(200, 3_000_000)
                ir     = sign * random.uniform(0, scale * 0.5)
                fx     = sign * random.uniform(0, scale * 0.3)
                commod = sign * random.uniform(0, scale * 0.2)
                rows.append((d, le, corr, scen, ir, fx, commod, ir + fx + commod))
        d += timedelta(days=1)

    _insert_batched(cur, conn, MACRO_SQL, rows, "macro_db/macro_risk_measure")
    cur.close()
    conn.close()


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    total_frtb_days = sum(
        (end - start).days + 1
        for _, start, end in FRTB_SHARDS
    )
    domain_days = (DOMAIN_END - DOMAIN_START).days + 1
    domain_rows_per_day = N_ENTITIES * N_CORR

    print("=== Large-volume reseed ===")
    print(f"frtb_sa_capital_output: {FRTB_ROWS_PER_DAY} rows/day × {total_frtb_days} days"
          f" = ~{FRTB_ROWS_PER_DAY * total_frtb_days:,} rows total (3 shards)")
    print(f"equity/credit/macro:    {domain_rows_per_day} rows/day × {domain_days} days"
          f" = ~{domain_rows_per_day * domain_days:,} rows each")
    print()

    for db_name, start, end in FRTB_SHARDS:
        print(f"{db_name} ({start} → {end}):")
        reseed_frtb(db_name, start, end)
        print()

    print("equity_db:")
    reseed_equity()
    print()

    print("credit_db:")
    reseed_credit()
    print()

    print("macro_db:")
    reseed_macro()
    print()

    print("Done. Reference tables (cobdate, entity_hierarchy, chorushierarchy,"
          " vw_pillar_node_mapping) unchanged.")
