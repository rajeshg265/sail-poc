#!/usr/bin/env python3
"""
Generate Parquet data files for sail_parquet benchmark.

Produces:
  data/parquet1.parquet   — 2024-01-01 to 2025-06-30  (~5 GB)
  data/parquet2.parquet   — 2025-07-01 to 2026-12-31  (~5 GB)
  data/entity_hierarchy.parquet
  data/chorushierarchy.parquet
  data/cobdate.parquet

Run once before starting the benchmark:
  python infra/generate_data.py

Requirements: pyarrow (pip install pyarrow)
"""

import os
import sys
import random
import math
from datetime import date, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Constants matching sail_spark_cluster schema ───────────────────────────────

N_ENTITIES  = 50
N_CORR      = 3
N_NODES     = 200
SCENARIOS   = ["LOW", "MEDIUM", "HIGH"]
NODE_IDS    = [f"NODE_{i:04d}" for i in range(1, N_NODES + 1)]
BATCH_IDS   = [f"BATCH_{i:04d}" for i in range(1, 21)]   # 20 batch IDs

DESKS   = ["Rates Trading","FX Trading","Equity Derivatives","Credit Trading",
           "Commodities","Structured Products","Emerging Markets","Prime Finance",
           "Macro Trading","Flow Credit"]
BOOKS   = ["BB_FLOW","BB_EXOTICS","BB_HEDGE","BB_STRUCTURED","BB_MM"]
PILLARS = ["Delta Risk","Vega Risk","Curvature Risk","JTD Risk","RRAO"]

# Rows per business day — tuned so each shard reaches ~5 GB on disk with snappy
# Validated: 40k rows/day → ~3 GB; 70k rows/day → ~5.2 GB
ROWS_PER_DAY = 70_000
CHUNK_ROWS   = 500_000   # write in 500k-row chunks to keep RAM under 1 GB

SHARDS = [
    ("parquet1", date(2024, 1, 1),  date(2025, 6, 30)),
    ("parquet2", date(2025, 7, 1),  date(2026, 12, 31)),
]

# ── PyArrow schema ─────────────────────────────────────────────────────────────

FRTB_SCHEMA = pa.schema([
    ("cobdate",                      pa.date32()),
    ("batch_id",                     pa.string()),
    ("node_id",                      pa.string()),
    ("node_id_level",                pa.int32()),
    ("node_id_path",                 pa.string()),
    ("scope_id",                     pa.int32()),
    ("hedge_flag",                   pa.bool_()),
    ("projection_index",             pa.int32()),
    ("legal_entity_group_id",        pa.int32()),
    ("consolidation_type_id",        pa.int32()),
    ("allocation_type_id",           pa.int32()),
    ("driving_scenario",             pa.string()),
    ("correlation_scenario_id",      pa.int32()),
    ("commodities_curvature",        pa.float64()),
    ("commodities_delta",            pa.float64()),
    ("commodities_vega",             pa.float64()),
    ("csrnonsec_curvature",          pa.float64()),
    ("csrnonsec_delta",              pa.float64()),
    ("csrnonsec_vega",               pa.float64()),
    ("csrsecnonctp_delta",           pa.float64()),
    ("csrsecnonctp_jump_to_default", pa.float64()),
    ("equities_curvature",           pa.float64()),
    ("equities_delta",               pa.float64()),
    ("equities_vega",                pa.float64()),
    ("foreign_exchange_curvature",   pa.float64()),
    ("foreign_exchange_delta",       pa.float64()),
    ("foreign_exchange_vega",        pa.float64()),
    ("interest_rates_curvature",     pa.float64()),
    ("interest_rates_delta",         pa.float64()),
    ("interest_rates_vega",          pa.float64()),
    ("nonsec_jump_to_default",       pa.float64()),
    ("rrao",                         pa.float64()),
    ("total",                        pa.float64()),
])

# ── Row generation ─────────────────────────────────────────────────────────────

def _business_days(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def _generate_chunk(dates_chunk: list[date]) -> pa.RecordBatch:
    n = len(dates_chunk) * ROWS_PER_DAY
    rng = random.Random(hash(str(dates_chunk[0])))

    cobdates          = []
    batch_ids         = []
    node_ids          = []
    node_levels       = []
    node_paths        = []
    scope_ids         = []
    hedge_flags       = []
    proj_indexes      = []
    entity_ids        = []
    consol_ids        = []
    alloc_ids         = []
    scenarios         = []
    corr_ids          = []
    float_cols        = [[] for _ in range(19)]  # 19 risk float cols; total appended after

    for d in dates_chunk:
        d32 = (d - date(1970, 1, 1)).days  # days since epoch for date32
        for _ in range(ROWS_PER_DAY):
            ni   = rng.randint(0, N_NODES - 1)
            nid  = NODE_IDS[ni]
            lvl  = ((ni) % 5) + 1
            eid  = rng.randint(1, N_ENTITIES)
            scen = SCENARIOS[rng.randint(0, 2)]
            corr = rng.randint(1, N_CORR)

            cobdates.append(d32)
            batch_ids.append(BATCH_IDS[rng.randint(0, len(BATCH_IDS) - 1)])
            node_ids.append(nid)
            node_levels.append(lvl)
            node_paths.append(f"ROOT/L1/L2/{nid}")
            scope_ids.append(rng.randint(1, 5))
            hedge_flags.append(rng.random() < 0.2)
            proj_indexes.append(rng.randint(0, 9))
            entity_ids.append(eid)
            consol_ids.append(rng.randint(1, 3))
            alloc_ids.append(rng.randint(1, 4))
            scenarios.append(scen)
            corr_ids.append(corr)

            scale = rng.uniform(0.1, 10.0)
            for fc in float_cols:
                fc.append(round(rng.gauss(0, 1_000_000) * scale, 4))

    # Compute total as sum of all 19 risk float columns, append as 20th
    totals = [sum(float_cols[j][i] for j in range(19)) for i in range(n)]
    float_cols.append(totals)

    arrays = [
        pa.array(cobdates,     type=pa.date32()),
        pa.array(batch_ids,    type=pa.string()),
        pa.array(node_ids,     type=pa.string()),
        pa.array(node_levels,  type=pa.int32()),
        pa.array(node_paths,   type=pa.string()),
        pa.array(scope_ids,    type=pa.int32()),
        pa.array(hedge_flags,  type=pa.bool_()),
        pa.array(proj_indexes, type=pa.int32()),
        pa.array(entity_ids,   type=pa.int32()),
        pa.array(consol_ids,   type=pa.int32()),
        pa.array(alloc_ids,    type=pa.int32()),
        pa.array(scenarios,    type=pa.string()),
        pa.array(corr_ids,     type=pa.int32()),
    ] + [pa.array(fc, type=pa.float64()) for fc in float_cols]

    return pa.record_batch(arrays, schema=FRTB_SCHEMA)


# ── Write a shard ──────────────────────────────────────────────────────────────

def generate_shard(name: str, start: date, end: date):
    path = os.path.join(DATA_DIR, f"{name}.parquet")
    if os.path.exists(path):
        size_gb = os.path.getsize(path) / 1024**3
        print(f"  {name}.parquet already exists ({size_gb:.2f} GB) — skipping")
        return

    bdays = list(_business_days(start, end))
    total_rows = len(bdays) * ROWS_PER_DAY
    chunk_days = max(1, CHUNK_ROWS // ROWS_PER_DAY)

    print(f"  {name}: {len(bdays)} business days × {ROWS_PER_DAY:,} rows = {total_rows:,} total rows")
    print(f"  Writing in chunks of {chunk_days} days ({chunk_days * ROWS_PER_DAY:,} rows) ...")

    writer = pq.ParquetWriter(
        path,
        FRTB_SCHEMA,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
        flavor="spark",
    )

    written = 0
    for i in range(0, len(bdays), chunk_days):
        chunk = bdays[i:i + chunk_days]
        batch = _generate_chunk(chunk)
        writer.write_batch(batch, row_group_size=512_000)
        written += len(batch)
        pct = written / total_rows * 100
        size_mb = os.path.getsize(path) / 1024**2
        print(f"    {written:>12,} / {total_rows:,} rows  ({pct:.0f}%)  {size_mb:.0f} MB written", end="\r")
        sys.stdout.flush()

    writer.close()
    size_gb = os.path.getsize(path) / 1024**3
    print(f"\n  ✓ {name}.parquet  {written:,} rows  {size_gb:.2f} GB")


# ── Lookup tables ──────────────────────────────────────────────────────────────

def generate_entity_hierarchy():
    path = os.path.join(DATA_DIR, "entity_hierarchy.parquet")
    if os.path.exists(path):
        print("  entity_hierarchy.parquet already exists — skipping")
        return

    groups = [
        ("EMEA", "IB",       "Europe North",          ["UK","Germany","Netherlands","Sweden"]),
        ("EMEA", "WM",       "Europe South",          ["France","Spain","Italy","Belgium"]),
        ("APAC", "IB",       "Asia Pacific Core",     ["Japan","Singapore","Hong Kong","Australia"]),
        ("APAC", "RETAIL",   "Asia Pacific Emerging", ["China","India","South Korea","Thailand"]),
        ("AMER", "IB",       "Americas North",        ["US East","US West","Canada","Mexico"]),
        ("AMER", "TREASURY", "Americas South",        ["Brazil","Argentina","Chile","Colombia"]),
    ]
    rows = {"legal_entity_group_id":[], "entity_name":[], "region":[], "business_unit":[], "level2_name":[], "level3_name":[]}
    eid = 1
    per_group = N_ENTITIES // len(groups)
    for g_idx, (region, bu, l2, l3_list) in enumerate(groups):
        count = per_group + (1 if g_idx < N_ENTITIES % len(groups) else 0)
        for i in range(count):
            rows["legal_entity_group_id"].append(eid)
            rows["entity_name"].append(f"Entity_{eid:03d}")
            rows["region"].append(region)
            rows["business_unit"].append(bu)
            rows["level2_name"].append(l2)
            rows["level3_name"].append(l3_list[i % len(l3_list)])
            eid += 1

    table = pa.table(rows)
    pq.write_table(table, path, compression="snappy", write_statistics=True)
    print(f"  ✓ entity_hierarchy.parquet  {len(table)} rows")


def generate_chorushierarchy():
    path = os.path.join(DATA_DIR, "chorushierarchy.parquet")
    if os.path.exists(path):
        print("  chorushierarchy.parquet already exists — skipping")
        return

    rows = {"node_id":[], "node_name":[], "parent_node_id":[], "node_level":[],
            "desk_name":[], "trading_book":[], "risk_pillar":[], "sds_unit_id":[]}
    for i in range(1, N_NODES + 1):
        node_id  = f"NODE_{i:04d}"
        level    = ((i - 1) % 5) + 1
        parent   = f"NODE_{max(1, i - 5):04d}" if level > 1 else None
        rows["node_id"].append(node_id)
        rows["node_name"].append(f"{DESKS[(i-1) % len(DESKS)]} L{level} Node {i}")
        rows["parent_node_id"].append(parent)
        rows["node_level"].append(level)
        rows["desk_name"].append(DESKS[(i-1) % len(DESKS)])
        rows["trading_book"].append(BOOKS[(i-1) % len(BOOKS)])
        rows["risk_pillar"].append(PILLARS[(i-1) % len(PILLARS)])
        rows["sds_unit_id"].append(f"SDS_{((i-1) // 10) + 1:03d}")

    table = pa.table(rows)
    pq.write_table(table, path, compression="snappy", write_statistics=True)
    print(f"  ✓ chorushierarchy.parquet  {len(table)} rows")


def generate_cobdate():
    path = os.path.join(DATA_DIR, "cobdate.parquet")
    if os.path.exists(path):
        print("  cobdate.parquet already exists — skipping")
        return

    start = date(2024, 1, 1) - timedelta(days=30)
    end   = date(2026, 12, 31) + timedelta(days=30)
    rows  = {"cobdate":[], "is_business_day":[], "fiscal_year":[], "fiscal_quarter":[], "fiscal_month":[]}
    d = start
    while d <= end:
        rows["cobdate"].append((d - date(1970, 1, 1)).days)
        rows["is_business_day"].append(d.weekday() < 5)
        rows["fiscal_year"].append(d.year)
        rows["fiscal_quarter"].append(((d.month - 1) // 3) + 1)
        rows["fiscal_month"].append(d.month)
        d += timedelta(days=1)

    schema = pa.schema([
        ("cobdate",         pa.date32()),
        ("is_business_day", pa.bool_()),
        ("fiscal_year",     pa.int16()),
        ("fiscal_quarter",  pa.int16()),
        ("fiscal_month",    pa.int16()),
    ])
    table = pa.table(rows, schema=schema)
    pq.write_table(table, path, compression="snappy", write_statistics=True)
    print(f"  ✓ cobdate.parquet  {len(table)} rows")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Generating FRTB Parquet benchmark data ===\n")
    print("Lookup tables:")
    generate_entity_hierarchy()
    generate_chorushierarchy()
    generate_cobdate()

    print("\nFRTB shard data (this will take several minutes):")
    for name, start, end in SHARDS:
        print(f"\n[{name}]")
        generate_shard(name, start, end)

    print("\n=== Done. Files in data/ ===")
    for f in sorted(os.listdir(DATA_DIR)):
        size = os.path.getsize(os.path.join(DATA_DIR, f)) / 1024**3
        print(f"  {f:<40} {size:.2f} GB")
