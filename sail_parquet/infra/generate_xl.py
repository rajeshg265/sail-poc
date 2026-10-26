#!/usr/bin/env python3
"""
Generate parquet_xl.parquet — N_NODES=2800 to produce ~10M groups
(cobdate x node_id x corr_scenario x driving_scenario) for Implementation 2 10M test.
Same row count as parquet_large (27M rows), just more node diversity.
"""
import os, sys, random
from datetime import date, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

N_ENTITIES  = 50
N_CORR      = 3
N_NODES     = 2800
SCENARIOS   = ["LOW", "MEDIUM", "HIGH"]
NODE_IDS    = [f"NODE_{i:04d}" for i in range(1, N_NODES + 1)]
BATCH_IDS   = [f"BATCH_{i:04d}" for i in range(1, 21)]

ROWS_PER_DAY = 70_000
CHUNK_ROWS   = 500_000

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

def _business_days(start, end):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)

def _generate_chunk(dates_chunk):
    n = len(dates_chunk) * ROWS_PER_DAY
    rng = random.Random(hash(str(dates_chunk[0])))
    cobdates, batch_ids, node_ids, node_levels, node_paths = [], [], [], [], []
    scope_ids, hedge_flags, proj_indexes, entity_ids = [], [], [], []
    consol_ids, alloc_ids, scenarios, corr_ids = [], [], [], []
    float_cols = [[] for _ in range(19)]

    for d in dates_chunk:
        d32 = (d - date(1970, 1, 1)).days
        for _ in range(ROWS_PER_DAY):
            ni  = rng.randint(0, N_NODES - 1)
            nid = NODE_IDS[ni]
            lvl = (ni % 5) + 1
            eid = rng.randint(1, N_ENTITIES)
            scen = SCENARIOS[rng.randint(0, 2)]
            corr = rng.randint(1, N_CORR)
            cobdates.append(d32)
            batch_ids.append(BATCH_IDS[rng.randint(0, len(BATCH_IDS)-1)])
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

path = os.path.join(DATA_DIR, "parquet_xl.parquet")
if os.path.exists(path):
    size_gb = os.path.getsize(path) / 1024**3
    print(f"parquet_xl.parquet already exists ({size_gb:.2f} GB) — skipping")
    sys.exit(0)

start, end = date(2024, 1, 1), date(2025, 6, 30)
bdays = list(_business_days(start, end))
total_rows = len(bdays) * ROWS_PER_DAY
chunk_days = max(1, CHUNK_ROWS // ROWS_PER_DAY)

print(f"Generating parquet_xl.parquet: {len(bdays)} days x {ROWS_PER_DAY:,} rows = {total_rows:,} rows, N_NODES={N_NODES}")
print(f"Expected by_node_date_scenario groups: {len(bdays)} x {N_NODES} x 9 = {len(bdays)*N_NODES*9:,}")

writer = pq.ParquetWriter(path, FRTB_SCHEMA, compression="snappy", use_dictionary=True, write_statistics=True, flavor="spark")
written = 0
for i in range(0, len(bdays), chunk_days):
    chunk = bdays[i:i+chunk_days]
    batch = _generate_chunk(chunk)
    writer.write_batch(batch, row_group_size=512_000)
    written += len(batch)
    pct = written / total_rows * 100
    size_mb = os.path.getsize(path) / 1024**2
    print(f"  {written:>12,} / {total_rows:,} rows  ({pct:.0f}%)  {size_mb:.0f} MB", end="\r")
    sys.stdout.flush()

writer.close()
size_gb = os.path.getsize(path) / 1024**3
print(f"\nDone: parquet_xl.parquet  {written:,} rows  {size_gb:.2f} GB")
