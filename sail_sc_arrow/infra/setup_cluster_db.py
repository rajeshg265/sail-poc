#!/usr/bin/env python3
"""
Create and seed cluster shard databases for the sail_spark_cluster POC.

Topology:
  cluster_shard1  → TimescaleDB container on port 5450 (cobdate 2024-2025)
  cluster_shard2  → TimescaleDB container on port 5451 (cobdate 2026-2027)

Each shard gets:
  - frtb_sa_capital_output   ~2.2M rows  (partitioned by cobdate)
  - entity_hierarchy         50 rows     (broadcast — JOIN on legal_entity_group_id)
  - chorushierarchy          200 rows    (broadcast — JOIN on node_id)
  - cobdate                  760+ rows   (broadcast — JOIN on cobdate, fiscal quarter)
  - vw_pillar_node_mapping   1400 rows   (broadcast — JOIN on node_id/pillar_code)
"""
import io
import csv
import random
import psycopg2
from datetime import date, timedelta

random.seed(77)

DB_USER = "rajeshganta"
DB_HOST = "127.0.0.1"

SHARDS = [
    ("cluster_shard1", 5450, date(2024, 1, 1),  date(2025, 12, 31)),
    ("cluster_shard2", 5451, date(2026, 1, 1),  date(2027, 12, 31)),
]

FRTB_ROWS_PER_DAY = 3000
N_ENTITIES = 50
N_CORR     = 3
N_NODES    = 200
SCENARIOS  = ["LOW", "MEDIUM", "HIGH"]
NODE_IDS   = [f"NODE_{i:04d}" for i in range(1, N_NODES + 1)]

RISK_PILLARS = [
    ("EQUITY",      "Equity Risk",            "equities"),
    ("RATES",       "Interest Rate Risk",     "interest_rates"),
    ("FX",          "Foreign Exchange Risk",  "foreign_exchange"),
    ("COMMODITIES", "Commodities Risk",       "commodities"),
    ("CSR_NONSEC",  "CSR Non-Securitisation", "csrnonsec"),
    ("CSR_SEC",     "CSR Sec Non-CTP",        "csrsecnonctp"),
    ("RRAO",        "Residual Risk Add-On",   "rrao"),
]
SENSITIVITY_TYPES = ["DELTA", "VEGA", "CURVATURE", "JTD"]
DESKS  = ["Rates Trading","FX Trading","Equity Derivatives","Credit Trading",
          "Commodities","Structured Products","Emerging Markets","Prime Finance",
          "Macro Trading","Flow Credit"]
BOOKS  = ["BB_FLOW","BB_EXOTICS","BB_HEDGE","BB_STRUCTURED","BB_MM"]
PILLARS_HIER = ["Delta Risk","Vega Risk","Curvature Risk","JTD Risk","RRAO"]

FRTB_COLS = [
    "cobdate","batch_id","node_id","node_id_level","node_id_path",
    "scope_id","hedge_flag","projection_index",
    "legal_entity_group_id","consolidation_type_id","allocation_type_id",
    "driving_scenario","correlation_scenario_id",
    "commodities_curvature","commodities_delta","commodities_vega",
    "csrnonsec_curvature","csrnonsec_delta","csrnonsec_vega",
    "csrsecnonctp_delta","csrsecnonctp_jump_to_default",
    "equities_curvature","equities_delta","equities_vega",
    "foreign_exchange_curvature","foreign_exchange_delta","foreign_exchange_vega",
    "interest_rates_curvature","interest_rates_delta","interest_rates_vega",
    "nonsec_jump_to_default","rrao","total",
]

DDL_FRTB = """
DROP TABLE IF EXISTS public.frtb_sa_capital_output CASCADE;
CREATE TABLE public.frtb_sa_capital_output (
    cobdate                        DATE             NOT NULL,
    batch_id                       TEXT,
    node_id                        TEXT,
    node_id_level                  INTEGER,
    node_id_path                   TEXT,
    scope_id                       INTEGER,
    hedge_flag                     BOOLEAN,
    projection_index               INTEGER,
    legal_entity_group_id          INTEGER,
    consolidation_type_id          INTEGER,
    allocation_type_id             INTEGER,
    driving_scenario               TEXT,
    correlation_scenario_id        INTEGER,
    commodities_curvature          DOUBLE PRECISION,
    commodities_delta              DOUBLE PRECISION,
    commodities_vega               DOUBLE PRECISION,
    csrnonsec_curvature            DOUBLE PRECISION,
    csrnonsec_delta                DOUBLE PRECISION,
    csrnonsec_vega                 DOUBLE PRECISION,
    csrsecnonctp_delta             DOUBLE PRECISION,
    csrsecnonctp_jump_to_default   DOUBLE PRECISION,
    equities_curvature             DOUBLE PRECISION,
    equities_delta                 DOUBLE PRECISION,
    equities_vega                  DOUBLE PRECISION,
    foreign_exchange_curvature     DOUBLE PRECISION,
    foreign_exchange_delta         DOUBLE PRECISION,
    foreign_exchange_vega          DOUBLE PRECISION,
    interest_rates_curvature       DOUBLE PRECISION,
    interest_rates_delta           DOUBLE PRECISION,
    interest_rates_vega            DOUBLE PRECISION,
    nonsec_jump_to_default         DOUBLE PRECISION,
    rrao                           DOUBLE PRECISION,
    total                          DOUBLE PRECISION
) PARTITION BY RANGE (cobdate);
CREATE INDEX ON public.frtb_sa_capital_output (cobdate);
CREATE INDEX ON public.frtb_sa_capital_output (legal_entity_group_id, cobdate);
CREATE INDEX ON public.frtb_sa_capital_output (node_id, cobdate);
"""


def _connect(dbname, port):
    return psycopg2.connect(host=DB_HOST, port=port, dbname=dbname, user=DB_USER)


def _build_entity_hierarchy():
    groups = [
        ("EMEA","IB",      "Europe North",         ["UK","Germany","Netherlands","Sweden"]),
        ("EMEA","WM",      "Europe South",          ["France","Spain","Italy","Belgium"]),
        ("APAC","IB",      "Asia Pacific Core",     ["Japan","Singapore","Hong Kong","Australia"]),
        ("APAC","RETAIL",  "Asia Pacific Emerging", ["China","India","South Korea","Thailand"]),
        ("AMER","IB",      "Americas North",        ["US East","US West","Canada","Mexico"]),
        ("AMER","TREASURY","Americas South",        ["Brazil","Argentina","Chile","Colombia"]),
    ]
    rows, eid = [], 1
    per_group = N_ENTITIES // len(groups)
    for g_idx, (region, bu, l2, l3_list) in enumerate(groups):
        count = per_group + (1 if g_idx < N_ENTITIES % len(groups) else 0)
        for i in range(count):
            rows.append((eid, f"Entity_{eid:03d}", region, bu, l2, l3_list[i % len(l3_list)]))
            eid += 1
    return rows


def _build_chorushierarchy():
    rows = []
    for i in range(1, N_NODES + 1):
        level  = ((i-1) % 5) + 1
        parent = f"NODE_{max(1,i-5):04d}" if level > 1 else None
        rows.append((
            f"NODE_{i:04d}", f"{DESKS[(i-1)%len(DESKS)]} L{level} Node {i}",
            parent, level,
            DESKS[(i-1)%len(DESKS)], BOOKS[(i-1)%len(BOOKS)],
            PILLARS_HIER[(i-1)%len(PILLARS_HIER)], f"SDS_{((i-1)//10)+1:03d}",
        ))
    return rows


def _build_pillar_mapping():
    rows = []
    for node_id in NODE_IDS:
        for pillar_code, pillar_name, col_prefix in RISK_PILLARS:
            rows.append((node_id, pillar_code, pillar_name,
                         random.choice(SENSITIVITY_TYPES), col_prefix,
                         round(random.uniform(0.8, 1.2), 4)))
    return rows


ENTITY_ROWS = _build_entity_hierarchy()
CHORUS_ROWS = _build_chorushierarchy()
PILLAR_ROWS = _build_pillar_mapping()


def seed_frtb(dbname, port, start_date, end_date):
    print(f"\n[{dbname}] frtb_sa_capital_output ({start_date} – {end_date})", flush=True)
    conn = _connect(dbname, port)
    cur  = conn.cursor()
    cur.execute(DDL_FRTB)

    y = start_date.year
    while y <= end_date.year:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS frtb_sa_capital_output_{y}
            PARTITION OF public.frtb_sa_capital_output
            FOR VALUES FROM ('{y}-01-01') TO ('{y+1}-01-01');
        """)
        y += 1
    conn.commit()

    # Stream rows via COPY for maximum throughput
    buf  = io.StringIO()
    w    = csv.writer(buf)
    total = 0
    d = start_date
    while d <= end_date:
        batch_id = f"BATCH_{d.strftime('%Y%m%d')}"
        for _ in range(FRTB_ROWS_PER_DAY):
            ni     = random.randint(0, N_NODES - 1)
            nid    = NODE_IDS[ni]
            level  = (ni % 5) + 1
            path   = "/".join([f"NODE_{max(1,ni-k):04d}" for k in range(level-1,-1,-1)])
            risks  = [round(random.gauss(0, 1_000_000), 2) for _ in range(19)]
            total_val = round(sum(risks), 2)
            w.writerow([
                d, batch_id, nid, level, path,
                random.randint(1,5), random.choice([True,False]), random.randint(0,10),
                random.randint(1,N_ENTITIES), random.randint(1,3), random.randint(1,2),
                random.choice(SCENARIOS), random.randint(1,N_CORR),
                *risks, total_val,
            ])
            total += 1
        d += timedelta(days=1)
        if total % 300_000 == 0:
            print(f"  buffered {total:,}…", flush=True)

    buf.seek(0)
    print(f"  COPY {total:,} rows → {dbname}…", flush=True)
    cur.copy_expert(
        f"COPY public.frtb_sa_capital_output ({','.join(FRTB_COLS)}) FROM STDIN WITH CSV",
        buf,
    )
    conn.commit()
    print(f"  {dbname}: {total:,} rows done", flush=True)
    cur.close(); conn.close()


def seed_broadcasts(dbname, port, start_date, end_date):
    print(f"[{dbname}] broadcast tables", flush=True)
    conn = _connect(dbname, port)
    cur  = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS public.entity_hierarchy CASCADE;
        CREATE TABLE public.entity_hierarchy (
            legal_entity_group_id INTEGER PRIMARY KEY,
            entity_name TEXT NOT NULL, region TEXT NOT NULL,
            business_unit TEXT NOT NULL, level2_name TEXT NOT NULL, level3_name TEXT NOT NULL
        );
    """)
    cur.executemany("INSERT INTO public.entity_hierarchy VALUES (%s,%s,%s,%s,%s,%s)", ENTITY_ROWS)
    print(f"  entity_hierarchy: {len(ENTITY_ROWS)} rows", flush=True)

    cur.execute("""
        DROP TABLE IF EXISTS public.chorushierarchy CASCADE;
        CREATE TABLE public.chorushierarchy (
            node_id TEXT PRIMARY KEY, node_name TEXT NOT NULL,
            parent_node_id TEXT, node_level INTEGER NOT NULL,
            desk_name TEXT NOT NULL, trading_book TEXT NOT NULL,
            risk_pillar TEXT NOT NULL, sds_unit_id TEXT NOT NULL
        );
    """)
    cur.executemany("INSERT INTO public.chorushierarchy VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", CHORUS_ROWS)
    print(f"  chorushierarchy: {len(CHORUS_ROWS)} rows", flush=True)

    cur.execute("""
        DROP TABLE IF EXISTS public.cobdate CASCADE;
        CREATE TABLE public.cobdate (
            cobdate DATE PRIMARY KEY, is_business_day BOOLEAN NOT NULL DEFAULT TRUE,
            fiscal_year SMALLINT NOT NULL, fiscal_quarter SMALLINT NOT NULL,
            fiscal_month SMALLINT NOT NULL
        );
    """)
    cobdate_rows = []
    d = start_date - timedelta(days=30)
    while d <= end_date + timedelta(days=30):
        cobdate_rows.append((d, d.weekday()<5, d.year, ((d.month-1)//3)+1, d.month))
        d += timedelta(days=1)
    cur.executemany(
        "INSERT INTO public.cobdate VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        cobdate_rows,
    )
    print(f"  cobdate: {len(cobdate_rows)} rows", flush=True)

    cur.execute("""
        DROP TABLE IF EXISTS public.vw_pillar_node_mapping CASCADE;
        CREATE TABLE public.vw_pillar_node_mapping (
            node_id TEXT NOT NULL, pillar_code TEXT NOT NULL,
            pillar_name TEXT NOT NULL, primary_sensitivity TEXT NOT NULL,
            capital_column_prefix TEXT NOT NULL,
            pillar_weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            PRIMARY KEY (node_id, pillar_code)
        );
    """)
    cur.executemany("INSERT INTO public.vw_pillar_node_mapping VALUES (%s,%s,%s,%s,%s,%s)", PILLAR_ROWS)
    print(f"  vw_pillar_node_mapping: {len(PILLAR_ROWS)} rows", flush=True)

    conn.commit(); cur.close(); conn.close()


if __name__ == "__main__":
    import sys
    print("=" * 60)
    print("sail_spark_cluster — database setup")
    print("=" * 60)

    for dbname, port, start, end in SHARDS:
        try:
            _connect(dbname, port).close()
        except Exception as e:
            print(f"ERROR: cannot connect to {dbname}:{port} — {e}")
            print("Run: bash infra/create_containers.sh")
            sys.exit(1)

    # Truncate shard1 which was partially seeded
    print("\nTruncating partially-seeded tables…")
    for dbname, port, _, __ in SHARDS:
        conn = _connect(dbname, port)
        cur  = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS public.frtb_sa_capital_output CASCADE;")
        conn.commit(); cur.close(); conn.close()
        print(f"  {dbname}: cleared")

    for dbname, port, start_date, end_date in SHARDS:
        seed_frtb(dbname, port, start_date, end_date)
        seed_broadcasts(dbname, port, start_date, end_date)

    print("\n" + "=" * 60)
    print("Setup complete.")
    for dbname, port, start_date, end_date in SHARDS:
        days = (end_date - start_date).days + 1
        print(f"  {dbname}:{port}  ~{days * FRTB_ROWS_PER_DAY:,} rows")
