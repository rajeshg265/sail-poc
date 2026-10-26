-- frtb_sa_capital_output partitioned by cobdate (simulates TimescaleDB hypertable)
CREATE TABLE IF NOT EXISTS public.frtb_sa_capital_output (
    cobdate                        DATE NOT NULL,
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

-- Shard 1 partitions: 2024-01-01 to 2025-06-30
CREATE TABLE frtb_sa_capital_output_2024
    PARTITION OF public.frtb_sa_capital_output
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE frtb_sa_capital_output_2025h1
    PARTITION OF public.frtb_sa_capital_output
    FOR VALUES FROM ('2025-01-01') TO ('2025-07-01');

-- Shard 2 partitions: 2025-07-01 to 2026-12-31
CREATE TABLE frtb_sa_capital_output_2025h2
    PARTITION OF public.frtb_sa_capital_output
    FOR VALUES FROM ('2025-07-01') TO ('2026-01-01');

CREATE TABLE frtb_sa_capital_output_2026
    PARTITION OF public.frtb_sa_capital_output
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE INDEX ON public.frtb_sa_capital_output (cobdate);
CREATE INDEX ON public.frtb_sa_capital_output (legal_entity_group_id, cobdate);
