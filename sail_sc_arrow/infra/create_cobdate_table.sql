-- cobdate broadcast reference table
-- ShardingSphere will replicate this to all shards automatically
CREATE TABLE IF NOT EXISTS cobdate (
    cobdate        DATE        NOT NULL PRIMARY KEY,
    is_business_day BOOLEAN    NOT NULL DEFAULT TRUE,
    fiscal_year    SMALLINT    NOT NULL,
    fiscal_quarter SMALLINT    NOT NULL,
    fiscal_month   SMALLINT    NOT NULL
);

-- Seed: every date in the full range (2024-01-01 to 2026-09-25)
INSERT INTO cobdate (cobdate, is_business_day, fiscal_year, fiscal_quarter, fiscal_month)
SELECT
    d::DATE,
    EXTRACT(DOW FROM d) NOT IN (0,6),          -- Mon-Fri = business day
    EXTRACT(YEAR  FROM d)::SMALLINT,
    ((EXTRACT(MONTH FROM d) - 1) / 3 + 1)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT
FROM generate_series('2024-01-01'::DATE, '2026-09-25'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT DO NOTHING;

SELECT COUNT(*) AS total_cobdates FROM cobdate;
