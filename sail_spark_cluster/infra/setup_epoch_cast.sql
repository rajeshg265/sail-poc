-- Workaround for pysail 0.7.1 JDBC date pushdown bug:
-- pysail converts DATE literals to integer epoch-days when building JDBC
-- WHERE clauses (e.g. cobdate >= 19723). PostgreSQL has no built-in
-- DATE >= INTEGER operator. We add an implicit CAST so the comparison works.

CREATE OR REPLACE FUNCTION integer_to_date(n INTEGER) RETURNS DATE AS $$
  SELECT ('1970-01-01'::date + n)
$$ LANGUAGE SQL IMMUTABLE STRICT;

-- Drop existing cast if already present (idempotent)
DROP CAST IF EXISTS (INTEGER AS DATE);
CREATE CAST (INTEGER AS DATE) WITH FUNCTION integer_to_date(INTEGER) AS IMPLICIT;
