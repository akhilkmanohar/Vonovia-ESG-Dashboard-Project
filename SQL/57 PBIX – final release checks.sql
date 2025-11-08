IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'rpt')
BEGIN
    EXEC(N'CREATE SCHEMA rpt AUTHORIZATION dbo');
END;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

/* Helper: latest year across unified dataset */
CREATE OR ALTER VIEW rpt.v_esg_latest_year
AS
SELECT MAX([year]) AS latest_year
FROM rpt.v_esg_pbix_dataset;
GO

/* Gaps: list missing years per pillar between min..max in dataset */
CREATE OR ALTER VIEW rpt.v_esg_year_gaps
AS
WITH bounds AS (
    SELECT pillar, MIN([year]) AS y_min, MAX([year]) AS y_max
    FROM rpt.v_esg_pbix_dataset
    WHERE pillar IS NOT NULL
    GROUP BY pillar
),
Nums AS (
    SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM sys.all_objects
),
Span AS (
    SELECT b.pillar, (b.y_min + n.n) AS [year]
    FROM bounds b
    JOIN Nums n
      ON (b.y_min + n.n) <= b.y_max
)
SELECT s.pillar, s.[year]
FROM Span s
LEFT JOIN (
    SELECT DISTINCT pillar, [year]
    FROM rpt.v_esg_pbix_dataset
    WHERE [year] IS NOT NULL AND pillar IS NOT NULL
) d
  ON d.pillar = s.pillar
 AND d.[year] = s.[year]
WHERE d.[year] IS NULL;
GO

/* Duplicates: more than one row for same pillar/stream/year/metric */
CREATE OR ALTER VIEW rpt.v_esg_duplicates_scan
AS
SELECT pillar, stream, [year], metric, COUNT_BIG(*) AS dup_rows
FROM rpt.v_esg_pbix_dataset
GROUP BY pillar, stream, [year], metric
HAVING COUNT_BIG(*) > 1;
GO

/* Model health: compact readiness metrics */
CREATE OR ALTER VIEW rpt.v_model_health_summary
AS
WITH ly AS (SELECT latest_year FROM rpt.v_esg_latest_year),
tot AS (SELECT COUNT_BIG(*) AS rows_total FROM rpt.v_esg_pbix_dataset),
counts AS (
    SELECT
        SUM(CASE WHEN stream = 'counts'  THEN 1 ELSE 0 END) AS rows_counts,
        SUM(CASE WHEN stream = 'rates'   THEN 1 ELSE 0 END) AS rows_rates,
        SUM(CASE WHEN stream = 'amounts' THEN 1 ELSE 0 END) AS rows_amounts
    FROM rpt.v_esg_pbix_dataset
)
SELECT 'esg_rows_total' AS metric, CAST(t.rows_total AS bigint) AS val
FROM tot t
UNION ALL
SELECT 'latest_year', CAST(ly.latest_year AS bigint)
FROM ly
UNION ALL
SELECT 'rows_counts', CAST(c.rows_counts AS bigint) FROM counts c
UNION ALL
SELECT 'rows_rates', CAST(c.rows_rates AS bigint) FROM counts c
UNION ALL
SELECT 'rows_amounts', CAST(c.rows_amounts AS bigint) FROM counts c;
GO
