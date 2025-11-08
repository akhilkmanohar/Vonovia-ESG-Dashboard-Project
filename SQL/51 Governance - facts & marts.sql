SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core') EXEC('CREATE SCHEMA core');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart') EXEC('CREATE SCHEMA mart');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt') EXEC('CREATE SCHEMA rpt');
GO

IF OBJECT_ID('core.v_gov_tagged','V') IS NULL  EXEC('CREATE VIEW core.v_gov_tagged AS SELECT 1 AS stub');
IF OBJECT_ID('core.v_gov_filtered','V') IS NULL EXEC('CREATE VIEW core.v_gov_filtered AS SELECT 1 AS stub');
IF OBJECT_ID('mart.v_gov_counts_by_year','V') IS NULL EXEC('CREATE VIEW mart.v_gov_counts_by_year AS SELECT 1 AS stub');
IF OBJECT_ID('mart.v_gov_rates_by_year','V')  IS NULL EXEC('CREATE VIEW mart.v_gov_rates_by_year  AS SELECT 1 AS stub');
IF OBJECT_ID('mart.v_gov_amounts_by_year','V') IS NULL EXEC('CREATE VIEW mart.v_gov_amounts_by_year AS SELECT 1 AS stub');
IF OBJECT_ID('rpt.v_gov_cards_latest','V') IS NULL EXEC('CREATE VIEW rpt.v_gov_cards_latest AS SELECT 1 AS stub');
GO

CREATE OR ALTER VIEW core.v_gov_tagged
AS
WITH base AS (
    SELECT
        g.[year],
        g.source,
        g.sheet,
        g.row_label,
        g.value_num,
        g.derived_unit,
        g.label_norm
    FROM core.v_gov_discovery g
    WHERE g.value_num IS NOT NULL
),
flags AS (
    SELECT
        b.*,
        CASE
            WHEN b.label_norm LIKE '%\%%' ESCAPE '\' THEN 1
            WHEN b.label_norm LIKE '% rate%' OR b.label_norm LIKE '% quote%' OR b.label_norm LIKE '% anteil%' OR b.label_norm LIKE '% share%'
                THEN 1 ELSE 0 END AS is_rate_like,
        CASE
            WHEN b.derived_unit IS NOT NULL
                 AND b.derived_unit COLLATE Latin1_General_100_CI_AI LIKE '%eur%'
                THEN 1 ELSE 0 END AS is_currency_like,
        CASE
            WHEN b.label_norm LIKE '%total%' OR b.label_norm LIKE '%summe%' OR b.label_norm LIKE '%gesamt%'
                THEN 1 ELSE 0 END AS is_total_like
    FROM base b
),
tags AS (
    SELECT
        f.*,
        CASE
            WHEN f.label_norm LIKE '%board%' OR f.label_norm LIKE '%aufsichtsrat%' OR f.label_norm LIKE '%vorstand%' THEN N'board'
            WHEN f.label_norm LIKE '%divers%' OR f.label_norm LIKE '%women%' OR f.label_norm LIKE '%female%' THEN N'diversity'
            WHEN f.label_norm LIKE '%whistle%' OR f.label_norm LIKE '%hinweis%' THEN N'whistleblowing'
            WHEN f.label_norm LIKE '%compliance%' OR f.label_norm LIKE '%anticorrupt%' OR f.label_norm LIKE '%korrupt%' THEN N'compliance'
            WHEN f.label_norm LIKE '%audit%' OR f.label_norm LIKE '%revision%' THEN N'audit'
            WHEN f.label_norm LIKE '%supplier%' OR f.label_norm LIKE '%lieferant%' THEN N'suppliers'
            WHEN f.label_norm LIKE '%training%' OR f.label_norm LIKE '%schulung%' THEN N'training'
            WHEN f.label_norm LIKE '%incident%' OR f.label_norm LIKE '%verstoß%' OR f.label_norm LIKE '%verstoss%' THEN N'incidents'
            WHEN f.label_norm LIKE '%privacy%' OR f.label_norm LIKE '%datenschutz%' THEN N'data_privacy'
            ELSE N'other'
        END AS measure_group
    FROM flags f
)
SELECT
    t.[year],
    t.source,
    t.sheet,
    t.row_label,
    t.value_num,
    t.derived_unit,
    t.label_norm,
    t.is_rate_like,
    t.is_currency_like,
    t.is_total_like,
    t.measure_group
FROM tags t;
GO

CREATE OR ALTER VIEW core.v_gov_filtered
AS
SELECT
    t.[year],
    t.measure_group,
    t.value_num,
    t.is_rate_like,
    t.is_currency_like,
    t.is_total_like,
    CASE
        WHEN t.is_rate_like = 1 THEN N'rate'
        WHEN t.is_currency_like = 1 THEN N'amount'
        ELSE N'count'
    END AS metric_type
FROM core.v_gov_tagged t
WHERE t.is_total_like = 0;
GO

CREATE OR ALTER VIEW mart.v_gov_counts_by_year
AS
SELECT
    [year],
    measure_group,
    SUM(value_num) AS value_num
FROM core.v_gov_filtered
WHERE metric_type = N'count'
GROUP BY [year], measure_group;
GO

CREATE OR ALTER VIEW mart.v_gov_rates_by_year
AS
SELECT
    [year],
    measure_group AS rate_metric,
    AVG(value_num) AS value_num
FROM core.v_gov_filtered
WHERE metric_type = N'rate'
GROUP BY [year], measure_group;
GO

CREATE OR ALTER VIEW mart.v_gov_amounts_by_year
AS
SELECT
    [year],
    measure_group,
    SUM(value_num) AS amount_eur
FROM core.v_gov_filtered
WHERE metric_type = N'amount'
GROUP BY [year], measure_group;
GO

CREATE OR ALTER VIEW rpt.v_gov_cards_latest
AS
WITH y AS (
    SELECT MAX([year]) AS y_max
    FROM core.v_gov_filtered
),
counts AS (
    SELECT TOP (5)
        'count' AS stream,
        c.measure_group AS metric,
        c.value_num,
        c.[year]
    FROM mart.v_gov_counts_by_year c
    CROSS JOIN y
    WHERE c.[year] = y.y_max
    ORDER BY c.value_num DESC
),
rates AS (
    SELECT TOP (5)
        'rate' AS stream,
        r.rate_metric AS metric,
        r.value_num,
        r.[year]
    FROM mart.v_gov_rates_by_year r
    CROSS JOIN y
    WHERE r.[year] = y.y_max
    ORDER BY r.value_num DESC
),
amounts AS (
    SELECT TOP (5)
        'amount' AS stream,
        a.measure_group AS metric,
        a.amount_eur AS value_num,
        a.[year]
    FROM mart.v_gov_amounts_by_year a
    CROSS JOIN y
    WHERE a.[year] = y.y_max
    ORDER BY a.amount_eur DESC
)
SELECT * FROM counts
UNION ALL
SELECT * FROM rates
UNION ALL
SELECT * FROM amounts;
GO
