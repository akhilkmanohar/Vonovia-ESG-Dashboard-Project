SET NOCOUNT ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt') EXEC('CREATE SCHEMA rpt');

-- Workforce (wide): passthrough to mart view (already aggregated & normalized)
IF OBJECT_ID('rpt.v_social_workforce_wide','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_workforce_wide AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_workforce_wide AS
SELECT
    CAST([year] AS int)      AS [year],
    CAST([measure] AS nvarchar(100)) AS [measure],
    CAST([value_num] AS decimal(38,6)) AS [value_num]
FROM mart.v_workforce_headcount_by_year;
GO

-- H&S counts (wide)
IF OBJECT_ID('rpt.v_social_hs_counts_wide','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_hs_counts_wide AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_hs_counts_wide AS
SELECT
    CAST([year] AS int)      AS [year],
    CAST([metric] AS nvarchar(150)) AS [metric],
    CAST([value_num] AS decimal(38,6)) AS [value_num]
FROM mart.v_hs_counts_by_year;
GO

-- H&S rates (wide)
IF OBJECT_ID('rpt.v_social_hs_rates_wide','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_hs_rates_wide AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_hs_rates_wide AS
SELECT
    CAST([year] AS int)      AS [year],
    CAST([rate_metric] AS nvarchar(150)) AS [metric],
    CAST([value_num] AS decimal(38,6)) AS [value_num]
FROM mart.v_hs_rates_by_year;
GO

-- Training (wide)
IF OBJECT_ID('rpt.v_social_training_wide','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_training_wide AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_training_wide AS
SELECT
    CAST([year] AS int)      AS [year],
    CAST([metric] AS nvarchar(150)) AS [metric],
    CAST([value_num] AS decimal(38,6)) AS [value_num]
FROM mart.v_training_by_year;
GO

-- Cards (latest snapshot across the four areas) - light, safe union
IF OBJECT_ID('rpt.v_social_cards_latest','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_cards_latest AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_cards_latest AS
WITH y AS (
    SELECT
        (SELECT MAX([year]) FROM mart.v_workforce_headcount_by_year) AS y_work,
        (SELECT MAX([year]) FROM mart.v_hs_counts_by_year)          AS y_counts,
        (SELECT MAX([year]) FROM mart.v_hs_rates_by_year)           AS y_rates,
        (SELECT MAX([year]) FROM mart.v_training_by_year)           AS y_train
)
SELECT 'workforce' AS section, w.[year], w.[measure]  AS label, w.[value_num] AS value_num
FROM mart.v_workforce_headcount_by_year w CROSS JOIN y
WHERE w.[year] = y.y_work
UNION ALL
SELECT 'hs_counts', c.[year], c.[metric], c.[value_num]
FROM mart.v_hs_counts_by_year c CROSS JOIN y
WHERE c.[year] = y.y_counts
UNION ALL
SELECT 'hs_rates', r.[year], r.[rate_metric], r.[value_num]
FROM mart.v_hs_rates_by_year r CROSS JOIN y
WHERE r.[year] = y.y_rates
UNION ALL
SELECT 'training', t.[year], t.[metric], t.[value_num]
FROM mart.v_training_by_year t CROSS JOIN y
WHERE t.[year] = y.y_train;
GO
