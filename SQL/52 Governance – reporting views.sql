-- Ensure schema exists using a separate dynamic batch (avoids "first statement" error)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'rpt')
BEGIN
    EXEC(N'CREATE SCHEMA rpt AUTHORIZATION dbo');
END;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO
CREATE OR ALTER VIEW rpt.v_gov_counts_long
AS
    SELECT TRY_CONVERT(int,[year]) AS [year],
           TRY_CAST([measure_group] AS nvarchar(200)) AS [measure_group],
           TRY_CONVERT(decimal(18,4),[value_num]) AS [value_num]
    FROM mart.v_gov_counts_by_year;
GO
CREATE OR ALTER VIEW rpt.v_gov_rates_long
AS
    SELECT TRY_CONVERT(int,[year]) AS [year],
           TRY_CAST([rate_metric] AS nvarchar(200)) AS [rate_metric],
           TRY_CONVERT(decimal(18,6),[value_num]) AS [value_num]
    FROM mart.v_gov_rates_by_year;
GO
CREATE OR ALTER VIEW rpt.v_gov_amounts_long
AS
    SELECT TRY_CONVERT(int,[year]) AS [year],
           TRY_CAST([measure_group] AS nvarchar(200)) AS [measure_group],
           TRY_CONVERT(decimal(18,2),[amount_eur]) AS [amount_eur]
    FROM mart.v_gov_amounts_by_year;
GO
CREATE OR ALTER VIEW rpt.v_gov_cards_latest_and_last5
AS
WITH yr AS (
    SELECT MAX(y) AS latest_year
    FROM (
        SELECT MAX([year]) AS y FROM rpt.v_gov_counts_long
        UNION ALL SELECT MAX([year]) FROM rpt.v_gov_rates_long
        UNION ALL SELECT MAX([year]) FROM rpt.v_gov_amounts_long
    ) s
),
last5 AS (
    SELECT 'counts' AS stream, c.[year], c.[measure_group] AS metric, c.[value_num] AS value_num
    FROM rpt.v_gov_counts_long c CROSS JOIN yr
    WHERE c.[year] >= yr.latest_year - 4
    UNION ALL
    SELECT 'rates', r.[year], r.[rate_metric], r.[value_num]
    FROM rpt.v_gov_rates_long r CROSS JOIN yr
    WHERE r.[year] >= yr.latest_year - 4
    UNION ALL
    SELECT 'amounts', a.[year], a.[measure_group], TRY_CONVERT(decimal(18,4), a.[amount_eur])
    FROM rpt.v_gov_amounts_long a CROSS JOIN yr
    WHERE a.[year] >= yr.latest_year - 4
),
latest_by_stream AS (
    SELECT 'latest_by_stream' AS section, *
    FROM (
        SELECT 'counts' AS stream, c.[year], c.[measure_group] AS metric, c.[value_num] AS value_num,
               ROW_NUMBER() OVER (PARTITION BY 'counts', c.[measure_group] ORDER BY c.[year] DESC) AS rn
        FROM rpt.v_gov_counts_long c
        UNION ALL
        SELECT 'rates', r.[year], r.[rate_metric], r.[value_num],
               ROW_NUMBER() OVER (PARTITION BY 'rates', r.[rate_metric] ORDER BY r.[year] DESC) AS rn
        FROM rpt.v_gov_rates_long r
        UNION ALL
        SELECT 'amounts', a.[year], a.[measure_group], TRY_CONVERT(decimal(18,4), a.[amount_eur]),
               ROW_NUMBER() OVER (PARTITION BY 'amounts', a.[measure_group] ORDER BY a.[year] DESC) AS rn
        FROM rpt.v_gov_amounts_long a
    ) x
    WHERE rn = 1
)
SELECT 'last5' AS section, l.stream, l.[year], l.metric, l.value_num
FROM last5 l
UNION ALL
SELECT section, stream, [year], metric, value_num
FROM latest_by_stream;
GO
CREATE OR ALTER VIEW rpt.v_gov_import_catalog
AS
SELECT 'rpt' AS [schema], 'v_gov_counts_long'  AS view_name, 'governance-counts'  AS category,
       'Long format counts by year from mart.v_gov_counts_by_year' AS notes
WHERE EXISTS (SELECT 1 FROM rpt.v_gov_counts_long)
UNION ALL
SELECT 'rpt','v_gov_rates_long','governance-rates',
       'Long format rates by year from mart.v_gov_rates_by_year'
WHERE EXISTS (SELECT 1 FROM rpt.v_gov_rates_long)
UNION ALL
SELECT 'rpt','v_gov_amounts_long','governance-amounts',
       'Long format EUR amounts by year from mart.v_gov_amounts_by_year'
WHERE EXISTS (SELECT 1 FROM rpt.v_gov_amounts_long)
UNION ALL
SELECT 'rpt','v_gov_cards_latest_and_last5','governance-cards',
       'Latest & last-5-years slices per stream'
UNION ALL
SELECT 'rpt','v_gov_cards_latest','governance-cards-latest',
       'Curated latest governance cards'
WHERE OBJECT_ID('rpt.v_gov_cards_latest','V') IS NOT NULL;
GO
CREATE OR ALTER PROCEDURE rpt.sp_refresh_governance_reporting
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @latest_year int =
    (
        SELECT MAX(y) FROM (
            SELECT MAX([year]) AS y FROM rpt.v_gov_counts_long
            UNION ALL SELECT MAX([year]) FROM rpt.v_gov_rates_long
            UNION ALL SELECT MAX([year]) FROM rpt.v_gov_amounts_long
        ) s
    );

    -- light touches only (no OPTION hints)
    DECLARE @c_latest int = (SELECT TOP(1) [year] FROM rpt.v_gov_counts_long  ORDER BY [year] DESC);
    DECLARE @r_latest int = (SELECT TOP(1) [year] FROM rpt.v_gov_rates_long   ORDER BY [year] DESC);
    DECLARE @a_latest int = (SELECT TOP(1) [year] FROM rpt.v_gov_amounts_long ORDER BY [year] DESC);

    IF OBJECT_ID('mart.sp_refresh_reporting_all','P') IS NOT NULL
    BEGIN
        BEGIN TRY
            EXEC mart.sp_refresh_reporting_all;
        END TRY
        BEGIN CATCH
            SELECT 'mart.sp_refresh_reporting_all failed: ' + ERROR_MESSAGE() AS warning;
        END CATCH
    END

    SELECT 'governance_reporting' AS scope,
           @latest_year           AS latest_year,
           @c_latest              AS counts_latest_seen_year,
           @r_latest              AS rates_latest_seen_year,
           @a_latest              AS amounts_latest_seen_year;
END
GO
