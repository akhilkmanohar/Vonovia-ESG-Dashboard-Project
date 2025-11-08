SET NOCOUNT ON;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

CREATE OR ALTER VIEW rpt.v_gov_pbix_dataset
AS
    SELECT
        TRY_CONVERT(int, [year])                   AS [year],
        CAST('counts' AS nvarchar(20))             AS stream,
        TRY_CAST([measure_group] AS nvarchar(200)) AS metric,
        TRY_CONVERT(decimal(18,6), [value_num])    AS value_num
    FROM rpt.v_gov_counts_long
    UNION ALL
    SELECT
        TRY_CONVERT(int, [year]),
        CAST('rates' AS nvarchar(20)),
        TRY_CAST([rate_metric] AS nvarchar(200)),
        TRY_CONVERT(decimal(18,6), [value_num])
    FROM rpt.v_gov_rates_long
    UNION ALL
    SELECT
        TRY_CONVERT(int, [year]),
        CAST('amounts' AS nvarchar(20)),
        TRY_CAST([measure_group] AS nvarchar(200)),
        TRY_CONVERT(decimal(18,6), [amount_eur])
    FROM rpt.v_gov_amounts_long;
GO

CREATE OR ALTER VIEW rpt.v_gov_import_catalog_plus
AS
SELECT [schema], view_name, category, notes
FROM rpt.v_gov_import_catalog
UNION ALL
SELECT
    'rpt' AS [schema],
    'v_gov_pbix_dataset' AS view_name,
    'governance-long' AS category,
    'Unified long dataset (counts/rates/amounts) for PBIX' AS notes;
GO
