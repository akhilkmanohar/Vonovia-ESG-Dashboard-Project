IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'rpt')
BEGIN
    EXEC(N'CREATE SCHEMA rpt AUTHORIZATION dbo');
END;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

/* Pillar dimension from unified dataset */
CREATE OR ALTER VIEW rpt.v_dim_pillar
AS
    SELECT DISTINCT
        TRY_CAST(pillar AS nvarchar(1)) AS pillar,
        CASE TRY_CAST(pillar AS nvarchar(1))
            WHEN N'E' THEN N'Environment / Energy'
            WHEN N'S' THEN N'Social'
            WHEN N'G' THEN N'Governance'
            ELSE N'Unknown'
        END AS pillar_name
    FROM rpt.v_esg_pbix_dataset
    WHERE pillar IS NOT NULL;
GO

/* Stream dimension from unified dataset (friendly labels) */
CREATE OR ALTER VIEW rpt.v_dim_stream
AS
    SELECT DISTINCT
        TRY_CAST(stream AS nvarchar(20)) AS stream,
        CASE LOWER(TRY_CAST(stream AS nvarchar(20)))
            WHEN N'counts'  THEN N'Counts (absolute)'
            WHEN N'rates'   THEN N'Rates / Percentages'
            WHEN N'amounts' THEN N'Amounts (EUR)'
            ELSE N'Other'
        END AS stream_name
    FROM rpt.v_esg_pbix_dataset
    WHERE stream IS NOT NULL;
GO

/* ESG-wide latest cards + last-5 slice */
CREATE OR ALTER VIEW rpt.v_esg_cards_latest_and_last5
AS
WITH yr AS (
    SELECT MAX([year]) AS latest_year
    FROM rpt.v_esg_pbix_dataset
),
last5 AS (
    SELECT
        e.pillar,
        e.stream,
        e.[year],
        e.metric,
        TRY_CONVERT(decimal(18,6), e.value_num) AS value_num
    FROM rpt.v_esg_pbix_dataset e
    CROSS JOIN yr
    WHERE e.[year] >= yr.latest_year - 4
),
latest_by_stream AS (
    SELECT *
    FROM (
        SELECT
            e.pillar,
            e.stream,
            e.[year],
            e.metric,
            TRY_CONVERT(decimal(18,6), e.value_num) AS value_num,
            ROW_NUMBER() OVER (
                PARTITION BY e.pillar, e.stream, e.metric
                ORDER BY e.[year] DESC
            ) AS rn
        FROM rpt.v_esg_pbix_dataset e
    ) x
    WHERE rn = 1
)
SELECT N'last5' AS section, pillar, stream, [year], metric, value_num
FROM last5
UNION ALL
SELECT N'latest_by_stream' AS section, pillar, stream, [year], metric, value_num
FROM latest_by_stream;
GO

/* PBIX model catalog: dims + datasets + windows (union) */
CREATE OR ALTER VIEW rpt.v_pbix_model_catalog
AS
    SELECT NULL AS pillar, N'rpt' AS [schema], N'v_dim_year'  AS view_name, N'helper'   AS category, N'Year dimension' AS notes
    UNION ALL
    SELECT NULL, N'rpt', N'v_dim_pillar',  N'helper',  N'Pillar dimension'
    UNION ALL
    SELECT NULL, N'rpt', N'v_dim_stream',  N'helper',  N'Stream dimension'
    UNION ALL
    SELECT NULL, N'rpt', N'v_esg_pbix_dataset', N'esg-long', N'Unified ESG long dataset'
    UNION ALL
    SELECT NULL, N'rpt', N'v_esg_import_catalog', N'manifest', N'Union of pillar catalogs'
    UNION ALL
    SELECT NULL, N'rpt', N'v_pbix_import_manifest', N'manifest', N'Manifest incl. dims'
    UNION ALL
    SELECT NULL, N'rpt', N'v_esg_cards_latest_and_last5', N'cards', N'Latest & last-5 windows (ESG)';
GO
