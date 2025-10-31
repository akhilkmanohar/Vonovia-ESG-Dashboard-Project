/* =========================================================================================
   Module 21 – Environment: Reporting Star (rpt schema)
   - Creates rpt schema (if needed)
   - Dim:   rpt.d_env_metric
   - Fact:  rpt.fact_env_totals  (MERGE from mart.v_env_unified_yearly)
   - Views: rpt.v_env_cards_latest, rpt.v_env_cards_ts_last5 (pass-through from marts)
   - Proc : rpt.sp_refresh_env_reporting to keep dim + fact in sync
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 0) Ensure schema exists
---------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt')
    EXEC ('CREATE SCHEMA rpt AUTHORIZATION dbo;');

---------------------------------------------------------------------------------------------
-- 1) Dimension: metrics
---------------------------------------------------------------------------------------------
IF OBJECT_ID('rpt.d_env_metric','U') IS NULL
BEGIN
    CREATE TABLE rpt.d_env_metric
    (
        metric_code   varchar(50)  NOT NULL PRIMARY KEY,  -- e.g., energy_mwh, water_m3, waste_t
        metric_label  nvarchar(200) NOT NULL,             -- display label
        unit          nvarchar(20)  NOT NULL              -- MWh / m³ / t
    );
END;

;WITH src AS (
    SELECT 'energy_mwh' AS metric_code, 'Energy (MWh)' AS metric_label, 'MWh' AS unit
    UNION ALL SELECT 'water_m3', N'Water (m³)', N'm³'
    UNION ALL SELECT 'waste_t', 'Waste (t)', 't'
)
MERGE rpt.d_env_metric AS tgt
USING src
   ON tgt.metric_code = src.metric_code
WHEN MATCHED AND (tgt.metric_label <> src.metric_label OR tgt.unit <> src.unit)
    THEN UPDATE SET tgt.metric_label = src.metric_label, tgt.unit = src.unit
WHEN NOT MATCHED BY TARGET
    THEN INSERT (metric_code, metric_label, unit) VALUES (src.metric_code, src.metric_label, src.unit)
WHEN NOT MATCHED BY SOURCE
    THEN DELETE
;

---------------------------------------------------------------------------------------------
-- 2) Fact: totals by year
---------------------------------------------------------------------------------------------
IF OBJECT_ID('rpt.fact_env_totals','U') IS NULL
BEGIN
    CREATE TABLE rpt.fact_env_totals
    (
        fact_env_totals_id bigint IDENTITY(1,1) PRIMARY KEY,
        metric_code  varchar(50)   NOT NULL REFERENCES rpt.d_env_metric(metric_code),
        year         int           NOT NULL,
        value        decimal(38,6) NULL,
        load_dts     datetime2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_fact_env_totals UNIQUE (metric_code, year)
    );
    CREATE INDEX IX_fact_env_totals_year  ON rpt.fact_env_totals(year);
    CREATE INDEX IX_fact_env_totals_code  ON rpt.fact_env_totals(metric_code);
END;

---------------------------------------------------------------------------------------------
-- 3) Refresh proc (MERGE from unified mart)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('rpt.sp_refresh_env_reporting','P') IS NULL
    EXEC ('CREATE PROC rpt.sp_refresh_env_reporting AS BEGIN SET NOCOUNT ON; END');
GO
ALTER PROC rpt.sp_refresh_env_reporting
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ch TABLE(action nvarchar(10));

    ;WITH src AS (
        SELECT metric_code, year, value
        FROM mart.v_env_unified_yearly
    )
    MERGE rpt.fact_env_totals AS tgt
    USING src
       ON tgt.metric_code = src.metric_code
      AND tgt.year        = src.year
    WHEN MATCHED AND (ISNULL(tgt.value,0) <> ISNULL(src.value,0))
        THEN UPDATE SET tgt.value = src.value, tgt.load_dts = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (metric_code, year, value) VALUES (src.metric_code, src.year, src.value)
    WHEN NOT MATCHED BY SOURCE
        THEN DELETE
    OUTPUT $action INTO @ch;

    SELECT
        inserted = COALESCE(SUM(CASE WHEN action='INSERT' THEN 1 ELSE 0 END),0),
        updated  = COALESCE(SUM(CASE WHEN action='UPDATE' THEN 1 ELSE 0 END),0),
        deleted  = COALESCE(SUM(CASE WHEN action='DELETE' THEN 1 ELSE 0 END),0)
    FROM @ch;
END
GO

-- Execute initial load
EXEC rpt.sp_refresh_env_reporting;

---------------------------------------------------------------------------------------------
-- 4) Reporting pass-through views for cards
---------------------------------------------------------------------------------------------
IF OBJECT_ID('rpt.v_env_cards_latest','V') IS NOT NULL
    DROP VIEW rpt.v_env_cards_latest;
GO
CREATE VIEW rpt.v_env_cards_latest
AS
SELECT metric_code, metric_label, unit, year, value, yoy_pct, yoy_abs_change, value_3yr_avg
FROM mart.v_env_cards_latest;
GO

IF OBJECT_ID('rpt.v_env_cards_ts_last5','V') IS NOT NULL
    DROP VIEW rpt.v_env_cards_ts_last5;
GO
CREATE VIEW rpt.v_env_cards_ts_last5
AS
SELECT metric_code, metric_label, unit, year, value
FROM mart.v_env_cards_ts_last5;
GO

-- Tiny peek for run summary
SELECT TOP (12) * FROM rpt.fact_env_totals ORDER BY metric_code, year DESC;
