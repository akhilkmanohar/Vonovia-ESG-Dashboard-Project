SET NOCOUNT ON;
-- Module 23c – Energy Mix pivots & shares (reporting convenience)
-- Depends on Module 23/23b (core.v_energy_mix_final, mart.v_energy_mix_by_year, mart.v_energy_renewable_share)

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt')  EXEC('CREATE SCHEMA rpt;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart') EXEC('CREATE SCHEMA mart;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core') EXEC('CREATE SCHEMA core;');
GO

/* 1) Wide pivot for PBIX: year-level columns per group + renewable/nonrenewable */
CREATE OR ALTER VIEW rpt.v_energy_mix_wide
AS
WITH f AS (
    SELECT [year], energy_group, is_renewable, mwh
    FROM core.v_energy_mix_final
)
SELECT
    f.[year],
    -- Electricity
    SUM(CASE WHEN f.energy_group='Electricity' AND f.is_renewable=1 THEN f.mwh ELSE 0 END) AS electricity_renewable_mwh,
    SUM(CASE WHEN f.energy_group='Electricity' AND f.is_renewable=0 THEN f.mwh ELSE 0 END) AS electricity_nonrenewable_mwh,
    -- Heat
    SUM(CASE WHEN f.energy_group='Heat' AND f.is_renewable=1 THEN f.mwh ELSE 0 END) AS heat_renewable_mwh,
    SUM(CASE WHEN f.energy_group='Heat' AND f.is_renewable=0 THEN f.mwh ELSE 0 END) AS heat_nonrenewable_mwh,
    -- Fuels
    SUM(CASE WHEN f.energy_group='Fuels' AND f.is_renewable=1 THEN f.mwh ELSE 0 END) AS fuels_renewable_mwh,
    SUM(CASE WHEN f.energy_group='Fuels' AND f.is_renewable=0 THEN f.mwh ELSE 0 END) AS fuels_nonrenewable_mwh,
    -- Other bucket (no renewable split)
    SUM(CASE WHEN f.energy_group='Other' THEN f.mwh ELSE 0 END) AS other_mwh,
    -- Totals
    SUM(f.mwh) AS total_mwh
FROM f
GROUP BY f.[year];
GO

/* 2) Share of renewables within each energy_group by year */
CREATE OR ALTER VIEW rpt.v_energy_mix_share_by_group
AS
WITH g AS (
    SELECT
        [year],
        energy_group,
        renewable_mwh = SUM(CASE WHEN is_renewable=1 THEN mwh ELSE 0 END),
        total_mwh     = SUM(mwh)
    FROM core.v_energy_mix_final
    GROUP BY [year], energy_group
)
SELECT
    [year],
    energy_group,
    renewable_mwh,
    total_mwh,
    renewable_share_group = CASE WHEN total_mwh>0
                                 THEN CAST(renewable_mwh AS decimal(38,6))/CAST(total_mwh AS decimal(38,6))
                                 ELSE NULL END
FROM g;
GO

/* 3) Quick peeks */
DECLARE @max_year int = (SELECT MAX([year]) FROM rpt.v_energy_mix_wide);

SELECT TOP (10) * FROM rpt.v_energy_mix_wide ORDER BY [year] DESC;

SELECT TOP (30) [year], energy_group, renewable_mwh, total_mwh, renewable_share_group
FROM rpt.v_energy_mix_share_by_group
ORDER BY [year] DESC, energy_group;
