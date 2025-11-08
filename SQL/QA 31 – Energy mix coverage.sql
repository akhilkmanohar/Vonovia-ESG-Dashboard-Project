SET NOCOUNT ON;
-- QA 31 – Energy mix coverage & residual 'Other' analysis
DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);

WITH y AS (
  SELECT [year],
         SUM(CASE WHEN energy_group='Electricity' THEN mwh ELSE 0 END) AS electricity_mwh,
         SUM(CASE WHEN energy_group='Heat'        THEN mwh ELSE 0 END) AS heat_mwh,
         SUM(CASE WHEN energy_group='Fuels'       THEN mwh ELSE 0 END) AS fuels_mwh,
         SUM(CASE WHEN energy_group='Other'       THEN mwh ELSE 0 END) AS other_mwh,
         SUM(mwh) AS total_mwh
  FROM core.v_energy_mix_final
  GROUP BY [year]
)
SELECT 'coverage_by_group_year' AS section, [year],
       electricity_mwh, heat_mwh, fuels_mwh, other_mwh, total_mwh,
       CASE WHEN total_mwh>0 THEN CAST(other_mwh AS decimal(38,6))/CAST(total_mwh AS decimal(38,6)) END AS other_ratio
FROM y
ORDER BY [year] DESC;

SELECT 'group_breakdown_recent' AS section, f.[year], f.energy_group, f.is_renewable, f.mwh
FROM core.v_energy_mix_final f
WHERE f.[year] >= ISNULL(@max_year,0) - 9
ORDER BY f.[year] DESC, f.energy_group, f.is_renewable DESC;

SELECT 'renewable_share_recent' AS section, r.[year], r.renewable_mwh, r.total_mwh, r.renewable_share
FROM mart.v_energy_renewable_share r
WHERE r.[year] >= ISNULL(@max_year,0) - 14
ORDER BY r.[year] DESC;

SELECT TOP (50) 'token_hits_sample' AS section,
       t.[year], t.row_label, t.energy_group, t.is_renewable, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.mwh IS NOT NULL AND t.is_rate_like = 0
ORDER BY t.[year] DESC, t.mwh DESC;

SELECT 'density_mwh_rows' AS section, e.[year], COUNT(*) AS rows_mwh
FROM core.energy_yearly e
WHERE e.derived_unit = 'MWh'
GROUP BY e.[year]
ORDER BY e.[year] DESC;
