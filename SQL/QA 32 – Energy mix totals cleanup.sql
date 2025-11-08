SET NOCOUNT ON;
-- QA 32 – Coverage impact after excluding TOTAL/SUM rows
DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);

WITH y AS (
  SELECT [year],
         SUM(CASE WHEN energy_group='Other' THEN mwh ELSE 0 END) AS other_mwh,
         SUM(mwh) AS total_mwh
  FROM core.v_energy_mix_final
  GROUP BY [year]
)
SELECT 'other_ratio_by_year' AS section, [year],
       other_mwh, total_mwh,
       CASE WHEN total_mwh>0 THEN CAST(other_mwh AS decimal(38,6))/CAST(total_mwh AS decimal(38,6)) END AS other_ratio
FROM y
ORDER BY [year] DESC;

SELECT TOP (50) 'exclusions_sample' AS section, t.[year], t.row_label, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.is_total_like = 1
ORDER BY t.[year] DESC, t.mwh DESC;

SELECT 'mix_recent' AS section, f.[year], f.energy_group, f.is_renewable, f.mwh
FROM core.v_energy_mix_final f
WHERE f.[year] >= ISNULL(@max_year,0) - 9
ORDER BY f.[year] DESC, f.energy_group, f.is_renewable DESC;

SELECT 'renewable_share_recent' AS section, r.[year], r.renewable_mwh, r.total_mwh, r.renewable_share
FROM mart.v_energy_renewable_share r
WHERE r.[year] >= ISNULL(@max_year,0) - 14
ORDER BY r.[year] DESC;
