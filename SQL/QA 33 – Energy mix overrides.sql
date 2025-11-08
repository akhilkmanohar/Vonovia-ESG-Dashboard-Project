SET NOCOUNT ON;
-- QA 33 – Overrides audit & candidate list

DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);

SELECT 'overrides_in_effect' AS section, t.[year], t.row_label, t.override_id, t.override_pattern,
       t.energy_group, t.is_renewable, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.override_id IS NOT NULL
ORDER BY t.[year] DESC, t.mwh DESC;

SELECT TOP (100) 'top_other_candidates' AS section, t.[year], t.row_label, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.override_id IS NULL
  AND t.mwh > 0
  AND t.[year] >= ISNULL(@max_year,0) - 9
  AND t.row_label IS NOT NULL AND t.row_label <> ''
  AND t.energy_group = 'Other'
ORDER BY t.[year] DESC, t.mwh DESC;

WITH y AS (
  SELECT [year],
         other_mwh = SUM(CASE WHEN energy_group='Other' THEN mwh ELSE 0 END),
         total_mwh = SUM(mwh)
  FROM core.v_energy_mix_final
  GROUP BY [year]
)
SELECT 'other_ratio_by_year' AS section, [year], other_mwh, total_mwh,
       CASE WHEN total_mwh>0 THEN CAST(other_mwh AS decimal(38,6))/CAST(total_mwh AS decimal(38,6)) END AS other_ratio
FROM y
ORDER BY [year] DESC;

SELECT 'renewable_share_recent' AS section, r.[year], r.renewable_mwh, r.total_mwh, r.renewable_share
FROM mart.v_energy_renewable_share r
WHERE r.[year] >= ISNULL(@max_year,0) - 14
ORDER BY r.[year] DESC;
