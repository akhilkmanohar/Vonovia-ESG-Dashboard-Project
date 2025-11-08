SET NOCOUNT ON;

DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);

-- SECTION: overrides_hits_recent (confirm the new tokens hit)
SELECT TOP (200) 'overrides_hits_recent' AS section,
       t.[year], t.row_label, t.override_id, t.override_pattern,
       t.energy_group, t.is_renewable, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.override_id IS NOT NULL
  AND t.[year] >= ISNULL(@max_year,0) - 9
ORDER BY t.[year] DESC, t.mwh DESC;

-- SECTION: other_ratio_by_year (post-override)
WITH y AS (
  SELECT [year],
         other_mwh = SUM(CASE WHEN energy_group = 'Other' THEN mwh ELSE 0 END),
         total_mwh = SUM(mwh)
  FROM core.v_energy_mix_final
  GROUP BY [year]
)
SELECT 'other_ratio_by_year' AS section, [year], other_mwh, total_mwh,
       CASE WHEN total_mwh > 0 THEN CAST(other_mwh AS decimal(38,6)) / CAST(total_mwh AS decimal(38,6)) END AS other_ratio
FROM y
ORDER BY [year] DESC;

-- SECTION: renewable_share_recent (sanity)
SELECT 'renewable_share_recent' AS section, r.[year], r.renewable_mwh, r.total_mwh, r.renewable_share
FROM mart.v_energy_renewable_share r
WHERE r.[year] >= ISNULL(@max_year,0) - 14
ORDER BY r.[year] DESC;

-- SECTION: residual_other_top (what remains in 'Other' last 10 years)
SELECT TOP (100) 'residual_other_top' AS section, t.[year], t.row_label, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.energy_group = 'Other'
  AND t.is_rate_like = 0 AND t.is_total_like = 0
  AND t.mwh > 0
  AND t.[year] >= ISNULL(@max_year,0) - 9
ORDER BY t.[year] DESC, t.mwh DESC;
