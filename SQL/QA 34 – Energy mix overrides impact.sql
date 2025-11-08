SET NOCOUNT ON;
-- QA 34 - Impact of newly-seeded overrides

DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);

-- SECTION: overrides_hits_recent (where overrides actually matched)
SELECT TOP (200) 'overrides_hits_recent' AS section,
       t.[year], t.row_label, t.override_id, t.override_pattern,
       t.energy_group, t.is_renewable, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.override_id IS NOT NULL
  AND t.[year] >= ISNULL(@max_year,0) - 9
ORDER BY t.[year] DESC, t.mwh DESC;

-- SECTION: other_ratio_by_year (post-seed)
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

-- SECTION: renewable_share_recent (sanity)
SELECT 'renewable_share_recent' AS section, r.[year], r.renewable_mwh, r.total_mwh, r.renewable_share
FROM mart.v_energy_renewable_share r
WHERE r.[year] >= ISNULL(@max_year,0) - 14
ORDER BY r.[year] DESC;

-- SECTION: active_overrides_list (for audit)
SELECT 'active_overrides_list' AS section, override_id, pattern_norm, energy_group, is_renewable, priority, notes
FROM core.energy_mix_overrides
WHERE is_active=1
ORDER BY priority DESC, override_id DESC;
