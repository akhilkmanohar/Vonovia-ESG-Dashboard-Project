SET NOCOUNT ON;

DECLARE @threshold decimal(5,4) = 0.12;
DECLARE @max_year INT = (SELECT MAX([year]) FROM mart.v_energy_mix_other_ratio);

-- SECTION: guardrail_recent (last 10 years)
SELECT 'guardrail_recent' AS section,
       r.[year], r.other_mwh, r.total_mwh, r.other_ratio, @threshold AS threshold,
       CASE WHEN r.other_ratio IS NOT NULL AND r.other_ratio > @threshold THEN 'BREACH' ELSE 'OK' END AS status
FROM mart.v_energy_mix_other_ratio r
WHERE r.[year] >= ISNULL(@max_year,0) - 9
ORDER BY r.[year] DESC;

-- SECTION: breach_summary
SELECT 'breach_summary' AS section,
       SUM(CASE WHEN r.other_ratio > @threshold THEN 1 ELSE 0 END) AS breach_years,
       COUNT(*) AS years_checked
FROM mart.v_energy_mix_other_ratio r
WHERE r.[year] >= ISNULL(@max_year,0) - 9;

-- SECTION: renewable_share_recent (sanity check)
SELECT 'renewable_share_recent' AS section, y.[year], y.renewable_mwh, y.total_mwh, y.renewable_share
FROM mart.v_energy_renewable_share y
WHERE y.[year] >= ISNULL(@max_year,0) - 14
ORDER BY y.[year] DESC;

-- SECTION: override_hits_recent (evidence)
SELECT TOP (200) 'override_hits_recent' AS section,
       t.[year], t.row_label, t.override_id, t.override_pattern,
       t.energy_group AS energy_group, t.is_renewable AS is_renewable, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.override_id IS NOT NULL
  AND t.[year] >= ISNULL(@max_year,0) - 9
ORDER BY t.[year] DESC, t.mwh DESC;

-- SECTION: residual_other_top (what remains to fix)
SELECT TOP (100) 'residual_other_top' AS section, t.[year], t.row_label, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.energy_group = 'Other'
  AND t.is_rate_like = 0 AND t.is_total_like = 0
  AND t.mwh > 0
  AND t.[year] >= ISNULL(@max_year,0) - 9
ORDER BY t.[year] DESC, t.mwh DESC;

-- SECTION: density (rows by year after filtering)
SELECT 'density' AS section, f.[year], COUNT(*) AS rows_mwh
FROM core.v_energy_mix_filtered f
GROUP BY f.[year]
ORDER BY f.[year] DESC;
