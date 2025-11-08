SET NOCOUNT ON;

DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);

-- SECTION: top_tokens_all (overall impact)
SELECT 'top_tokens_all' AS section, token, years_covered, hits, total_mwh, avg_mwh, last_year, recommended_group
FROM core.v_energy_mix_other_suggestions
ORDER BY total_mwh DESC, hits DESC
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY;

-- SECTION: top_tokens_recent (last 10 years)
SELECT 'top_tokens_recent' AS section, s.token, s.years_covered, s.hits, s.total_mwh, s.avg_mwh, s.last_year, s.recommended_group
FROM core.v_energy_mix_other_suggestions s
WHERE s.last_year >= ISNULL(@max_year,0) - 9
ORDER BY s.total_mwh DESC, s.hits DESC
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY;

-- SECTION: by_year_coverage (share of "Other" captured by suggested tokens)
WITH candidate_tokens AS (
    SELECT token
    FROM core.v_energy_mix_other_suggestions
    WHERE recommended_group <> 'Other'
), flagged AS (
    SELECT
        b.[year],
        b.mwh,
        matched = CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM candidate_tokens ct
                        WHERE b.rl_norm LIKE '%' + ct.token + '%'
                    ) THEN 1 ELSE 0
                  END
    FROM core.v_energy_mix_other_base b
)
SELECT 'by_year_coverage' AS section,
       f.[year],
       matched_mwh = SUM(CASE WHEN f.matched = 1 THEN f.mwh ELSE 0 END),
       total_other_mwh = SUM(f.mwh),
       matched_rows = SUM(CASE WHEN f.matched = 1 THEN 1 ELSE 0 END),
       total_rows = COUNT(*)
FROM flagged f
GROUP BY f.[year]
ORDER BY f.[year] DESC;

-- SECTION: examples_for_top5 (sample labels for highest-impact tokens)
;WITH top5 AS (
  SELECT TOP (5) token
  FROM core.v_energy_mix_other_suggestions
  ORDER BY total_mwh DESC, hits DESC
)
SELECT 'examples_for_top5' AS section, tkn.token, b.[year], b.row_label, b.mwh
FROM top5 tkn
JOIN core.v_energy_mix_other_base b
  ON b.rl_norm LIKE '%' + tkn.token + '%'
ORDER BY tkn.token, b.[year] DESC, b.mwh DESC;

-- SECTION: other_ratio_by_year (context)
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
