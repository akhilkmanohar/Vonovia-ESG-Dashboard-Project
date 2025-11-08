SET NOCOUNT ON;
-- === QA 44c - Social reporting (smoke) ===

-- coverage
SELECT 'coverage' AS section, 'workforce_rows' AS k, COUNT_BIG(1) AS v FROM mart.v_workforce_headcount_by_year
UNION ALL SELECT 'coverage','hs_counts_rows', COUNT_BIG(1) FROM mart.v_hs_counts_by_year
UNION ALL SELECT 'coverage','hs_rates_rows',  COUNT_BIG(1) FROM mart.v_hs_rates_by_year
UNION ALL SELECT 'coverage','training_rows',  COUNT_BIG(1) FROM mart.v_training_by_year;

-- latest snapshots (one row each)
SELECT TOP 1 'latest_workforce' AS section, * FROM rpt.v_social_workforce_wide ORDER BY [year] DESC;
SELECT TOP 1 'latest_hs_counts' AS section,  * FROM rpt.v_social_hs_counts_wide ORDER BY [year] DESC;
SELECT TOP 1 'latest_hs_rates'  AS section,  * FROM rpt.v_social_hs_rates_wide  ORDER BY [year] DESC;
SELECT TOP 1 'latest_training'  AS section,  * FROM rpt.v_social_training_wide  ORDER BY [year] DESC;

-- cards preview
SELECT TOP 8 * FROM rpt.v_social_cards_latest ORDER BY section, [year] DESC;
