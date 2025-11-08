SET NOCOUNT ON;
-- === QA 44d - Social PBIX handoff ===

-- catalog
SELECT 'catalog' AS section, schema_name, view_name, purpose
FROM rpt.v_social_import_catalog
ORDER BY view_name;

-- latest year per area
SELECT 'latest_years' AS section, 'workforce' AS area, MAX([year]) AS latest FROM mart.v_workforce_headcount_by_year
UNION ALL SELECT 'latest_years','hs_counts', MAX([year]) FROM mart.v_hs_counts_by_year
UNION ALL SELECT 'latest_years','hs_rates',  MAX([year]) FROM mart.v_hs_rates_by_year
UNION ALL SELECT 'latest_years','training',  MAX([year]) FROM mart.v_training_by_year;

-- counts (reporting views)
SELECT 'counts' AS section, 'rpt.v_social_workforce_wide' AS view_name, COUNT_BIG(1) AS rows FROM rpt.v_social_workforce_wide
UNION ALL SELECT 'counts','rpt.v_social_hs_counts_wide', COUNT_BIG(1) FROM rpt.v_social_hs_counts_wide
UNION ALL SELECT 'counts','rpt.v_social_hs_rates_wide',  COUNT_BIG(1) FROM rpt.v_social_hs_rates_wide
UNION ALL SELECT 'counts','rpt.v_social_training_wide',  COUNT_BIG(1) FROM rpt.v_social_training_wide
UNION ALL SELECT 'counts','rpt.v_social_cards_latest5',  COUNT_BIG(1) FROM rpt.v_social_cards_latest5;

-- latest5 preview
SELECT TOP 20 'cards5' AS qa_section, v.section, v.[year], v.label, v.value_num
FROM rpt.v_social_cards_latest5 v
ORDER BY v.section, v.[year] DESC, v.label;
