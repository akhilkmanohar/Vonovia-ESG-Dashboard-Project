SET NOCOUNT ON;

-- Run the wrapper (returns messages + counts)
EXEC mart.sp_refresh_reporting_all;

-- Latest year sample checks (single-thread to avoid stalls)
SET LOCK_TIMEOUT 10000;  -- 10s
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1 'latest_workforce' AS section, * FROM rpt.v_social_workforce_wide ORDER BY [year] DESC OPTION (MAXDOP 1);
SELECT TOP 1 'latest_hs_counts' AS section,  * FROM rpt.v_social_hs_counts_wide  ORDER BY [year] DESC OPTION (MAXDOP 1);
SELECT TOP 1 'latest_hs_rates'  AS section,  * FROM rpt.v_social_hs_rates_wide   ORDER BY [year] DESC OPTION (MAXDOP 1);
SELECT TOP 1 'latest_training'  AS section,  * FROM rpt.v_social_training_wide   ORDER BY [year] DESC OPTION (MAXDOP 1);
SELECT TOP 10 'cards5' AS qa_section, v.section, v.[year], v.label, v.value_num
FROM rpt.v_social_cards_latest5 v
ORDER BY v.section, v.[year] DESC OPTION (MAXDOP 1);
