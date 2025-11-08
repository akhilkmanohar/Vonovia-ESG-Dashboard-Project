EXEC rpt.sp_refresh_governance_reporting;
;WITH c AS (SELECT COUNT_BIG(*) AS n FROM rpt.v_gov_counts_long),
       r AS (SELECT COUNT_BIG(*) AS n FROM rpt.v_gov_rates_long),
       a AS (SELECT COUNT_BIG(*) AS n FROM rpt.v_gov_amounts_long)
SELECT 'counts' AS stream, n FROM c
UNION ALL SELECT 'rates', n FROM r
UNION ALL SELECT 'amounts', n FROM a
OPTION (MAXDOP 1);
GO
