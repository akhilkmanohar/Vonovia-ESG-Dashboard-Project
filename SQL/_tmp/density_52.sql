SET NOCOUNT ON;
SELECT 'counts' AS stream, [year], COUNT_BIG(*) AS rows FROM rpt.v_gov_counts_long  GROUP BY [year]
UNION ALL
SELECT 'rates',  [year], COUNT_BIG(*)           FROM rpt.v_gov_rates_long   GROUP BY [year]
UNION ALL
SELECT 'amounts',[year], COUNT_BIG(*)           FROM rpt.v_gov_amounts_long GROUP BY [year]
ORDER BY stream, [year] DESC;
