USE [Vonovia_ESG_DB];
GO

-- Kick the orchestrator
EXEC mart.sp_refresh_all;

-- Quick counts after refresh
SELECT 'ghg_balance_yearly' AS table_name, COUNT(*) AS rows_cnt FROM core.ghg_balance_yearly
UNION ALL
SELECT 'ghg_balance_yearly_country', COUNT(*) FROM core.ghg_balance_yearly_country;

-- Validate mart views return rows
SELECT TOP (10) * FROM mart.v_ghg_total_by_year ORDER BY [year] DESC;
SELECT TOP (10) * FROM mart.v_ghg_by_country_year ORDER BY [year] DESC, iso2;
SELECT TOP (10) * FROM mart.v_ghg_by_scope_year ORDER BY [year] DESC, scope;
GO
