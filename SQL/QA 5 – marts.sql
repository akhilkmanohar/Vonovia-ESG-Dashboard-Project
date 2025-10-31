USE [Vonovia_ESG_DB];
GO
SELECT * FROM mart.v_ghg_total_by_year ORDER BY [year] DESC;
SELECT * FROM mart.v_ghg_by_country_year ORDER BY [year] DESC, iso2;
SELECT * FROM mart.v_ghg_by_scope_year ORDER BY [year] DESC, scope;
GO
