IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'rpt')
BEGIN
    EXEC(N'CREATE SCHEMA rpt AUTHORIZATION dbo');
END;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

/* Energy PBIX dataset builder */
DECLARE @has_env_master bit = CASE WHEN OBJECT_ID(N'rpt.v_env_pbix_dataset', N'V') IS NOT NULL THEN 1 ELSE 0 END;

IF (@has_env_master = 1)
BEGIN
    EXEC(N'
    CREATE OR ALTER VIEW rpt.v_energy_pbix_dataset AS
        SELECT TRY_CONVERT(int,[year])        AS [year],
               TRY_CAST([stream] AS nvarchar(20))  AS stream,
               TRY_CAST([metric] AS nvarchar(200)) AS metric,
               TRY_CONVERT(decimal(18,6), [value_num]) AS value_num
        FROM rpt.v_env_pbix_dataset;');
END
ELSE
BEGIN
    DECLARE @parts nvarchar(max) = N'';

    DECLARE @candidates TABLE(view_name sysname, stream nvarchar(20));
    INSERT INTO @candidates(view_name, stream) VALUES
        (N'rpt.v_energy_counts_long',       N'counts'),
        (N'rpt.v_env_counts_long',          N'counts'),
        (N'rpt.v_environment_counts_long',  N'counts'),
        (N'mart.v_energy_counts_by_year',   N'counts'),
        (N'mart.v_env_counts_by_year',      N'counts'),
        (N'rpt.v_energy_rates_long',        N'rates'),
        (N'rpt.v_env_rates_long',           N'rates'),
        (N'rpt.v_energy_shares_long',       N'rates'),
        (N'mart.v_energy_rates_by_year',    N'rates'),
        (N'mart.v_env_rates_by_year',       N'rates'),
        (N'rpt.v_energy_amounts_long',      N'amounts'),
        (N'rpt.v_env_amounts_long',         N'amounts'),
        (N'rpt.v_emissions_amounts_long',   N'amounts'),
        (N'mart.v_energy_amounts_by_year',  N'amounts'),
        (N'mart.v_env_amounts_by_year',     N'amounts'),
        (N'mart.v_emissions_by_year',       N'amounts');

    DECLARE @view sysname, @stream nvarchar(20);
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR SELECT view_name, stream FROM @candidates;
    OPEN cur;
    FETCH NEXT FROM cur INTO @view, @stream;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF OBJECT_ID(@view, N'V') IS NOT NULL
        BEGIN
            DECLARE @ycol sysname, @mcol sysname, @vcol sysname;

            SELECT TOP (1) @ycol = name
            FROM sys.columns
            WHERE object_id = OBJECT_ID(@view)
              AND LOWER(name) IN (N'year', N'report_year', N'yr', N'fyear', N'cal_year')
            ORDER BY CASE LOWER(name)
                         WHEN N'year' THEN 1
                         WHEN N'report_year' THEN 2
                         WHEN N'yr' THEN 3
                         WHEN N'cal_year' THEN 4
                         ELSE 5
                     END;

            SELECT TOP (1) @mcol = name
            FROM sys.columns
            WHERE object_id = OBJECT_ID(@view)
              AND LOWER(name) IN (N'metric', N'measure', N'measure_group', N'category', N'name', N'indicator', N'kpi', N'item')
            ORDER BY CASE LOWER(name)
                         WHEN N'metric' THEN 1
                         WHEN N'measure' THEN 2
                         WHEN N'measure_group' THEN 3
                         WHEN N'category' THEN 4
                         WHEN N'name' THEN 5
                         WHEN N'indicator' THEN 6
                         WHEN N'item' THEN 7
                         ELSE 8
                     END;

            SELECT TOP (1) @vcol = name
            FROM sys.columns
            WHERE object_id = OBJECT_ID(@view)
              AND LOWER(name) IN (
                    N'value_num', N'value', N'val', N'amount', N'amount_eur',
                    N'energy_kwh', N'kwh', N'mwh', N'gwh',
                    N'emissions_tco2e', N'tco2e', N'co2e_t', N'co2e', N'emissions_t', N'tonnes_co2e', N't_co2e',
                    N'rate_val', N'rate_pct', N'share_pct', N'percent', N'rate_num'
              )
            ORDER BY CASE LOWER(name)
                         WHEN N'value_num' THEN 1
                         WHEN N'value' THEN 2
                         WHEN N'val' THEN 3
                         WHEN N'amount_eur' THEN 4
                         WHEN N'amount' THEN 5
                         WHEN N'energy_kwh' THEN 6
                         WHEN N'kwh' THEN 7
                         WHEN N'mwh' THEN 8
                         WHEN N'gwh' THEN 9
                         WHEN N'emissions_tco2e' THEN 10
                         WHEN N'tco2e' THEN 11
                         WHEN N'co2e_t' THEN 12
                         WHEN N'co2e' THEN 13
                         WHEN N'emissions_t' THEN 14
                         WHEN N'tonnes_co2e' THEN 15
                         WHEN N't_co2e' THEN 16
                         WHEN N'rate_val' THEN 17
                         WHEN N'rate_pct' THEN 18
                         WHEN N'share_pct' THEN 19
                         WHEN N'percent' THEN 20
                         ELSE 21
                     END;

            IF @ycol IS NOT NULL AND @mcol IS NOT NULL AND @vcol IS NOT NULL
            BEGIN
                SET @parts = @parts + N' UNION ALL SELECT
                        TRY_CONVERT(int,' + QUOTENAME(@ycol) + N') AS [year],
                        N''' + @stream + N''' AS stream,
                        TRY_CAST(' + QUOTENAME(@mcol) + N' AS nvarchar(200)) AS metric,
                        TRY_CONVERT(decimal(18,6), ' + QUOTENAME(@vcol) + N') AS value_num
                    FROM ' + @view;
            END
        END
        FETCH NEXT FROM cur INTO @view, @stream;
    END
    CLOSE cur;
    DEALLOCATE cur;

    DECLARE @sql nvarchar(max);
    IF LEN(@parts) > 0
    BEGIN
        SET @parts = STUFF(@parts, 1, LEN(N' UNION ALL '), N'');
        SET @sql = N'CREATE OR ALTER VIEW rpt.v_energy_pbix_dataset AS ' + @parts + N';';
    END
    ELSE
    BEGIN
        SET @sql = N'
        CREATE OR ALTER VIEW rpt.v_energy_pbix_dataset AS
            SELECT CAST(NULL AS int) AS [year],
                   CAST(NULL AS nvarchar(20)) AS stream,
                   CAST(NULL AS nvarchar(200)) AS metric,
                   CAST(NULL AS decimal(18,6)) AS value_num
            WHERE 1 = 0;';
    END
    EXEC sys.sp_executesql @sql;
END;
GO

CREATE OR ALTER VIEW rpt.v_env_pbix_dataset
AS
    SELECT [year], stream, metric, value_num
    FROM rpt.v_energy_pbix_dataset;
GO

/* Energy import catalog */
CREATE OR ALTER VIEW rpt.v_energy_import_catalog
AS
SELECT N'rpt' AS [schema],
       N'v_energy_pbix_dataset' AS view_name,
       N'energy-long' AS category,
       N'Energy/Environment long-format dataset (year, stream, metric, value_num)' AS notes
UNION ALL
SELECT N'rpt', N'v_env_pbix_dataset', N'compat', N'Compatibility alias to v_energy_pbix_dataset';
GO

CREATE OR ALTER VIEW rpt.v_env_import_catalog
AS
    SELECT * FROM rpt.v_energy_import_catalog;
GO
