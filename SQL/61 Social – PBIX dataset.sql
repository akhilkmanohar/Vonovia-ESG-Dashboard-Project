IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'rpt')
BEGIN
    EXEC(N'CREATE SCHEMA rpt AUTHORIZATION dbo');
END;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

/* Build or alias Social PBIX dataset */
DECLARE @has_social_master bit = CASE WHEN OBJECT_ID(N'rpt.v_social_pbix_dataset', N'V') IS NOT NULL THEN 1 ELSE 0 END;

IF (@has_social_master = 1)
BEGIN
    EXEC(N'
    CREATE OR ALTER VIEW rpt.v_soc_pbix_dataset AS
        SELECT TRY_CONVERT(int,[year]) AS [year],
               TRY_CAST([stream] AS nvarchar(20)) AS stream,
               TRY_CAST([metric] AS nvarchar(200)) AS metric,
               TRY_CONVERT(decimal(18,6), [value_num]) AS value_num
        FROM rpt.v_social_pbix_dataset;');
END
ELSE
BEGIN
    DECLARE @parts nvarchar(max) = N'';

    DECLARE @candidates TABLE(view_name sysname, stream nvarchar(20));
    INSERT INTO @candidates(view_name, stream) VALUES
        (N'rpt.v_social_counts_long',     N'counts'),
        (N'rpt.v_soc_counts_long',        N'counts'),
        (N'mart.v_social_counts_by_year', N'counts'),
        (N'mart.v_soc_counts_by_year',    N'counts'),
        (N'rpt.v_social_rates_long',      N'rates'),
        (N'rpt.v_soc_rates_long',         N'rates'),
        (N'mart.v_social_rates_by_year',  N'rates'),
        (N'mart.v_soc_rates_by_year',     N'rates'),
        (N'rpt.v_social_amounts_long',    N'amounts'),
        (N'rpt.v_soc_amounts_long',       N'amounts'),
        (N'mart.v_social_amounts_by_year',N'amounts'),
        (N'mart.v_soc_amounts_by_year',   N'amounts');

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
              AND LOWER(name) IN (N'year', N'report_year', N'yr', N'fyear')
            ORDER BY CASE LOWER(name)
                         WHEN N'year' THEN 1
                         WHEN N'report_year' THEN 2
                         WHEN N'yr' THEN 3
                         ELSE 4
                     END;

            SELECT TOP (1) @mcol = name
            FROM sys.columns
            WHERE object_id = OBJECT_ID(@view)
              AND LOWER(name) IN (N'metric', N'measure', N'measure_group', N'category', N'name', N'indicator', N'kpi')
            ORDER BY CASE LOWER(name)
                         WHEN N'metric' THEN 1
                         WHEN N'measure' THEN 2
                         WHEN N'measure_group' THEN 3
                         WHEN N'category' THEN 4
                         WHEN N'name' THEN 5
                         WHEN N'indicator' THEN 6
                         ELSE 7
                     END;

            SELECT TOP (1) @vcol = name
            FROM sys.columns
            WHERE object_id = OBJECT_ID(@view)
              AND LOWER(name) IN (N'value_num', N'value', N'val', N'amount_eur', N'count_val', N'rate_val', N'rate_pct', N'rate_num')
            ORDER BY CASE LOWER(name)
                         WHEN N'value_num' THEN 1
                         WHEN N'value' THEN 2
                         WHEN N'val' THEN 3
                         WHEN N'amount_eur' THEN 4
                         WHEN N'count_val' THEN 5
                         WHEN N'rate_val' THEN 6
                         WHEN N'rate_pct' THEN 7
                         ELSE 8
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
        SET @sql = N'CREATE OR ALTER VIEW rpt.v_soc_pbix_dataset AS ' + @parts + N';';
    END
    ELSE
    BEGIN
        SET @sql = N'
        CREATE OR ALTER VIEW rpt.v_soc_pbix_dataset AS
            SELECT CAST(NULL AS int) AS [year],
                   CAST(NULL AS nvarchar(20)) AS stream,
                   CAST(NULL AS nvarchar(200)) AS metric,
                   CAST(NULL AS decimal(18,6)) AS value_num
            WHERE 1 = 0;';
    END
    EXEC sys.sp_executesql @sql;
END;
GO

CREATE OR ALTER VIEW rpt.v_social_pbix_dataset
AS
    SELECT [year], stream, metric, value_num
    FROM rpt.v_soc_pbix_dataset;
GO

/* Social import catalog */
CREATE OR ALTER VIEW rpt.v_soc_import_catalog
AS
SELECT N'rpt' AS [schema],
       N'v_soc_pbix_dataset' AS view_name,
       N'social-long' AS category,
       N'Social long-format dataset (year, stream, metric, value_num)' AS notes
UNION ALL
SELECT N'rpt', N'v_social_pbix_dataset', N'compat', N'Compatibility alias to v_soc_pbix_dataset';
GO

CREATE OR ALTER VIEW rpt.v_social_import_catalog
AS
    SELECT * FROM rpt.v_soc_import_catalog;
GO
