ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 1;
GO
DECLARE @major int = TRY_CONVERT(int, SERVERPROPERTY('ProductMajorVersion'));
IF (@major IS NOT NULL AND @major >= 13)
BEGIN
    BEGIN TRY
        EXEC(N'ALTER DATABASE SCOPED CONFIGURATION SET AUTO_UPDATE_STATISTICS_ASYNC = ON;');
    END TRY
    BEGIN CATCH
        -- Ignore if the instance does not support async stats toggle.
    END CATCH;
END;
GO
