PRINT '[Preflight] Starting…';
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 1;
GO
DECLARE @db sysname = DB_NAME();
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @db AND is_auto_update_stats_async_on = 0)
    EXEC('ALTER DATABASE [' + @db + '] SET AUTO_UPDATE_STATISTICS_ASYNC ON');
PRINT '[Preflight] MAXDOP=1, Async Stats ensured.';
GO
