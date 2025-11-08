SET NOCOUNT ON;
PRINT ''[Preflight] Starting…'';
BEGIN TRY
    ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 1;
    PRINT ''[Preflight] Database-scoped MAXDOP = 1.'';
END TRY BEGIN CATCH
    PRINT CONCAT(''[Preflight] MAXDOP set failed: '', ERROR_MESSAGE());
END CATCH;
BEGIN TRY
    ALTER DATABASE SCOPED CONFIGURATION SET AUTO_UPDATE_STATISTICS_ASYNC = ON;
    PRINT ''[Preflight] AUTO_UPDATE_STATISTICS_ASYNC = ON.'';
END TRY BEGIN CATCH
    PRINT CONCAT(''[Preflight] Async stats set failed: '', ERROR_MESSAGE());
END CATCH;

IF OBJECT_ID(''tempdb..#kill'') IS NOT NULL DROP TABLE #kill;
WITH blockers AS (
    SELECT DISTINCT blocking_session_id AS spid
    FROM sys.dm_exec_requests
    WHERE blocking_session_id <> 0
),
killable AS (
    SELECT b.spid
    FROM blockers b
    JOIN sys.dm_exec_sessions s ON s.session_id = b.spid
    WHERE s.is_user_process = 1 AND s.status = ''sleeping'' AND s.open_transaction_count = 0
)
SELECT spid INTO #kill FROM killable;

DECLARE @spid int;
WHILE EXISTS (SELECT 1 FROM #kill)
BEGIN
    SELECT TOP (1) @spid = spid FROM #kill ORDER BY spid;
    BEGIN TRY
        EXEC(''KILL ''+CAST(@spid AS nvarchar(10)));
        PRINT CONCAT(''[Preflight] KILLED sleeping blocker spid='', @spid);
    END TRY
    BEGIN CATCH
        PRINT CONCAT(''[Preflight] Kill failed for spid='', @spid, '' -> '', ERROR_MESSAGE());
    END CATCH;
    DELETE FROM #kill WHERE spid=@spid;
END
PRINT ''[Preflight] Complete.'';
