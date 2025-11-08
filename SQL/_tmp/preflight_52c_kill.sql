SET NOCOUNT ON;
DECLARE @k TABLE(spid int PRIMARY KEY);
INSERT INTO @k(spid)
SELECT DISTINCT r.blocking_session_id
FROM sys.dm_exec_requests r
WHERE r.blocking_session_id <> 0;

DECLARE @spid int;
WHILE EXISTS (SELECT 1 FROM @k)
BEGIN
    SELECT TOP (1) @spid = spid FROM @k ORDER BY spid;
    BEGIN TRY
        DECLARE @status nvarchar(60), @isuser bit, @opentrans int;
        SELECT @status = s.status, @isuser = s.is_user_process, @opentrans = s.open_transaction_count
        FROM sys.dm_exec_sessions s WHERE s.session_id = @spid;

        IF @isuser = 1 AND @opentrans = 0 AND @status = N''sleeping''
        BEGIN
            EXEC (N''KILL '' + CAST(@spid AS nvarchar(10)));
            PRINT CONCAT(''[Preflight] KILLED sleeping blocker spid='', @spid);
        END
    END TRY
    BEGIN CATCH
        PRINT CONCAT(''[Preflight] Kill failed for spid='', @spid, '' -> '', ERROR_MESSAGE());
    END CATCH;
    DELETE FROM @k WHERE spid = @spid;
END
GO
