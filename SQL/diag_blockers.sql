SET NOCOUNT ON;

;WITH req AS (
  SELECT r.session_id, r.blocking_session_id, r.status, r.wait_type, r.wait_time,
         r.cpu_time, r.total_elapsed_time, DB_NAME(r.database_id) AS dbname,
         r.command, r.start_time, s.host_name, s.program_name, s.login_name
  FROM sys.dm_exec_requests r
  JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id
  WHERE r.session_id <> @@SPID AND r.database_id = DB_ID()
)
SELECT 'requests' AS section, GETDATE() AS captured_at, *
FROM req
ORDER BY blocking_session_id DESC, total_elapsed_time DESC;

;WITH blockers AS (
  SELECT blocking_session_id AS blocker, COUNT(*) AS victims, MAX(total_elapsed_time) AS max_ms
  FROM sys.dm_exec_requests
  WHERE blocking_session_id <> 0 AND database_id = DB_ID()
  GROUP BY blocking_session_id
)
SELECT 'blockers' AS section, *
FROM blockers
ORDER BY victims DESC, max_ms DESC;

PRINT 'Blocker diagnostic (no auto-kill) complete.';
