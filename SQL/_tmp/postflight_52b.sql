PRINT '[Postflight] Restoring MAXDOP…';
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
PRINT '[Postflight] Restored Database-scoped MAXDOP = 0.';
GO
