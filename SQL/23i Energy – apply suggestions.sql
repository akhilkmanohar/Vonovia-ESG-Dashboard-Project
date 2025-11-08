SET NOCOUNT ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core;');

IF OBJECT_ID('core.energy_mix_overrides','U') IS NULL
BEGIN
    RAISERROR('Missing table core.energy_mix_overrides (create via Module 23f).',16,1);
    RETURN;
END

/* Conservative override pack derived from Module 23h suggestions. */
DECLARE @pack TABLE(
    pattern_norm  nvarchar(400) NOT NULL,
    energy_group  varchar(20)   NULL,
    is_renewable  bit           NULL,
    is_rate_like  bit           NULL,
    is_total_like bit           NULL,
    priority      int           NOT NULL,
    notes         nvarchar(400) NULL
);

INSERT INTO @pack(pattern_norm, energy_group, is_renewable, is_rate_like, is_total_like, priority, notes) VALUES
(N'renewable',        'Electricity', 1, NULL, NULL, 70, N'23h suggestion: renewable -> Electricity (renewable)'),
(N'renewables',       'Electricity', 1, NULL, NULL, 65, N'Plural form'),
(N'renewable energy', 'Electricity', 1, NULL, NULL, 65, N'Phrase form');

DECLARE @changes TABLE(action nvarchar(10));

MERGE core.energy_mix_overrides AS tgt
USING @pack AS src
   ON tgt.pattern_norm = src.pattern_norm
WHEN MATCHED THEN
    UPDATE SET
        tgt.energy_group  = COALESCE(src.energy_group, tgt.energy_group),
        tgt.is_renewable  = COALESCE(src.is_renewable, tgt.is_renewable),
        tgt.is_rate_like  = COALESCE(src.is_rate_like, tgt.is_rate_like),
        tgt.is_total_like = COALESCE(src.is_total_like, tgt.is_total_like),
        tgt.priority      = CASE WHEN src.priority > tgt.priority THEN src.priority ELSE tgt.priority END,
        tgt.is_active     = 1,
        tgt.notes         = COALESCE(tgt.notes, src.notes)
WHEN NOT MATCHED BY TARGET THEN
    INSERT (pattern_norm, energy_group, is_renewable, is_rate_like, is_total_like, priority, notes, is_active)
    VALUES (src.pattern_norm, src.energy_group, src.is_renewable, src.is_rate_like, src.is_total_like, src.priority, src.notes, 1)
OUTPUT $action INTO @changes(action);

DECLARE @inserted int = (SELECT COUNT(*) FROM @changes WHERE action = 'INSERT'),
        @updated  int = (SELECT COUNT(*) FROM @changes WHERE action = 'UPDATE');

PRINT CONCAT('Overrides upserted - inserted: ', @inserted, ', updated: ', @updated, '.');
SELECT 'upsert_counts' AS section, @inserted AS inserted_count, @updated AS updated_count;

SELECT 'overrides_applied' AS section, override_id, pattern_norm, energy_group, is_renewable, priority, is_active, notes
FROM core.energy_mix_overrides
WHERE pattern_norm IN (SELECT pattern_norm FROM @pack)
ORDER BY priority DESC, override_id DESC;

DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);
SELECT TOP (20) 'hits_after_upsert' AS section, t.[year], t.row_label, t.override_id, t.override_pattern,
       t.energy_group, t.is_renewable, t.mwh
FROM core.v_energy_mix_tagged t
WHERE t.override_id IS NOT NULL AND t.[year] >= ISNULL(@max_year,0) - 9
ORDER BY t.[year] DESC, t.mwh DESC;

