SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart;');

IF OBJECT_ID('core.energy_mix_overrides','U') IS NULL
BEGIN
    RAISERROR('Missing table core.energy_mix_overrides (create via Module 23f).',16,1);
    RETURN;
END

DECLARE @ins int = 0;
DECLARE @upd int = 0;

IF OBJECT_ID('core.v_energy_mix_other_suggestions','V') IS NOT NULL
BEGIN
    DECLARE @cand TABLE(
        pattern_norm  nvarchar(400) COLLATE SQL_Latin1_General_CP1_CI_AS PRIMARY KEY,
        energy_group  varchar(20)   NULL,
        is_renewable  bit           NULL,
        is_rate_like  bit           NULL,
        is_total_like bit           NULL,
        priority      int           NOT NULL,
        notes         nvarchar(400) NULL
    );

    INSERT INTO @cand(pattern_norm, energy_group, is_renewable, is_rate_like, is_total_like, priority, notes)
    SELECT
        s.token COLLATE SQL_Latin1_General_CP1_CI_AS AS pattern_norm,
        s.recommended_group AS energy_group,
        CASE WHEN s.token IN (
                N'pv',N'photovoltaik',N'solar',N'solarstrom',N'oekostrom',N'oeko',N'gruenstrom',
                N'green',N'hkn',N'herkunftsnachweis',N'renewable',N'renewables',
                N'solarthermie',N'biogas',N'biomethan',N'biomasse',N'holz',N'pellet'
            ) THEN 1 ELSE NULL END AS is_renewable,
        CAST(NULL AS bit) AS is_rate_like,
        CAST(NULL AS bit) AS is_total_like,
        CASE WHEN s.token IN (
                N'pv',N'photovoltaik',N'solar',N'solarstrom',N'oekostrom',N'oeko',N'gruenstrom',
                N'green',N'hkn',N'herkunftsnachweis',N'renewable',N'renewables',
                N'solarthermie',N'biogas',N'biomethan',N'biomasse',N'holz',N'pellet'
            ) THEN 65 ELSE 55 END AS priority,
        N'23j auto from suggestions' AS notes
    FROM core.v_energy_mix_other_suggestions s
    WHERE s.recommended_group IN ('Electricity','Heat','Fuels')
      AND NOT EXISTS (
          SELECT 1
          FROM core.energy_mix_overrides o
          WHERE o.pattern_norm = s.token COLLATE SQL_Latin1_General_CP1_CI_AS
      )
      AND (s.years_covered >= 2 OR s.hits >= 3 OR s.total_mwh >= 500);

    IF EXISTS (SELECT 1 FROM @cand)
    BEGIN
        UPDATE o
        SET
            o.energy_group  = COALESCE(c.energy_group, o.energy_group),
            o.is_renewable  = COALESCE(c.is_renewable, o.is_renewable),
            o.is_rate_like  = COALESCE(c.is_rate_like, o.is_rate_like),
            o.is_total_like = COALESCE(c.is_total_like, o.is_total_like),
            o.priority      = CASE WHEN c.priority > o.priority THEN c.priority ELSE o.priority END,
            o.is_active     = 1,
            o.notes         = COALESCE(o.notes, c.notes)
        FROM core.energy_mix_overrides o
        JOIN @cand c ON o.pattern_norm = c.pattern_norm
        WHERE
            (c.energy_group IS NOT NULL AND ISNULL(o.energy_group,'') <> c.energy_group)
            OR (COALESCE(o.is_renewable, -1) <> COALESCE(c.is_renewable, -1))
            OR (COALESCE(o.is_rate_like, -1) <> COALESCE(c.is_rate_like, -1))
            OR (COALESCE(o.is_total_like, -1) <> COALESCE(c.is_total_like, -1))
            OR (c.priority > o.priority)
            OR (o.is_active <> 1)
            OR (o.notes IS NULL AND c.notes IS NOT NULL);

        SET @upd = @@ROWCOUNT;

        INSERT INTO core.energy_mix_overrides(pattern_norm, energy_group, is_renewable, is_rate_like, is_total_like, priority, notes, is_active)
        SELECT c.pattern_norm, c.energy_group, c.is_renewable, c.is_rate_like, c.is_total_like, c.priority, c.notes, 1
        FROM @cand c
        WHERE NOT EXISTS (SELECT 1 FROM core.energy_mix_overrides o WHERE o.pattern_norm = c.pattern_norm);

        SET @ins = @@ROWCOUNT;

        PRINT CONCAT('23j overrides auto-applied — inserted: ', @ins, ', updated: ', @upd, '.');

        SELECT TOP (30) 'overrides_added_peek' AS section, o.override_id, o.pattern_norm, o.energy_group, o.is_renewable, o.priority
        FROM core.energy_mix_overrides o
        WHERE o.pattern_norm IN (SELECT pattern_norm FROM @cand)
        ORDER BY o.override_id DESC;
    END
    ELSE
    BEGIN
        PRINT '23j auto-apply: no eligible suggestions found.';
    END
END
ELSE
BEGIN
    PRINT 'Notice: core.v_energy_mix_other_suggestions not found; skipping auto-apply phase.';
END

SELECT 'auto_apply_counts' AS section,
       @ins AS inserted_count,
       @upd AS updated_count,
       CASE WHEN OBJECT_ID('core.v_energy_mix_other_suggestions','V') IS NULL THEN 1 ELSE 0 END AS suggestions_missing;
GO

CREATE OR ALTER VIEW mart.v_energy_mix_other_ratio
AS
SELECT
    [year],
    other_mwh = SUM(CASE WHEN energy_group = 'Other' THEN mwh ELSE 0 END),
    total_mwh = SUM(mwh),
    other_ratio = CASE WHEN SUM(mwh) > 0 THEN CAST(SUM(CASE WHEN energy_group = 'Other' THEN mwh ELSE 0 END) AS decimal(38,6)) / CAST(SUM(mwh) AS decimal(38,6)) END
FROM core.v_energy_mix_final
GROUP BY [year];
GO

CREATE OR ALTER VIEW mart.v_energy_mix_guardrails
AS
SELECT
    r.[year],
    r.other_mwh,
    r.total_mwh,
    r.other_ratio,
    threshold = CAST(0.12 AS decimal(5,4)),
    breach    = CASE WHEN r.other_ratio IS NOT NULL AND r.other_ratio > 0.12 THEN 1 ELSE 0 END
FROM mart.v_energy_mix_other_ratio r;
GO

DECLARE @max_year INT = (SELECT MAX([year]) FROM mart.v_energy_mix_other_ratio);
SELECT TOP (10) 'guardrail_recent_peek' AS section,
       r.[year], r.other_mwh, r.total_mwh, r.other_ratio,
       0.12 AS threshold,
       CASE WHEN r.other_ratio > 0.12 THEN 'BREACH' ELSE 'OK' END AS status
FROM mart.v_energy_mix_other_ratio r
WHERE r.[year] >= ISNULL(@max_year,0) - 9
ORDER BY r.[year] DESC;
