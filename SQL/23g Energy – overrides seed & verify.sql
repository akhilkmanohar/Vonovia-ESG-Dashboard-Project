SET NOCOUNT ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core;');
IF OBJECT_ID('core.energy_mix_overrides','U') IS NULL
BEGIN
    RAISERROR('Missing table core.energy_mix_overrides (create by Module 23f first).', 16, 1);
    RETURN;
END

-- 1) Seed list (patterns must be LOWERCASE ASCII; map umlauts: ae->a, oe->o, ue->u, sz->ss)
--    Keep this conservative. Each row can optionally pin is_renewable; NULL means "leave to base logic".
DECLARE @seed TABLE(
    pattern_norm   nvarchar(400) NOT NULL,
    energy_group   varchar(20)   NULL,
    is_renewable   bit           NULL,
    is_rate_like   bit           NULL,
    is_total_like  bit           NULL,
    priority       int           NOT NULL DEFAULT(10),
    notes          nvarchar(400) NULL
);

-- Electricity (generic building/common electricity)
INSERT INTO @seed(pattern_norm, energy_group, is_renewable, notes, priority) VALUES
 (N'gebaeudestrom',     'Electricity', 0,  N'Building electricity catch-all', 20),
 (N'allgemeinstrom',    'Electricity', 0,  N'Common areas electricity',       20),
 (N'hausstrom',         'Electricity', 0,  N'House electricity',              20),
 (N'gemeinschaftsflaechen strom', 'Electricity', 0, N'Gemeinschaftsflaechen Strom', 20),
 (N'treppenhaus strom', 'Electricity', 0,  N'Stairwell electricity',          20);

-- Electricity (renewable)
INSERT INTO @seed(pattern_norm, energy_group, is_renewable, notes, priority) VALUES
 (N'pv',               'Electricity', 1, N'PV electricity',                   50),
 (N'photovoltaik',     'Electricity', 1, N'PV electricity',                   50),
 (N'solarstrom',       'Electricity', 1, N'Solar electricity',                50),
 (N'oekostrom',        'Electricity', 1, N'Green electricity',                60),
 (N'gruenstrom',       'Electricity', 1, N'Green electricity',                60),
 (N'green electricity','Electricity', 1, N'Green electricity',                60),
 (N'herkunftsnachweis','Electricity', 1, N'Guarantee of origin (HKN)',        60),
 (N'hkn',              'Electricity', 1, N'Guarantee of origin (HKN)',        60);

-- Heat
INSERT INTO @seed(pattern_norm, energy_group, is_renewable, notes, priority) VALUES
 (N'fernwaerme',  'Heat', NULL, N'District heat', 30),
 (N'nahwaerme',   'Heat', NULL, N'Local district heat', 30),
 (N'dampf',       'Heat', NULL, N'Steam heat', 30),
 (N'waermepumpe', 'Heat', NULL, N'Heat pump (don''t force renewable)', 25),
 (N'solarthermie','Heat', 1,    N'Solar thermal heat', 50);

-- Fuels (non-renewable)
INSERT INTO @seed(pattern_norm, energy_group, is_renewable, notes, priority) VALUES
 (N'erdgas',   'Fuels', 0, N'Natural gas', 40),
 (N'diesel',   'Fuels', 0, N'Diesel (fleet/stationary)', 40),
 (N'benzin',   'Fuels', 0, N'Petrol', 40),
 (N'petrol',   'Fuels', 0, N'Petrol EN', 40),
 (N'heizol',   'Fuels', 0, N'Heating oil', 40),
 (N'heizoel',  'Fuels', 0, N'Heating oil (alt spelling)', 40),
 (N'lpg',      'Fuels', 0, N'LPG', 40),
 (N'cng',      'Fuels', 0, N'CNG', 40),
 (N'lng',      'Fuels', 0, N'LNG', 40),
 (N'fuhrpark diesel', 'Fuels', 0, N'Fleet diesel', 45);

-- Fuels (renewable)
INSERT INTO @seed(pattern_norm, energy_group, is_renewable, notes, priority) VALUES
 (N'biogas',     'Fuels', 1, N'Biogas', 45),
 (N'biomethan',  'Fuels', 1, N'Biomethane', 45),
 (N'biomasse',   'Fuels', 1, N'Biomass', 45),
 (N'pellet',     'Fuels', 1, N'Wood pellets', 45),
 (N'holz',       'Fuels', 1, N'Wood fuel', 45);

-- 2) Upsert seeds (insert only when pattern doesn't already exist)
;WITH src AS (
    SELECT DISTINCT pattern_norm, energy_group, is_renewable, is_rate_like, is_total_like, priority, notes
    FROM @seed
),
missing AS (
    SELECT s.*
    FROM src s
    WHERE NOT EXISTS (SELECT 1 FROM core.energy_mix_overrides o WHERE o.pattern_norm = s.pattern_norm)
)
INSERT INTO core.energy_mix_overrides(pattern_norm, energy_group, is_renewable, is_rate_like, is_total_like, priority, notes)
SELECT pattern_norm, energy_group, is_renewable, is_rate_like, is_total_like, priority, notes
FROM missing;

-- 3) Report inserted count and show active overrides summary
DECLARE @inserted INT = @@ROWCOUNT;

SELECT 'overrides_active' AS section,
       COUNT(*) AS active_count,
       SUM(CASE WHEN energy_group='Electricity' THEN 1 ELSE 0 END) AS elec_rules,
       SUM(CASE WHEN energy_group='Heat' THEN 1 ELSE 0 END)        AS heat_rules,
       SUM(CASE WHEN energy_group='Fuels' THEN 1 ELSE 0 END)       AS fuel_rules
FROM core.energy_mix_overrides
WHERE is_active = 1;

SELECT TOP (50) 'overrides_sample' AS section, pattern_norm, energy_group, is_renewable, priority, notes
FROM core.energy_mix_overrides
WHERE is_active=1
ORDER BY priority DESC, override_id DESC;

-- 4) Quick effect check (latest 10y)
DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);
SELECT 'mix_recent' AS section, f.[year], f.energy_group,
       SUM(CASE WHEN f.is_renewable=1 THEN f.mwh ELSE 0 END) AS ren_mwh,
       SUM(CASE WHEN f.is_renewable=0 THEN f.mwh ELSE 0 END) AS nonren_mwh,
       SUM(f.mwh) AS total_mwh
FROM core.v_energy_mix_final f
WHERE f.[year] >= ISNULL(@max_year,0) - 9
GROUP BY f.[year], f.energy_group
ORDER BY f.[year] DESC, f.energy_group;

-- 5) Print inserted count (so the runner can surface it)
PRINT CONCAT('Seed overrides inserted: ', @inserted);
SELECT 'seed_inserted' AS section, @inserted AS inserted_count;

