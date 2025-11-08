SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core;');
GO

/* 1) Base: rows still landing in Other after overrides */
CREATE OR ALTER VIEW core.v_energy_mix_other_base
AS
SELECT
    t.[year],
    t.row_label,
    t.mwh,
    rl_norm = LOWER(
                REPLACE(REPLACE(REPLACE(REPLACE(
                    CONVERT(nvarchar(4000), t.row_label) COLLATE Latin1_General_100_CI_AI,
                    NCHAR(228), N'a'), NCHAR(246), N'o'), NCHAR(252), N'u'), NCHAR(223), N'ss')
              )
FROM core.v_energy_mix_tagged t
WHERE t.energy_group = 'Other'
  AND t.is_rate_like = 0
  AND t.is_total_like = 0
  AND t.mwh > 0
  AND t.row_label IS NOT NULL
  AND LTRIM(RTRIM(t.row_label)) <> '';
GO

/* 2) Token frequency + impact */
CREATE OR ALTER VIEW core.v_energy_mix_other_tokens
AS
WITH cleaned AS (
    SELECT
        b.[year],
        b.mwh,
        txt = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                  b.rl_norm,
                  '.', ' '), ',', ' '), ';', ' '), ':', ' '), '/', ' '), '\\', ' '), '-', ' '), '(', ' '), ')', ' '), '%', ' ')
    FROM core.v_energy_mix_other_base b
), split AS (
    SELECT c.[year], c.mwh, LTRIM(RTRIM(s.value)) AS token
    FROM cleaned c
    CROSS APPLY STRING_SPLIT(c.txt, ' ') s
)
SELECT
    token,
    years_covered = COUNT(DISTINCT [year]),
    hits          = COUNT_BIG(*),
    total_mwh     = SUM(CAST(mwh AS decimal(38,6))),
    avg_mwh       = AVG(CAST(mwh AS decimal(38,6))),
    last_year     = MAX([year])
FROM split
WHERE token IS NOT NULL
  AND token <> ''
  AND LEN(token) >= 3
  AND token NOT LIKE '%[0-9]%'
  AND token NOT IN (
      'und','oder','der','die','das','ein','eine','ist','von','vom','zum','zur','im','in','am','an','auf','mit','ohne',
      'gesamt','summe','gesamtverbrauch','total','share','anteil','quote','rate','struktur','mix','verbrauch','energie','energy',
      'energies','environment','card','year','years','latest','bericht','report','gesamtenergie','overview','summary',
      'unit','units','wert','sales','sal','generation','number','efficiency','standard','standards','mwh','mwp'
  )
GROUP BY token;
GO

/* 3) Heuristic suggestions per token */
CREATE OR ALTER VIEW core.v_energy_mix_other_suggestions
AS
SELECT
    t.token,
    t.years_covered,
    t.hits,
    t.total_mwh,
    t.avg_mwh,
    t.last_year,
    recommended_group = CASE
        WHEN t.token IN ('pv','photovoltaik','solar','solarstrom','oekostrom','oeko','gruenstrom','green','wind','windstrom','strom','strombezug','netzstrom','power','electric','electricity','renewable','erneuerbar','erneuerbare','erneuerbaren') THEN 'Electricity'
        WHEN t.token LIKE '%strom%' OR t.token LIKE '%elektr%' OR t.token LIKE '%electric%' OR t.token LIKE '%power%' OR t.token LIKE '%netzstrom%' OR t.token LIKE '%renewable%' THEN 'Electricity'
        WHEN t.token IN ('fernwaerme','fernwarme','nahwaerme','nahwarme','waerme','warme','dampf','steam','heizung','heizwaerme','heizwarme','waermepumpe','warmpumpe','solarthermie','heatpump','district','heating') THEN 'Heat'
        WHEN t.token LIKE '%waerme%' OR t.token LIKE '%warme%' OR t.token LIKE '%heiz%' OR t.token LIKE '%steam%' THEN 'Heat'
        WHEN t.token IN ('erdgas','gas','erdgasversorgung','diesel','benzin','petrol','heizol','heizoel','lpg','cng','lng','biogas','biomethan','biomasse','holz','pellet','kraftstoff','fuhrpark','fleet','brennstoff') THEN 'Fuels'
        WHEN t.token LIKE '%gas%' OR t.token LIKE '%diesel%' OR t.token LIKE '%benzin%' OR t.token LIKE '%petrol%' OR t.token LIKE '%fuel%' THEN 'Fuels'
        ELSE 'Other'
    END
FROM core.v_energy_mix_other_tokens t;
GO

/* 4) Preview top tokens */
SELECT TOP (50)
    token,
    total_mwh,
    hits,
    years_covered,
    recommended_group
FROM core.v_energy_mix_other_suggestions
ORDER BY total_mwh DESC, hits DESC;
GO
