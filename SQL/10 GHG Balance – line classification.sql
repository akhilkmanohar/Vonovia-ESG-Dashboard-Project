USE [Vonovia_ESG_DB];
GO
/* File: 10 GHG Balance – line classification.sql
   Purpose:
     - Map GHG Balance row labels to Scope (1/2/3) and Segment (Portfolio / Business operations)
     - Flag totals and subtotals (e.g., "of which..." lines)
     - Produce a tagged view for marts/Power BI
   Depends on:
     - core.v_ghg_balance_yearly  (already built)
*/

------------------------------------------------------------
-- 1) Rules table (idempotent)
------------------------------------------------------------
IF OBJECT_ID('core.dim_ghg_line_map','U') IS NULL
BEGIN
  CREATE TABLE core.dim_ghg_line_map(
    applies_order int NOT NULL,              -- lower = higher priority
    label_pattern nvarchar(400) NOT NULL,    -- LIKE pattern applied to label
    scope tinyint NULL,                      -- 1,2,3 if known
    segment nvarchar(40) NULL,               -- 'Portfolio' | 'Business operations'
    is_total bit NOT NULL DEFAULT 0,
    is_subtotal bit NOT NULL DEFAULT 0,
    notes nvarchar(200) NULL,
    CONSTRAINT PK_core_dim_ghg_line_map PRIMARY KEY (applies_order, label_pattern)
  );
END;
GO

-- Wipe and seed cleanly (adjust later as we learn more labels)
TRUNCATE TABLE core.dim_ghg_line_map;
GO

------------------------------------------------------------
-- 2) Seed rules (ordered)
------------------------------------------------------------

-- A) Grand total
INSERT INTO core.dim_ghg_line_map(applies_order,label_pattern,scope,segment,is_total,is_subtotal,notes) VALUES
(10, N'%Total portfolio + business operations%', NULL, NULL, 1, 0, N'Grand total');

-- B) Subtotals by segment
INSERT INTO core.dim_ghg_line_map VALUES
(20, N'of which emissions from portfolio%',            NULL, N'Portfolio',             0, 1, N'Subtotal portfolio'),
(30, N'of which emissions from  business operations%', NULL, N'Business operations',   0, 1, N'Subtotal operations');

-- C) Scope hints (liberal patterns; refine later as needed)
INSERT INTO core.dim_ghg_line_map VALUES
-- Scope 1 (direct combustion/process on-site)
(100, N'%combustion%on-site%',                         1, NULL, 0, 0, N'Scope 1 on-site combustion'),
(110, N'%from natural gas (ME)%',                      1, NULL, 0, 0, N'Scope 1 natural gas'),
(120, N'%from fuel oil (ME)%',                         1, NULL, 0, 0, N'Scope 1 fuel oil'),
-- Scope 2 (purchased electricity/heat/steam)
(200, N'%purchased electricity% (ME)%',                2, NULL, 0, 0, N'Scope 2 electricity'),
(210, N'%purchased heat% (ME)%',                       2, NULL, 0, 0, N'Scope 2 heat'),
(220, N'%purchased steam% (ME)%',                      2, NULL, 0, 0, N'Scope 2 steam'),
-- Scope 3 (catch common phrasing if present)
(300, N'%upstream%',                                   3, NULL, 0, 0, N'Scope 3 upstream'),
(310, N'%downstream%',                                 3, NULL, 0, 0, N'Scope 3 downstream');

GO

------------------------------------------------------------
-- 3) Tagged view: first matching rule wins by applies_order
------------------------------------------------------------
CREATE OR ALTER VIEW core.v_ghg_balance_tagged
AS
WITH rules AS (
  SELECT * FROM core.dim_ghg_line_map
),
matchy AS (
  SELECT
    y.year_label, y.[year], y.row_num, y.label, y.derived_unit AS unit, y.value_num,
    r.applies_order, r.scope, r.segment, r.is_total, r.is_subtotal
  FROM core.v_ghg_balance_yearly y
  OUTER APPLY (
      SELECT TOP (1) r.*
      FROM rules r
      WHERE y.label LIKE r.label_pattern
      ORDER BY r.applies_order
  ) r
)
SELECT
  year_label, [year], row_num, label, unit, value_num,
  scope, segment,
  ISNULL(is_total,0)    AS is_total,
  ISNULL(is_subtotal,0) AS is_subtotal
FROM matchy;
GO
