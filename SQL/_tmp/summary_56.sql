SET NOCOUNT ON;

SELECT 'pillars' AS metric,
       COUNT(*)  AS val
FROM rpt.v_dim_pillar
UNION ALL
SELECT 'streams',
       COUNT(*)
FROM rpt.v_dim_stream
UNION ALL
SELECT 'cards_last5_rows',
       COUNT_BIG(*)
FROM rpt.v_esg_cards_latest_and_last5
WHERE section = 'last5'
UNION ALL
SELECT 'cards_latest_rows',
       COUNT_BIG(*)
FROM rpt.v_esg_cards_latest_and_last5
WHERE section = 'latest_by_stream';
