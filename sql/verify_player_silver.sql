-- 1. Every validated Bronze player should appear once in Silver.
SELECT
  (SELECT COUNT(*) FROM ai_dev_class_demo.bronze_layer.player_master) AS bronze_players,
  (SELECT COUNT(*) FROM ai_dev_class_demo.silver_layer.player_silver) AS silver_players;

-- 2. Silver player keys must be unique (expect zero rows).
SELECT player_id, COUNT(*) AS occurrences
FROM ai_dev_class_demo.silver_layer.player_silver
GROUP BY player_id
HAVING COUNT(*) > 1;

-- 3. Every non-null Silver region reference must resolve (expect zero rows).
SELECT s.player_id, s.home_region_id
FROM ai_dev_class_demo.silver_layer.player_silver s
LEFT JOIN ai_dev_class_demo.bronze_layer.regions r
  ON s.home_region_id = r.id
WHERE s.home_region_id IS NOT NULL AND r.id IS NULL;

-- 4. Examine enriched players.
SELECT player_id, first_name, last_name, home_region_id,
       country_code, region_name, state_province_code
FROM ai_dev_class_demo.silver_layer.player_silver
LIMIT 20;

-- 5. Review DQ logs (typically empty for this clean tutorial).
SELECT * FROM ai_dev_class_demo.silver_layer.player_master_dq_issues
ORDER BY run_timestamp DESC LIMIT 20;
SELECT * FROM ai_dev_class_demo.silver_layer.regions_dq_issues
ORDER BY run_timestamp DESC LIMIT 20;

-- 6. Confirm exact Silver column names, order, and types.
DESCRIBE TABLE ai_dev_class_demo.silver_layer.player_silver;

-- 7. Null home regions retain players with null enrichment (expect zero rows).
SELECT player_id, home_region_id, country_code, region_type, region_name,
       state_province_code, population, latitude, longitude
FROM ai_dev_class_demo.silver_layer.player_silver
WHERE home_region_id IS NULL
  AND (country_code IS NOT NULL OR region_type IS NOT NULL
       OR region_name IS NOT NULL OR state_province_code IS NOT NULL
       OR population IS NOT NULL OR latitude IS NOT NULL OR longitude IS NOT NULL);

-- 8. Published country codes must match the referenced region (expect zero rows).
SELECT s.player_id, s.country_code, r.country_code AS region_country_code
FROM ai_dev_class_demo.silver_layer.player_silver s
JOIN ai_dev_class_demo.bronze_layer.regions r ON s.home_region_id = r.id
WHERE NOT (s.country_code <=> r.country_code);
