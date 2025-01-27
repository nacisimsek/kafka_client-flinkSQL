SELECT
  title,
  SUBSTR(`datetime`, 1, 10) AS view_date,
  COUNT(*) AS daily_view_count,
  SUM(duration) AS daily_total_watch_time
FROM `default`.`kafka_naci`.`netflix-uk-views`
GROUP BY
  title,
  SUBSTR(`datetime`, 1, 10);