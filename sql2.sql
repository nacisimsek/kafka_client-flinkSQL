SELECT
  title,
  DATE_FORMAT(datetime, 'yyyy-MM-dd') AS date_day,
  COUNT(*) AS daily_view_count,
  SUM(duration) AS daily_total_watch_time
FROM `default`.`kafka_naci`.`netflix-uk-views`
  WHERE `duration` > 0
GROUP BY
  title,DATE_FORMAT(datetime, 'yyyy-MM-dd');