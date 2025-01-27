SELECT
  title,
  AVG(duration) AS avg_watch_duration
FROM `default`.`kafka_naci`.`netflix-uk-views`
GROUP BY title;