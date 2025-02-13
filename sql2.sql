SELECT
    window_start,
    window_end,
    title,
    COUNT(*) AS daily_view_count,
    SUM(duration) AS daily_total_watch_time
FROM TABLE (
    TUMBLE(TABLE `default`.`kafka_naci`.`netflix-uk-views`, DESCRIPTOR(datetime), INTERVAL '1' DAY)
)
GROUP BY window_start, window_end, title;