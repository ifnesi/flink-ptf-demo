-- Run the PTF on the user-clicks topic, partitioned per user_id.
-- The PTF emits (total_clicks) after the configured timeout of event-time inactivity.
-- Flink auto-prepends the partition key (user_id) and the rowtime; we project
-- user_id into the sink's `key` column (BYTES) and use $rowtime as detected_at.
INSERT INTO `user-clicks-summary`
SELECT
    CAST(`user_id` AS BYTES) AS `key`,
    CURRENT_TIMESTAMP AS `detected_at`,
    `total_clicks`
FROM inactivity_summary(
    input => TABLE `user-clicks` PARTITION BY `user_id`,
    on_time => DESCRIPTOR(`click_ts`),
    uid => 'user-clicks-summary-v1'
);
