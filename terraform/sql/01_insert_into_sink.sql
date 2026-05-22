-- Run the PTF on the user-clicks topic, partitioned per user_id.
-- The PTF emits (detected_at, click_counts) after 10 s of event-time inactivity.
-- Flink auto-prepends the partition key (user_id) and the rowtime; we project
-- user_id into the sink's `key` column (BYTES) and drop the rowtime.
INSERT INTO `user-clicks-summary`
SELECT
    CAST(user_id AS BYTES) AS `key`,
    detected_at,
    click_counts
FROM inactivity_summary(
    input        => TABLE `user-clicks` PARTITION BY user_id,
    timeout_secs => 10,
    on_time      => DESCRIPTOR(`$rowtime`),
    uid          => 'inactivity-summary-v1'
);
