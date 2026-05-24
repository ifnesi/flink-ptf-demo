-- Run the PTF on the user-clicks topic, partitioned per user_id.
-- The PTF emits (clicks_summary) after the configured timeout of event-time inactivity (Terraform variable `inactivity_timeout_seconds`).
-- Flink auto-prepends the partition key (user_id)
INSERT INTO `user-clicks-summary`
SELECT
    CAST(`user_id` AS BYTES) AS `key`,
    `user_id`,
    `detected_at`,
    `clicks_summary`
FROM inactivity_summary(
    input => TABLE `user-clicks` PARTITION BY `user_id`,
    timeoutSeconds => ${inactivity_timeout_seconds},
    on_time => DESCRIPTOR(`click_ts`),
    uid => 'user-clicks-summary-v1'
);