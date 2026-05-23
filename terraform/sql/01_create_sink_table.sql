-- Create the sink table for user-clicks-summary
CREATE TABLE `user-clicks-summary` (
  `key` BYTES,
  `user_id` VARCHAR NOT NULL,
  `detected_at` TIMESTAMP_LTZ(3) NOT NULL,
  `clicks_summary` VARCHAR NOT NULL
)
DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
);
