-- Create the sink table for user-clicks-summary
CREATE TABLE `user-clicks-summary` (
  `key` BYTES,
  `detected_at` TIMESTAMP_LTZ(3) NOT NULL,
  `total_clicks` INT NOT NULL
)
DISTRIBUTED BY (`key`)
WITH (
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
);
