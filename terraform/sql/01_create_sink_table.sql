-- Create the sink table for user-clicks-summary
CREATE TABLE `user-clicks-summary` (
  `key` BYTES,
  `detected_at` TIMESTAMP_LTZ(3) NOT NULL,
  `click_counts` ARRAY<ROW<
    product_id STRING,
    product_name STRING,
    `count` INT
  > > NOT NULL
)
DISTRIBUTED BY (`key`)
WITH (
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
);
