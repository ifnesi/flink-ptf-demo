-- Make click_ts the event-time attribute with a small lateness allowance
ALTER TABLE `user-clicks`
  MODIFY WATERMARK FOR `click_ts` AS `click_ts`;
