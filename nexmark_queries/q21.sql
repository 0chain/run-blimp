-- Nexmark q21 (add channel id): bids of the four named channels or carrying a channel_id, with the id
SELECT auction, bidder, price, channel,
  CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3'
       ELSE regexp_extract(url, '(&|^)channel_id=([^&]*)', 2) END AS channel_id, dateTime
FROM bid WHERE regexp_extract(url, '(&|^)channel_id=([^&]*)', 2) <> '' OR lower(channel) IN ('apple','google','facebook','baidu')
ORDER BY auction DESC, bidder, price, dateTime LIMIT 100;
