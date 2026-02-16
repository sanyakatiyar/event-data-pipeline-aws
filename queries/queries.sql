-- query 1, conversion funnel (per product: view -> add_to_cart -> purchase)
WITH product_events AS (
  SELECT
    product_id,
    COUNT_IF(event_type = 'page_view')        AS views,
    COUNT_IF(event_type = 'add_to_cart')      AS adds,
    COUNT_IF(event_type = 'purchase')         AS purchases
  FROM capstone_sanyak07_db.events_parquet
  WHERE product_id IS NOT NULL
  GROUP BY product_id
)
SELECT
  product_id,
  views,
  adds,
  purchases,
  CASE WHEN views > 0 THEN adds * 1.0 / views ELSE 0 END       AS view_to_add_rate,
  CASE WHEN adds  > 0 THEN purchases * 1.0 / adds ELSE 0 END   AS add_to_purchase_rate,
  CASE WHEN views > 0 THEN purchases * 1.0 / views ELSE 0 END  AS view_to_purchase_rate
FROM product_events
ORDER BY views DESC
LIMIT 1000
;

-- query 2, hourly revenue (price * quantity for purchases)
SELECT
  year,
  month,
  day,
  hour,
  SUM(price * quantity)    AS total_revenue,
  SUM(quantity)            AS units_sold,
  COUNT(*)                 AS purchase_events
FROM capstone_sanyak07_db.events_parquet
WHERE event_type = 'purchase'
GROUP BY year, month, day, hour
ORDER BY year, month, day, hour
;

-- query 3, top 10 products by views
SELECT
  product_id,
  category,
  COUNT(*) AS view_count
FROM capstone_sanyak07_db.events_parquet
WHERE event_type = 'page_view'
  AND product_id IS NOT NULL
GROUP BY product_id, category
ORDER BY view_count DESC
LIMIT 10
;

-- query 4, category performance (daily event counts by category and type)
SELECT
  date(event_ts)                               AS event_day,
  category,
  COUNT(*)                                     AS total_events,
  COUNT_IF(event_type = 'page_view')          AS page_views,
  COUNT_IF(event_type = 'add_to_cart')        AS adds,
  COUNT_IF(event_type = 'remove_from_cart')   AS removes,
  COUNT_IF(event_type = 'purchase')           AS purchases,
  COUNT_IF(event_type = 'search')             AS searches
FROM capstone_sanyak07_db.events_parquet
WHERE category IS NOT NULL
GROUP BY date(event_ts), category
ORDER BY event_day, category
;

-- query 5, user activity (daily unique users and sessions)
SELECT
  date(event_ts)                  AS event_day,
  COUNT(DISTINCT user_id)         AS unique_users,
  COUNT(DISTINCT session_id)      AS unique_sessions,
  COUNT(*)                        AS total_events
FROM capstone_sanyak07_db.events_parquet
GROUP BY date(event_ts)
ORDER BY event_day
;

