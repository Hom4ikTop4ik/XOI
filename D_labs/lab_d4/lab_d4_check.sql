-- Процент покрытия будущих рейсов
SELECT 
    COUNT(DISTINCT flight_id) AS total_future_flights,
    COUNT(DISTINCT CASE WHEN recommended_price IS NOT NULL THEN flight_id END) AS flights_with_price,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN recommended_price IS NOT NULL THEN flight_id END) / 
          COUNT(DISTINCT flight_id), 2) AS flight_coverage_percent
FROM lab_d4_upcoming_flight_prices;

--  total_future_flights | flights_with_price | flight_coverage_percent
-- ----------------------+--------------------+-------------------------
--                 10650 |              10650 |                  100.00



-- Распределение правил по приоритетам
SELECT 
    rule_priority,
    COUNT(*) AS usage_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS usage_percent
FROM lab_d4_upcoming_flight_prices
WHERE recommended_price IS NOT NULL
GROUP BY rule_priority
ORDER BY rule_priority;

--  rule_priority | usage_count | usage_percent
-- ---------------+-------------+---------------
--              1 |        9666 |         30.25
--              2 |        9117 |         28.54
--              3 |       13167 |         41.21



-- Цены по классам
SELECT 
    fare_conditions,
    COUNT(*) AS total,
    MIN(recommended_price) AS min_price,
    ROUND(AVG(recommended_price)::NUMERIC, 2) AS avg_price,
    MAX(recommended_price) AS max_price
FROM lab_d4_final_price_list
GROUP BY fare_conditions
ORDER BY fare_conditions;

--  fare_conditions | total | min_price | avg_price | max_price
-- -----------------+-------+-----------+-----------+-----------
--  Business        | 10650 |   3500.00 |  10181.57 |  33500.00
--  Comfort         | 10650 |   2275.00 |   6323.26 |  18525.00
--  Economy         | 10650 |   1750.00 |   5090.79 |  16750.00
