-- Шаг 1: таблица с восстановленной ценовой информацией для каждого прошедшего рейса
CREATE TABLE IF NOT EXISTS lab_d4_historical_flight_prices AS
SELECT 
    f.flight_id,           -- номер рейса
    f.route_no,            -- номер маршрута
    r.departure_airport,   -- аэропорт вылета (три буковы)
    r.arrival_airport,     -- аэропорт прибытия (также три буковы)
    r.airplane_code,       -- код самолёта
    s.fare_conditions,     -- класс (эконом, комфорт или бизнес)
    f.scheduled_departure, -- плановая дата вылета
    COUNT(*) AS tickets_sold,   -- кол-во ПРОДАННЫХ билетов на конкретный класс
    MIN(s.price) AS min_price,  -- цены: мин, средняя, медианная, макс
    ROUND(AVG(s.price)::NUMERIC, 2) AS avg_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.price) AS median_price,
    MAX(s.price) AS max_price
FROM bookings.segments s  -- таблица перелётов, содержит цену и класс для каждого билета на рейс
JOIN bookings.flights f ON s.flight_id = f.flight_id -- присоединяем таблицу рейсов, чтобы узнать статус и дату рейса
JOIN bookings.routes r ON r.route_no = f.route_no    -- присоединяем таблицу маршрутов
    AND r.validity @> f.scheduled_departure -- проверка: дата рейса соответсвует дате конкретного маршрута (маршруты меняются иногда: без правила 16166 рейсов, с правилом 28132 - убрали дубликаты в данных)
WHERE f.scheduled_departure < bookings.now()  -- только прошедшие рейсы
    AND f.status <> 'Cancelled'  -- исключаем отменённые рейсы
    AND s.price IS NOT NULL      -- и если цена есть
GROUP BY f.flight_id, f.route_no, r.departure_airport, r.arrival_airport, 
         r.airplane_code, s.fare_conditions, f.scheduled_departure  -- Группируем: каждому уникальному сочетанию (рейс + класс) соответствует одна строка в результате 
ORDER BY f.flight_id, s.fare_conditions;

-- Проверяем, что получилось
SELECT COUNT(*) FROM lab_d4_historical_flight_prices;
SELECT * FROM lab_d4_historical_flight_prices LIMIT 10;





-- Шаг 2: таблица вместимости классов для каждого самолёта
CREATE TABLE IF NOT EXISTS lab_d4_class_capacity AS
SELECT 
    airplane_code,   -- код самолёта
    fare_conditions, -- класс (экогном, комфорт или бизнес)
    COUNT(*) AS capacity -- вместимость, собственно
FROM bookings.seats
GROUP BY airplane_code, fare_conditions
ORDER BY airplane_code, fare_conditions;

-- Проверяем
SELECT * FROM lab_d4_class_capacity LIMIT 25; -- всего строк вышло 20





-- Шаг 3: создадим правила для отбора (3 правила, по приоритетам)
CREATE TABLE IF NOT EXISTS lab_d4_pricing_rules (
    rule_id SERIAL PRIMARY KEY, -- ID правила как ключ
    priority INTEGER NOT NULL,  -- приоритет  числом: 1 = самый точный, 3 = самый общий
    rule_type TEXT NOT NULL,    -- приоритет текстом: 'route_airplane_class', 'airport_pair_class', 'global_class'
    route_no TEXT,              -- номер маршрута (для правила 1)
    departure_airport CHAR(3),  -- код аэропорта вылета (правила 1 и 2)
    arrival_airport CHAR(3),    -- код аэропорта прибытия (правила 1 и 2)
    airplane_code CHAR(3),      -- код самолёта (правило 1)
    fare_conditions TEXT NOT NULL, -- класс обслуживания (правила 1-2-3)
    recommended_price NUMERIC(10,2), -- предсказываемая цена
    min_price NUMERIC(10,2),
    max_price NUMERIC(10,2),
    sample_count INTEGER,    -- кол-во проданных билетов 
    sample_flights INTEGER,  -- кол-во уникальных рейсов в выборке (будет использовать для статистически значимх полётов `кол-во >= 3`)
    created_at TIMESTAMPTZ DEFAULT bookings.now()
);

-- Правило 1: конкретный маршрут + самолёт + класс (высший приоритет)
INSERT INTO lab_d4_pricing_rules (priority, rule_type, route_no, departure_airport, arrival_airport, 
                                   airplane_code, fare_conditions, recommended_price, 
                                   min_price, max_price, sample_count, sample_flights)
SELECT 
    1 AS priority,
    'route_airplane_class' AS rule_type,
    route_no,
    MIN(departure_airport) AS departure_airport,
    MIN(arrival_airport) AS arrival_airport,
    airplane_code,
    fare_conditions,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_price) AS recommended_price,
    MIN(min_price) AS min_price,
    MAX(max_price) AS max_price,
    SUM(tickets_sold) AS sample_count,
    COUNT(DISTINCT flight_id) AS sample_flights
FROM lab_d4_historical_flight_prices
GROUP BY route_no, airplane_code, fare_conditions
HAVING COUNT(DISTINCT flight_id) >= 3;  -- 3+ разных рейсов, статистическая значимость 

-- Правило 2: аэропорт_отправки-аэропорт_прибытия + класс (без учёта самолёта и маршрута) (средний приоритет)
INSERT INTO lab_d4_pricing_rules (priority, rule_type, route_no, departure_airport, arrival_airport, 
                                   airplane_code, fare_conditions, recommended_price, 
                                   min_price, max_price, sample_count, sample_flights)
SELECT 
    2 AS priority,
    'airport_pair_class' AS rule_type,
    NULL AS route_no,
    departure_airport,
    arrival_airport,
    NULL AS airplane_code,
    fare_conditions,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_price) AS recommended_price,
    MIN(min_price) AS min_price,
    MAX(max_price) AS max_price,
    SUM(tickets_sold) AS sample_count,
    COUNT(DISTINCT flight_id) AS sample_flights
FROM lab_d4_historical_flight_prices
GROUP BY departure_airport, arrival_airport, fare_conditions
HAVING COUNT(DISTINCT flight_id) >= 3;

-- Правило 3: общее правило по классу (наименьший приоритет)
INSERT INTO lab_d4_pricing_rules (priority, rule_type, route_no, departure_airport, arrival_airport, 
                                   airplane_code, fare_conditions, recommended_price, 
                                   min_price, max_price, sample_count, sample_flights)
SELECT 
    3 AS priority,
    'global_class' AS rule_type,
    NULL AS route_no,
    NULL AS departure_airport,
    NULL AS arrival_airport,
    NULL AS airplane_code,
    fare_conditions,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_price) AS recommended_price,
    MIN(min_price) AS min_price,
    MAX(max_price) AS max_price,
    SUM(tickets_sold) AS sample_count,
    COUNT(DISTINCT flight_id) AS sample_flights
FROM lab_d4_historical_flight_prices
GROUP BY fare_conditions;

-- Проверяем количество правил по уровням
SELECT priority, rule_type, COUNT(*) 
FROM lab_d4_pricing_rules 
GROUP BY priority, rule_type 
ORDER BY priority;

--  priority |      rule_type       | count
-- ----------+----------------------+-------
--         1 | route_airplane_class |  1342
--         2 | airport_pair_class   |  1180
--         3 | global_class         |     3 (эконом, комфорт и бизнес)





-- Шаг 4: применяем правила к будущим рейсам, с ценами для каждого класса
CREATE TABLE IF NOT EXISTS lab_d4_upcoming_flight_prices AS
-- подтабличка 1: только будущие полёты
WITH future_flights AS (
    SELECT 
        f.flight_id,
        f.route_no,
        r.departure_airport,
        r.arrival_airport,
        r.airplane_code,
        f.scheduled_departure,
        f.status
    FROM bookings.flights f
    JOIN bookings.routes r ON r.route_no = f.route_no 
        AND r.validity @> f.scheduled_departure
    WHERE f.scheduled_departure >= bookings.now()  -- только будущие рейсы
        AND f.status <> 'Cancelled'
),
-- подтабличка 2: классы обслуживания
all_classes AS (
    SELECT DISTINCT fare_conditions 
    FROM lab_d4_class_capacity
),
-- подтабличка 3: рейсы X классы
flight_classes AS (
    SELECT 
        ff.*,
        ac.fare_conditions,
        cc.capacity
    FROM future_flights ff
    CROSS JOIN all_classes ac
    LEFT JOIN lab_d4_class_capacity cc ON cc.airplane_code = ff.airplane_code 
        AND cc.fare_conditions = ac.fare_conditions
)
-- сам запрос: выбираем информацию о рейсе+классе и подбираем подходящее правило ценообразования
SELECT 
    fc.flight_id,
    fc.route_no,
    fc.departure_airport,
    fc.arrival_airport,
    fc.airplane_code,
    fc.fare_conditions,
    fc.capacity AS class_capacity,
    fc.scheduled_departure,
    fc.status,
    pr.recommended_price,
    pr.priority AS rule_priority,
    pr.rule_type,
    pr.sample_flights
FROM flight_classes fc
LEFT JOIN LATERAL (
    SELECT * 
    FROM lab_d4_pricing_rules pr
    WHERE pr.fare_conditions = fc.fare_conditions
        AND (
               (pr.priority = 1 AND pr.route_no = fc.route_no AND pr.airplane_code = fc.airplane_code)
            OR (pr.priority = 2 AND pr.departure_airport = fc.departure_airport AND pr.arrival_airport = fc.arrival_airport)
            OR (pr.priority = 3)
        )
    ORDER BY pr.priority
    LIMIT 1
) pr ON true;

-- Проверяем покрытие
SELECT 
    COUNT(*) AS total_flight_classes,
    COUNT(recommended_price) AS with_price,
    ROUND(100.0 * COUNT(recommended_price) / COUNT(*), 2) AS coverage_percent
FROM lab_d4_upcoming_flight_prices; -- выдало 100 процентов





-- Шаг 5: итоговая таблицы цен для всех будущих рейсов
CREATE TABLE IF NOT EXISTS lab_d4_final_price_list AS
SELECT 
    flight_id,
    route_no,
    departure_airport,
    arrival_airport,
    airplane_code,
    fare_conditions,
    class_capacity,
    scheduled_departure,
    recommended_price,
    rule_priority,
    rule_type
FROM lab_d4_upcoming_flight_prices
WHERE recommended_price IS NOT NULL
ORDER BY scheduled_departure, flight_id, fare_conditions;

-- Проверяем
SELECT * FROM lab_d4_final_price_list LIMIT 20;
