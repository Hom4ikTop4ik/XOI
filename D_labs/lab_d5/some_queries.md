```sql
SELECT 
    p.flight_id, 
    p.scheduled_departure, 
    p.departure_airport, 
    p.arrival_airport,
    p.class_capacity - (SELECT COUNT(*) FROM bookings.segments s WHERE s.flight_id = p.flight_id AND s.fare_conditions = p.fare_conditions) as free_seats,
    p.fare_conditions
FROM lab_d4_final_price_list p
WHERE p.class_capacity > (SELECT COUNT(*) FROM bookings.segments s WHERE s.flight_id = p.flight_id AND s.fare_conditions = p.fare_conditions)
ORDER BY free_seats DESC
LIMIT 10;

 flight_id |  scheduled_departure   | departure_airport | arrival_airport | free_seats | fare_conditions
-----------+------------------------+-------------------+-----------------+------------+-----------------
     15431 | 2025-12-25 20:00:00+07 | YYC               | YVR             |        326 | Economy
     15037 | 2025-12-23 15:55:00+07 | HND               | CTS             |        326 | Economy
     15334 | 2025-12-25 06:40:00+07 | SVO               | LED             |        326 | Economy
     14681 | 2025-12-21 15:55:00+07 | HND               | CTS             |        326 | Economy
     14944 | 2025-12-23 03:30:00+07 | DEL               | AMD             |        326 | Economy
     13028 | 2025-12-12 09:55:00+07 | LHR               | ORY             |        326 | Economy
     14144 | 2025-12-18 15:55:00+07 | HND               | CTS             |        326 | Economy
     14643 | 2025-12-21 11:25:00+07 | YVR               | YYC             |        326 | Economy
     14321 | 2025-12-19 15:55:00+07 | HND               | CTS             |        326 | Economy
     15570 | 2025-12-26 15:55:00+07 | HND               | CTS             |        326 | Economy
```


```sql
SELECT b.book_ref, t.ticket_no, t.passenger_name
FROM bookings.bookings b
JOIN bookings.tickets t ON b.book_ref = t.book_ref
WHERE b.book_ref = 'DIK961'
LIMIT 10;
```


```sql
SELECT 
    s.ticket_no, 
    s.flight_id, 
    s.fare_conditions, 
    s.price,
    f.departure_airport, 
    f.arrival_airport, 
    f.scheduled_departure
FROM bookings.segments s
JOIN lab_d4_final_price_list f ON s.flight_id = f.flight_id AND s.fare_conditions = f.fare_conditions
WHERE s.ticket_no = '0005434990772'; -- Замените на ваш ticket_no
```



```sql
SELECT 
    p.flight_id, 
    p.fare_conditions,
    p.class_capacity AS "Всего мест",
    (SELECT COUNT(*) FROM bookings.segments s 
     WHERE s.flight_id = p.flight_id 
       AND s.fare_conditions = p.fare_conditions) AS "Занято билетов"
FROM lab_d4_final_price_list p
WHERE p.flight_id IN (15421,16578,17777,20145,20495)
  AND p.fare_conditions = 'Business';
```





```sql
SELECT 
    t.book_ref,
    t.passenger_name,
    t.ticket_no,
    s.flight_id,
    p.departure_airport || ' -> ' || p.arrival_airport AS "Route",
    p.scheduled_departure
FROM bookings.tickets t
JOIN bookings.segments s ON t.ticket_no = s.ticket_no
JOIN lab_d4_final_price_list p ON s.flight_id = p.flight_id AND s.fare_conditions = p.fare_conditions
WHERE t.book_ref = 'DZMTE6' -- Замените на ваш book_ref
ORDER BY p.scheduled_departure;
```








```sql
SELECT 
    t.passenger_name,
    bp.ticket_no,
    bp.flight_id,
    bp.seat_no,
    bp.boarding_no,
    p.departure_airport || ' -> ' || p.arrival_airport AS route,
    p.scheduled_departure
FROM bookings.boarding_passes bp
JOIN bookings.tickets t ON bp.ticket_no = t.ticket_no
JOIN lab_d4_final_price_list p ON bp.flight_id = p.flight_id 
     AND p.fare_conditions = (SELECT fare_conditions FROM bookings.segments 
                              WHERE ticket_no = bp.ticket_no AND flight_id = bp.flight_id)
WHERE bp.ticket_no = '0005434990772';
```


```sql
SELECT * 
FROM bookings.boarding_passes 
WHERE ticket_no = '0005434990772' AND flight_id = 15421;
```


```sql
SELECT seat_no, ticket_no, boarding_no
FROM bookings.boarding_passes
WHERE flight_id = 15421
ORDER BY seat_no;
```







---
---
---



```sql
SELECT * FROM bookings.routes WHERE departure_airport = 'SVO' AND arrival_airport = 'UFA';
 route_no |                      validity                       | departure_airport | arrival_airport | airplane_code | days_of_week  | scheduled_time | duration
----------+-----------------------------------------------------+-------------------+-----------------+---------------+---------------+----------------+----------
 PG0012   | ["2025-10-01 07:00:00+07","2025-11-01 07:00:00+07") | SVO               | UFA             | 351           | {2}           | 13:55:00       | 01:55:00
 PG0012   | ["2025-11-01 07:00:00+07","2025-12-01 07:00:00+07") | SVO               | UFA             | E70           | {2,3,5,6,7}   | 13:55:00       | 02:05:00
 PG0012   | ["2025-12-01 07:00:00+07","2026-01-01 07:00:00+07") | SVO               | UFA             | E70           | {1,2,3,5,6,7} | 13:55:00       | 02:05:00
 PG0012   | ["2026-01-01 07:00:00+07","2026-02-01 07:00:00+07") | SVO               | UFA             | E70           | {1,2,3,4,5,6} | 13:55:00       | 02:05:00
```



```sql
SELECT 
    flight_id, 
    scheduled_departure, 
    departure_airport, 
    arrival_airport, 
    fare_conditions, 
    recommended_price
FROM lab_d4_final_price_list
WHERE departure_airport = 'SVO' AND arrival_airport = 'UFA'
ORDER BY scheduled_departure
LIMIT 20;
```



```sql
SELECT scheduled_departure::date, count(*) as flights_count
FROM lab_d4_final_price_list
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```

