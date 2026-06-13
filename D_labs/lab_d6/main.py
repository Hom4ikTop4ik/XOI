import psycopg
import random
import string
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import List, Dict, Optional

# Настройки подключения
DB_CONFIG = "dbname=demo user=postgres host=localhost password=postgres"

# --- [RU] Логика генерации ID | [EN] ID Generation Logic ---
def generate_book_ref():
    """[RU] Генерирует 6-значный код брони (цифры и заглавные буквы)"""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

def generate_ticket_no(cur):
    """[RU] Генерирует уникальный 13-значный номер билета на основе текущего максимума"""
    cur.execute("SELECT MAX(ticket_no::bigint) FROM bookings.tickets")
    max_no = cur.fetchone()[0]
    return str(max_no + 1).zfill(13)

# --- [RU] События жизненного цикла | [EN] Lifespan events ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # [RU] Действия при запуске
    print("--- [RU] Сервер запущен | [EN] Server started ---")
    yield
    # [RU] Действия при остановке
    print("--- [RU] Сервер остановлен | [EN] Server stopped ---")

app = FastAPI(title="Flights API Service", lifespan=lifespan)

def get_db_connection():
    return psycopg.connect(DB_CONFIG)

# --- [RU] Модели данных | [EN] Data Models ---
class SearchRequest(BaseModel):
    from_point: str
    to_point: str
    departure_date: str
    booking_class: str
    max_connections: Optional[int] = None
    limit: int = 50

class BookingRequest(BaseModel):
    passenger_id: str
    passenger_name: str
    booking_class: str
    flight_ids: List[int] # [RU] Список ID рейсов из поиска

# --- [RU] Эндпоинты (1-5) | [EN] Endpoints (1-5) ---

@app.get("/cities")
def get_cities():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT city FROM bookings.airports_data;")
            return [row[0] for row in cur.fetchall()]

@app.get("/airports")
def get_airports():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT airport_code, airport_name, city FROM bookings.airports_data;")
            return [{"code": r[0], "name": r[1], "city": r[2]} for r in cur.fetchall()]

@app.get("/cities/{city_name}/airports")
def get_airports_in_city(city_name: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT airport_code, airport_name FROM bookings.airports_data
                WHERE city->>'ru' = %s OR city->>'en' = %s;
            """, (city_name, city_name))
            rows = cur.fetchall()
            if not rows: raise HTTPException(status_code=404, detail="City not found")
            return [{"code": r[0], "name": r[1]} for r in rows]

@app.get("/airports/{code}/schedule/inbound")
def get_inbound_schedule(code: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT days_of_week, (scheduled_time + duration)::time, route_no, departure_airport
                FROM bookings.routes WHERE arrival_airport = UPPER(%s);
            """, (code,))
            return [{"days": r[0], "arrival": str(r[1]), "flight_no": r[2], "origin": r[3]} for r in cur.fetchall()]

@app.get("/airports/{code}/schedule/outbound")
def get_outbound_schedule(code: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT days_of_week, scheduled_time, route_no, arrival_airport
                FROM bookings.routes WHERE departure_airport = UPPER(%s);
            """, (code,))
            return [{"days": r[0], "departure": str(r[1]), "flight_no": r[2], "destination": r[3]} for r in cur.fetchall()]

# --- [RU] 6. Регистрация | [EN] Check-in ---
@app.post("/check-in")
def check_in(ticket_no: str, flight_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT fare_conditions FROM bookings.segments WHERE ticket_no = %s AND flight_id = %s;", (ticket_no, flight_id))
            seg = cur.fetchone()
            if not seg: raise HTTPException(status_code=404, detail="Ticket not found")

            cur.execute("SELECT 1 FROM bookings.boarding_passes WHERE ticket_no = %s AND flight_id = %s;", (ticket_no, flight_id))
            if cur.fetchone(): raise HTTPException(status_code=400, detail="Already checked in")

            cur.execute("""
                SELECT s.seat_no FROM bookings.seats s
                WHERE s.airplane_code = (SELECT airplane_code FROM bookings.flights WHERE flight_id = %s)
                AND s.fare_conditions = %s AND s.seat_no NOT IN (SELECT seat_no FROM bookings.boarding_passes WHERE flight_id = %s)
                ORDER BY random() LIMIT 1;
            """, (flight_id, seg[0], flight_id))
            seat = cur.fetchone()
            if not seat: raise HTTPException(status_code=409, detail="No seats left")

            cur.execute("""
                INSERT INTO bookings.boarding_passes (ticket_no, flight_id, seat_no, boarding_no)
                VALUES (%s, %s, %s, (SELECT COALESCE(MAX(boarding_no), 0) + 1 FROM bookings.boarding_passes WHERE flight_id = %s))
            """, (ticket_no, flight_id, seat[0], flight_id))
            conn.commit()
            return {"status": "Success", "seat": seat[0]}

# --- [RU] 7. Поиск маршрутов | [EN] Search Routes ---
@app.post("/search")
def search_flights(req: SearchRequest):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            search_query = """
            WITH RECURSIVE flight_paths AS (
                -- 1. Базовый случай (Anchor)
                -- Используем p для аэропортов, f для времени прибытия
                SELECT
                    p.flight_id,
                    p.departure_airport,
                    p.arrival_airport,
                    p.scheduled_departure,
                    f.scheduled_arrival,
                    p.recommended_price::numeric as total_price,
                    0 as connections,
                    ARRAY[p.flight_id] as path_ids,
                    ARRAY[p.departure_airport, p.arrival_airport]::text[] as path_codes
                FROM lab_d4_final_price_list p
                JOIN bookings.flights f ON p.flight_id = f.flight_id
                WHERE (p.departure_airport = UPPER(%s) OR p.departure_airport IN (
                    SELECT airport_code FROM bookings.airports_data WHERE city->>'ru' = %s OR city->>'en' = %s))
                  AND p.scheduled_departure >= %s::timestamp AND p.scheduled_departure < %s::timestamp + interval '1 day'
                  AND p.fare_conditions = %s
                  AND (SELECT COUNT(*) FROM bookings.segments s WHERE s.flight_id = p.flight_id AND s.fare_conditions = %s) < p.class_capacity

                UNION ALL

                -- 2. Рекурсия (Recursive member)
                SELECT
                    p.flight_id,
                    fp.departure_airport,
                    p.arrival_airport,
                    fp.scheduled_departure,
                    f.scheduled_arrival,
                    (fp.total_price + p.recommended_price)::numeric,
                    fp.connections + 1,
                    fp.path_ids || p.flight_id,
                    (fp.path_codes || p.arrival_airport)::text[]
                FROM lab_d4_final_price_list p
                JOIN bookings.flights f ON p.flight_id = f.flight_id
                JOIN flight_paths fp ON p.departure_airport = fp.arrival_airport
                WHERE f.scheduled_departure > fp.scheduled_arrival + interval '1 hour'
                  AND fp.connections < COALESCE(%s, 5)
                  AND p.fare_conditions = %s
                  AND NOT p.arrival_airport = ANY(fp.path_codes)
                  AND (SELECT COUNT(*) FROM bookings.segments s WHERE s.flight_id = p.flight_id AND s.fare_conditions = %s) < p.class_capacity
            )
            -- Финальный выбор
            SELECT path_ids, departure_airport, arrival_airport, scheduled_departure, total_price, connections
            FROM flight_paths
            WHERE arrival_airport = UPPER(%s) OR arrival_airport IN (
                SELECT airport_code FROM bookings.airports_data WHERE city->>'ru' = %s OR city->>'en' = %s)
            ORDER BY connections ASC, total_price ASC LIMIT %s;
            """

            # Параметры остаются теми же, что и были в коде выше
            params = (
                req.from_point, req.from_point, req.from_point,
                req.departure_date, req.departure_date,
                req.booking_class, req.booking_class,
                # Рекурсия
                req.max_connections,
                req.booking_class, req.booking_class,
                # Финиш
                req.to_point, req.to_point, req.to_point,
                req.limit
            )

            cur.execute(search_query, params)

            return [
                {
                    "flights": r[0],
                    "origin": r[1],
                    "destination": r[2],
                    "departure": str(r[3]),
                    "price": float(r[4]),
                    "connections": r[5]
                } for r in cur.fetchall()
            ]

# --- [RU] 8. Бронирование | [EN] Booking ---
@app.post("/bookings")
def create_booking(req: BookingRequest):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                # 1. Получаем актуальные цены и проверяем наличие мест для каждого сегмента
                total_amount = 0
                segments_data = []
                for fid in req.flight_ids:
                    cur.execute("""
                        SELECT recommended_price, class_capacity
                        FROM lab_d4_final_price_list
                        WHERE flight_id = %s AND fare_conditions = %s
                    """, (fid, req.booking_class))
                    res = cur.fetchone()
                    if not res: raise HTTPException(status_code=400, detail=f"Flight {fid} not available")

                    price, capacity = res
                    # Проверяем занятость мест
                    cur.execute("SELECT COUNT(*) FROM bookings.segments WHERE flight_id = %s AND fare_conditions = %s", (fid, req.booking_class))
                    occupied = cur.fetchone()[0]
                    if occupied >= capacity:
                        raise HTTPException(status_code=409, detail=f"No seats left on flight {fid}")

                    total_amount += price
                    segments_data.append((fid, price))

                # 2. Создаем запись в bookings
                book_ref = generate_book_ref()
                cur.execute("INSERT INTO bookings.bookings (book_ref, book_date, total_amount) VALUES (%s, bookings.now(), %s)", (book_ref, total_amount))

                # 3. Создаем билет
                t_no = generate_ticket_no(cur)
                cur.execute("INSERT INTO bookings.tickets (ticket_no, book_ref, passenger_id, passenger_name, outbound) VALUES (%s, %s, %s, %s, true)",
                            (t_no, book_ref, req.passenger_id, req.passenger_name))

                # 4. Создаем сегменты (связка билет-рейс)
                for fid, price in segments_data:
                    cur.execute("INSERT INTO bookings.segments (ticket_no, flight_id, fare_conditions, price) VALUES (%s, %s, %s, %s)",
                                (t_no, fid, req.booking_class, price))

                conn.commit()
                return {"book_ref": book_ref, "ticket_no": t_no, "total_price": float(total_amount)}
            except Exception as e:
                conn.rollback()
                if isinstance(e, HTTPException): raise e
                raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
