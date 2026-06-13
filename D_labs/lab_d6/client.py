import requests
import json

BASE_URL = "http://127.0.0.1:8000"

def print_line():
    print("-" * 60)

def main_menu():
    while True:
        print_line()
        print("FROM lab_d4_final_price_list;\n     Начало продаж      |      Конец продаж      | Всего записей\n------------------------+------------------------+---------------\n 2025-12-01 07:10:00+07 | 2026-01-30 06:55:00+07 |         18783\n")
        print("--- [RU] ГЛАВНОЕ МЕНЮ | [EN] MAIN MENU ---")
        print("1. [RU] Список городов           | [EN] List Cities")
        print("2. [RU] Список аэропортов        | [EN] List Airports")
        print("3. [RU] Аэропорты в городе       | [EN] Airports in City")
        print("4. [RU] Входящее расписание      | [EN] Inbound Schedule")
        print("5. [RU] Исходящее расписание     | [EN] Outbound Schedule")
        print("6. [RU] Онлайн регистрация       | [EN] Check-in")
        print("7. [RU] Поиск маршрутов (Top-X)  | [EN] Search Routes")
        print("8. [RU] Создать бронирование     | [EN] Create Booking")
        print("0. [RU] Выход                    | [EN] Exit")

        choice = input("\n[RU] Выберите пункт: | [EN] Choose item: ")

        try:
            if choice == '1':
                res = requests.get(f"{BASE_URL}/cities")
                cities = res.json()
                print(f"\n[RU] Доступные города: | [EN] Available cities:")
                for c in cities:
                    print(f" - {c['ru']} / {c['en']}")

            elif choice == '2':
                res = requests.get(f"{BASE_URL}/airports")
                for a in res.json():
                    print(f"[{a['code']}] {a['name']['ru']} ({a['city']['ru']})")

            elif choice == '3':
                city = input("[RU] Введите город: | [EN] Enter city: ")
                res = requests.get(f"{BASE_URL}/cities/{city}/airports")
                if res.status_code == 200:
                    for a in res.json():
                        print(f"Code: {a['code']} | Name: {a['name']['ru']}")
                else:
                    print(f"Error: {res.json().get('detail')}")

            elif choice == '4':
                code = input("[RU] Код аэропорта (SVO): | [EN] Airport code: ").upper()
                res = requests.get(f"{BASE_URL}/airports/{code}/schedule/inbound")
                for f in res.json():
                    print(f"Flight {f['flight_no']} from {f['origin']} at {f['arrival']} (Days: {f['days']})")

            elif choice == '5':
                code = input("[RU] Код аэропорта (SVO): | [EN] Airport code: ").upper()
                res = requests.get(f"{BASE_URL}/airports/{code}/schedule/outbound")
                for f in res.json():
                    print(f"Flight {f['flight_no']} to {f['destination']} at {f['departure']} (Days: {f['days']})")

            elif choice == '6':
                t_no = input("[RU] Номер билета (13 цифр): | [EN] Ticket No: ")
                f_id = input("[RU] ID рейса (число): | [EN] Flight ID: ")
                res = requests.post(f"{BASE_URL}/check-in", params={"ticket_no": t_no, "flight_id": f_id})
                print(json.dumps(res.json(), indent=2, ensure_ascii=False))

            elif choice == '7':
                print("\n--- [RU] ПОИСК МАРШРУТА | [EN] SEARCH ROUTE ---")
                from_p = input("From (SVO / Москва): ")
                to_p = input("To (UFA / Уфа): ")
                date = input("Date (YYYY-MM-DD): ")
                b_class = input("Class (Economy/Comfort/Business): ")

                print("[RU] Введите число (0-3) или 'unbound' для любого кол-ва пересадок")
                conn_input = input("Connections: ").lower()

                if conn_input == 'unbound' or conn_input == '':
                    conn_max = None
                else:
                    conn_max = int(conn_input)

                limit_val = input("[RU] Сколько результатов показать? (Enter=50): ") or "50"

                payload = {
                    "from_point": from_p,
                    "to_point": to_p,
                    "departure_date": date,
                    "booking_class": b_class,
                    "max_connections": conn_max,
                    "limit": int(limit_val)
                }

                res = requests.post(f"{BASE_URL}/search", json=payload)
                if res.status_code == 200:
                    data = res.json()
                    print(f"\n[RU] Найдено: {len(data)} | [EN] Found: {len(data)}")
                    for i, path in enumerate(data, 1):
                        route_str = " -> ".join(map(str, path['flights']))
                        print(f"{i}. {route_str} | {path['price']} руб. ({path['connections']} перес.)")
                        print(f"   Departure: {path['departure']}")
                else:
                    print(f"Error: {res.text}")

            elif choice == '8':
                print("\n--- [RU] БРОНИРОВАНИЕ | [EN] BOOKING ---")
                p_id = input("Passenger ID (Passport): ")
                p_name = input("Passenger Name (IVAN IVANOV): ")
                b_class = input("Class (Economy/Comfort/Business): ")
                f_ids_str = input("Flight IDs (через запятую, напр: 11486,11487): ")

                f_ids = [int(i.strip()) for i in f_ids_str.split(',')]

                payload = {
                    "passenger_id": p_id,
                    "passenger_name": p_name,
                    "booking_class": b_class,
                    "flight_ids": f_ids
                }

                res = requests.post(f"{BASE_URL}/bookings", json=payload)
                if res.status_code in [200, 201]:
                    print("\n[RU] УСПЕШНО! | [EN] SUCCESS!")
                    print(json.dumps(res.json(), indent=2, ensure_ascii=False))
                else:
                    print(f"\n[RU] ОШИБКА БРОНИРОВАНИЯ | [EN] BOOKING ERROR")
                    print(res.text)

            elif choice == '0':
                print("[RU] Выход... | [EN] Exiting...")
                break

        except Exception as e:
            print(f"\n[RU] Произошла ошибка: {e} | [EN] Error occurred: {e}")

if __name__ == "__main__":
    main_menu()
