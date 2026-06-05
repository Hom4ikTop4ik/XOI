#### создать БД
createdb -E UTF8 -l C -T template0 demo

#### проверить наличие таблицы
psql -c "\l" | grep demo

#### Воссатновить данные из дампа
psql -d demo -f demo-20250901-3m.sql

#### Проверить версию БД
psql -d demo -c "SELECT bookings.version();"

#### Посмотреть список таблиц
psql -d demo -c "\dt"

#### Проверить количество рейсов
psql -d demo -c "SELECT COUNT(*) FROM flights;"

#### Подключитсья к БД для выполнения запросов
psql -d demo



#### создать, использовать дамп, войти
createdb -E UTF8 -l C -T template0 demo
psql -d demo -f demo-20250901-3m.sql
psql -d demo



#### почистить
psql -d postgres -c "DROP DATABASE demo;"
