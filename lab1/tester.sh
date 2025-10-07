#!/bin/bash

# Проверка аргумента
NUM_CLIENTS=$1
if [ -z "$NUM_CLIENTS" ]; then
  echo "Usage: $0 <num_clients>"
  exit 1
fi

DELAY_SECS=5
SECS_MOD=7
THREADS_STD=4
PORT_STD=12345
ISSUER_NAME_STD="TestCA"

# Пути к JAR
SERVER_JAR="out/ServerMain.jar"
CLIENT_JAR="out/Client.jar"

# Параметры сервера
SERVER_THREADS=$2
if [ -z "$SERVER_THREADS" ]; then
  SERVER_THREADS=$THREADS_STD
fi
SERVER_PORT=$3
if [ -z "$SERVER_PORT" ]; then
  SERVER_PORT=$PORT_STD
fi
ISSUER_NAME=$4
if [ -z "$ISSUER_NAME" ]; then
  ISSUER_NAME=$ISSUER_NAME_STD
fi
PRIVATE_KEY_PATH="ca_private.pem"
CERT_PATH="ca_cert.pem"

# Запуск сервера в фоне
echo "[Script] Starting server..."
# java -jar "$SERVER_JAR" $SERVER_PORT $SERVER_THREADS --issuer "$ISSUER_NAME" --key "$PRIVATE_KEY_PATH" --cert "$CERT_PATH" &
java -jar "$SERVER_JAR" $SERVER_PORT $SERVER_THREADS --issuer "$ISSUER_NAME" --key "$PRIVATE_KEY_PATH" --cert "$CERT_PATH" 2>&1 | sed "s/^/{SERVER} /" &
SERVER_PID=$!

# Даем серверу время стартовать
sleep 2

# Запуск клиентов
echo "[Script] Starting $NUM_CLIENTS clients..."
for i in $(seq 1 $NUM_CLIENTS); do
    CLIENT_NAME="clie_$i"
    DELAY_SECS=$(( (i + DELAY_SECS * DELAY_SECS) % SECS_MOD ))
    # java -jar "$CLIENT_JAR" "localhost" $SERVER_PORT "$CLIENT_NAME" --delay $DELAY_SECS &
    java -jar "$CLIENT_JAR" "localhost" $SERVER_PORT "$CLIENT_NAME" --delay $DELAY_SECS \
      2>&1 | sed "s/^/{$CLIENT_NAME} /" &
done

# Ждем завершения всех клиентов
wait

# Остановка сервера
echo "[Script] Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "[Script] Done."
