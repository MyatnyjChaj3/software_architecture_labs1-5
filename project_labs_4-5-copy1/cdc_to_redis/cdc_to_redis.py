# cdc_to_redis.py
import json
import signal
import sys

import redis
from confluent_kafka import Consumer

# Настройка Redis
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# Настройка Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'cdc-to-redis-handler',
    'auto.offset.reset': 'earliest'
})

# Топик, соответствующий таблице students
TOPIC = 'university-server.public.students'
consumer.subscribe([TOPIC])

print(f"Listening to topic: {TOPIC}")

# Функция для корректного завершения
def shutdown_handler(signum, frame):
    print("Shutting down consumer...")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"[Error] {msg.error()}")
        continue

    try:
        # Debezium использует JSON без схемы, ключ и значение - это байты
        key_str = msg.key().decode('utf-8')
        key = json.loads(key_str)

        if msg.value() is None:
            value = None
        else:
            value_str = msg.value().decode('utf-8')
            value = json.loads(value_str)

    except Exception as e:
        print(f"[Parse error] {e} on key='{msg.key()}' value='{msg.value()}'")
        continue

    # Debezium помещает первичный ключ в payload ключа
    student_id = key.get("id")
    if not student_id:
        print(f"[Key error] No 'id' in message key: {key}")
        continue

    redis_key = f"student:{student_id}"

    # Логика обработки CDC
    if value is None or value.get("after") is None:
        # DELETE: "value" is null (tombstone record)
        if redis_client.exists(redis_key):
            redis_client.delete(redis_key)
            print(f"[DEL] {redis_key}")
        else:
            print(f"[DEL-SKIP] Key {redis_key} not found in Redis.")
    else:
        # INSERT/UPDATE: "value" has "after" field
        student_data = value["after"]
        # HSET идеально подходит для сохранения объекта как хэша в Redis
        redis_client.hset(redis_key, mapping=student_data)
        print(f"[SET] {redis_key} => {student_data}")