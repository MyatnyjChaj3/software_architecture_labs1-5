import time

from elasticsearch import Elasticsearch


# Подключение к Elasticsearch с ожиданием готовности
def wait_for_elasticsearch():
    es = Elasticsearch(hosts=["http://elasticsearch:9200"])
    max_attempts = 10
    attempt = 1
    while attempt <= max_attempts:
        try:
            if es.ping():
                print("Elasticsearch доступен!")
                return es
            else:
                print(f"Попытка {attempt}/{max_attempts}: Elasticsearch не отвечает. Ждём 10 секунд...")
                time.sleep(10)
                attempt += 1
        except Exception as e:
            print(f"Попытка {attempt}/{max_attempts}: Ошибка подключения: {e}. Ждём 10 секунд...")
            time.sleep(10)
            attempt += 1
    raise Exception("Не удалось подключиться к Elasticsearch после нескольких попыток.")

# Определение маппинга для индекса materials
index_mapping = {
    "mappings": {
        "properties": {
            "id": {"type": "integer"},
            "id_lect": {"type": "integer"},
            "name": {"type": "text", "analyzer": "russian"},
            "full_text_description": {"type": "text", "analyzer": "russian"}
        }
    }
}

# Название индекса
index_name = "materials"

# Подключение и создание индекса
try:
    es = wait_for_elasticsearch()
    
    # Проверка, существует ли индекс
    if es.indices.exists(index=index_name):
        print(f"Индекс {index_name} уже существует. Удаляем его...")
        es.indices.delete(index=index_name)

    # Создание индекса
    print(f"Создаём индекс {index_name}...")
    es.indices.create(index=index_name, body=index_mapping)
    print(f"Индекс {index_name} успешно создан!")
except Exception as e:
    print(f"Ошибка при создании индекса: {e}")
    raise