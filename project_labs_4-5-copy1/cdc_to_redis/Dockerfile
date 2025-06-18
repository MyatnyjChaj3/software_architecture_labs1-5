FROM python:3.11-slim
WORKDIR /app

COPY cdc_to_redis.py .

RUN pip install confluent-kafka redis

CMD ["python", "cdc_to_redis.py"]