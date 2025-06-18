FROM python:3.12.5

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD sleep 30 && python generator.py && python create_elasticsearch_index.py