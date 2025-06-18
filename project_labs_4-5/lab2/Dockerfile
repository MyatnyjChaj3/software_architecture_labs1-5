FROM python:3.12.5

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app .

EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "lab2:app", "--host", "0.0.0.0", "--port", "8000"]