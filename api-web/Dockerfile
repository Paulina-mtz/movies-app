FROM python:3.11-slim

WORKDIR /app

# Para psycopg2-binary no necesitamos build tools.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

EXPOSE 5000

CMD ["python", "app.py"]
