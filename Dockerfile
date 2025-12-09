FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main_2.py .

# Default entrypoint expects the raw dataset to be mounted into /data.
ENTRYPOINT ["python", "main.py", "--input", "/data/mainpipe_data_v1.jsonl", "--output", "/app/outputs"]
