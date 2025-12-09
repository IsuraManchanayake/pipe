FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./
COPY pipelib ./pipelib

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && mkdir -p /data \
    && curl -L "https://s3.us-east-1.amazonaws.com/mainpipe.maincode.com/mainpipe_data_v1.jsonl" \
        -o /data/mainpipe_data_v1.jsonl \
    && rm -rf /var/lib/apt/lists/*
    
# Default entrypoint expects the raw dataset to be mounted into /data.
ENTRYPOINT ["python", "main.py", "--input", "/data/mainpipe_data_v1.jsonl", "--output", "/app/outputs"]
