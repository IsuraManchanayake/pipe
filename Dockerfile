FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./
COPY pipelib ./pipelib

# Fetch the default dataset during the build so the image can run without a mounted input file.
ARG DATA_URL="https://s3.us-east-1.amazonaws.com/mainpipe.maincode.com/mainpipe_data_v1.jsonl"
RUN mkdir -p /data \
    && python - <<'PY'
import os
import urllib.request

url = os.environ["DATA_URL"]
destination = "/data/mainpipe_data_v1.jsonl"
urllib.request.urlretrieve(url, destination)
PY
    
# Default entrypoint expects the raw dataset to be mounted into /data.
ENTRYPOINT ["python", "main.py", "--input", "/data/mainpipe_data_v1.jsonl", "--output", "/app/outputs"]
