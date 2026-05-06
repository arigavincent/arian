FROM python:3.12-slim

WORKDIR /app

COPY sync/sync_worker_prod.py /app/sync_worker.py

RUN pip install psycopg2-binary requests

CMD ["python", "-u", "sync_worker.py", "--mode", "full"]
