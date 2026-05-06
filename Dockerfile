FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir psycopg2-binary requests

# Copy the production sync worker
COPY sync/sync_worker_prod.py /app/sync_worker.py

# Run with full mode
CMD ["python", "-u", "sync_worker.py", "--mode", "full"]
