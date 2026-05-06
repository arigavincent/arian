FROM python:3.12-slim

WORKDIR /app

# Copy only the sync worker
COPY sync/sync_worker_prod.py /app/sync_worker.py

# Install dependencies
RUN pip install --no-cache-dir psycopg2-binary requests

# Run the sync worker
CMD ["python", "-u", "sync_worker.py", "--mode", "full"]
