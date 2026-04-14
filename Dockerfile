FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY backend/ backend/
COPY comp_files/ comp_files/

# Railway injects PORT env var
ENV PORT=8000
ENV ENV=production
ENV LOG_LEVEL=INFO

EXPOSE ${PORT}

CMD uvicorn backend.main:app --host 0.0.0.0 --port ${PORT}
