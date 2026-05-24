FROM python:3.12-slim

WORKDIR /app

# Install system dependencies required by C-extension packages (e.g. psutil)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
