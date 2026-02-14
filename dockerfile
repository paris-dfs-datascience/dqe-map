# Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application code files
COPY battlecard_generator.py .
COPY battlecard_config.py .
COPY battlecard_llm.py .
COPY battlecard_processor.py .
COPY battlecard_storage.py .

# Copy the CSV file
COPY tenants_enriched.csv .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV GCS_BUCKET=dqe-fiber-data
ENV PROJECT_ID=lma-website-461920

# Default command
CMD ["python", "battlecard_generator.py"]