#Dockerfile
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

# Copy application code
COPY csv_battle_card_generator.py .

#Copy the CSV file
COPY tenants_enriched.csv .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV GCS_BUCKET=dqe-fiber-data
ENV PROJECT_ID=lma-website-461920

# Default command
CMD ["python", "csv_battle_card_generator.py"]
