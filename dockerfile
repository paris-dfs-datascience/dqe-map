FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY battlecard_generator.py .
COPY battlecard_config.py .
COPY battlecard_llm.py .
COPY battlecard_processor.py .
COPY battlecard_storage.py .
COPY hubspot_matcher.py .
COPY netsuite_matcher.py .

ENV PYTHONUNBUFFERED=1
ENV GCS_BUCKET=dqe-fiber-data
ENV PROJECT_ID=lma-website-461920

CMD ["python", "battlecard_generator.py"]