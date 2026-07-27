# Container image for the native (confluent-kafka) store_operation producer.
# Use this on Cloud Run / Render / Fly / Azure Container Apps when you want the NATIVE
# Kafka protocol (no Aiven REST proxy). For Cloudflare Workers use producer_worker.js.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer_service.py .

# Set STREAM_INTERVAL_SEC>0 to make the container a continuous producer;
# else it only produces on POST /produce.
EXPOSE 7860
CMD ["sh", "-c", "uvicorn producer_service:app --host 0.0.0.0 --port ${PORT:-7860}"]
