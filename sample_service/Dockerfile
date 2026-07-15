# Portable image for the DLM mock service.
# Works on: Hugging Face Spaces (Docker SDK), Render, Google Cloud Run, Koyeb,
#           Azure Container Apps, Fly.io.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api_service.py .

# HF Spaces probes port 7860 by default; Render/Cloud Run/Koyeb/ACA inject $PORT.
# ${PORT:-7860} honours their port when set, else falls back to 7860 for HF.
EXPOSE 7860
CMD ["sh", "-c", "uvicorn api_service:app --host 0.0.0.0 --port ${PORT:-7860}"]
