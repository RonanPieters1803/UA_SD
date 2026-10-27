# Dockerfile
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY WM_Central.py .
RUN mkdir -p /app/data
ENV DB_PATH=/app/data/wm_central.db
ENV SOCKET_PORT=5000
EXPOSE 8080 5000
CMD ["python", "WM_Central.py"]