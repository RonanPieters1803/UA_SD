# Dockerfile WS
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY WM_WS_E.py WM_WS_M.py ./
CMD ["python", "WM_WS_M.py"]