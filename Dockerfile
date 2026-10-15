# Dockerfile
FROM python:3.12-alpine
WORKDIR /app
COPY . /app
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi
EXPOSE 5000
CMD ["python", "server.py"]