FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api.py index.html ./
EXPOSE 8088
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8088", "--log-level", "warning"]
