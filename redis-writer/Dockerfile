FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY writer.py .
EXPOSE 8099
ENTRYPOINT ["python", "-u", "writer.py"]
