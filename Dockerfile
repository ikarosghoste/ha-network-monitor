FROM python:3.12-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nut_monitor.py .

CMD ["python", "-u", "/app/nut_monitor.py"]
