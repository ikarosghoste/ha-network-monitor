FROM python:3.12-alpine

RUN apk add --no-cache iputils

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY network_monitor.py .
COPY run.sh .

RUN chmod +x run.sh

CMD ["/app/run.sh"]
