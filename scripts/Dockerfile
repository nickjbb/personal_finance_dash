FROM python:3.10-alpine3.13

RUN apk add --no-cache gcc musl-dev libffi-dev make bash curl busybox-suid

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl.py ../etl.py

COPY crontab /etc/crontabs/root

CMD ["crond", "-f", "-l", "2"]