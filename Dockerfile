FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p data

ENV WEATHER_HOST=0.0.0.0
ENV WEATHER_PORT=5000
EXPOSE 5000

# The first Slovak forecast downloads an ALADIN run. Give the worker time.
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:5000", "--timeout", "120", "scripts.api:app"]
