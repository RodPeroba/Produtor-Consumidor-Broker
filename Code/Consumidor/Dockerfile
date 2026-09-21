FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY consumidor.py .
COPY .env .

EXPOSE 5001

CMD ["python", "consumidor.py"]