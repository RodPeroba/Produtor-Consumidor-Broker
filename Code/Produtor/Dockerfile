FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY produtor.py .

EXPOSE 5000

CMD ["python", "produtor.py"]