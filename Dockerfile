FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["streamlit", "run", "app/sima_app.py", \
     "--server.port=8080", \
     "--server.address=0.0.0.0", \
     "--logger.level=debug"]
     
# docker build -t sima-dashboard:local .
# docker run --rm -p 8501:8080 --env-file .env sima-dashboard:local