FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

# Install build dependencies for python_bcrypt
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["streamlit", "run", "main_app.py", \
     "--server.port=8080", \
     "--server.address=0.0.0.0", \
     "--logger.level=debug"]