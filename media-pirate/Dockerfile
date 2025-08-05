FROM python:3.14.0b4-slim-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ffmpeg \
    build-essential \
    gcc \
    python3-dev \
    libffi-dev \
    libuv1-dev \
 && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies first for better caching
COPY requirements.txt .

RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Default command to run your app
CMD ["python3", "main.py"]
