FROM python:3.14.0b4-slim-bullseye

WORKDIR /app

# Copy requirements and install dependencies first for better caching
COPY requirements.txt .

RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Default command to run your app
CMD ["python3", "main.py"]
