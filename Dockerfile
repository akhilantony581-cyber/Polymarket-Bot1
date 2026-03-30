FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (required for web3 + py-clob-client)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Create logs directory
RUN mkdir -p logs

# Expose dashboard port
EXPOSE 8080

# Run both bot and dashboard concurrently
CMD ["sh", "-c", "python main.py & python dashboard.py"]
