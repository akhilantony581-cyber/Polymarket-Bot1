FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Force clean install — remove any cached packages from previous builds
RUN pip install --no-cache-dir --upgrade pip && \
    pip uninstall -y py-clob-client eip712-structs py-order-utils 2>/dev/null || true

# Install Python dependencies (cache bust: 2026-03-30-v2)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Create logs directory
RUN mkdir -p logs

# Expose dashboard port
EXPOSE 8080

CMD ["python", "main.py"]
