# ─────────────────────────────────────────────
# Base image
# ─────────────────────────────────────────────
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source code
COPY bot.py .

# Create logs directory
RUN mkdir -p logs

# Expose health check port (default 8000, overridable via ENV)
EXPOSE 8000

# Run the bot
CMD ["python", "bot.py"]
