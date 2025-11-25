# Base image
FROM python:3.10-slim


# Set work directory
WORKDIR /app


# Install system dependencies
RUN apt-get update && apt-get install -y \
build-essential \
libpq-dev \
&& rm -rf /var/lib/apt/lists/*


# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Copy application code
COPY . .


# Default command (can be overridden by docker-compose)
CMD ["python", "src/extract/fetch_daily.py"]