
# Use a minimal Python image
FROM python:3.11-slim

# Install system dependencies for pyodbc (SQL Server)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    gnupg \
    unixodbc-dev \
    libssl-dev \
    libsasl2-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency list and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files into the image
COPY . .

# Set default command
CMD ["python", "main.py"]
