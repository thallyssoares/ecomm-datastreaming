FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt-bigquery (for dagster-dbt to parse manifest)
RUN pip install --no-cache-dir dbt-bigquery==1.7.4

# Copy project
COPY . .

# Non-root user
RUN useradd -m -u 1000 dagster && chown -R dagster:dagster /app
USER dagster

# Environment
ENV DAGSTER_HOME=/app/dagster_home

# Default command
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
