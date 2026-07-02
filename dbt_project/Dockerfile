FROM python:3.11-slim

WORKDIR /app

# Install dbt-bigquery
RUN pip install --no-cache-dir \
    dbt-bigquery==1.7.4 \
    dbt-expectations==0.10.2 \
    dbt-utils==1.1.1

# Copy project
COPY . .

# Non-root user
RUN useradd -m -u 1000 dbtuser && chown -R dbtuser:dbtuser /app
USER dbtuser

# Default command
CMD ["dbt", "debug"]
