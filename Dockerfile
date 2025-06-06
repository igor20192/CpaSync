FROM python:3.12-slim-bullseye

# Create a group and an application user
RUN groupadd -r appgroup && useradd --no-log-init -r -g appgroup appuser

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application and change the owner
COPY . .
RUN chown -R appuser:appgroup /app

# Switch to a non-root user
USER appuser

CMD ["python", "run.py", "--start-date", "2025-06-04", "--end-date", "2025-06-06"]