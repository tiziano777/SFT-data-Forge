FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app"

# Install system dependencies and tini for detached process management
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    python3-dev \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create required directories for paths defined in .env
RUN mkdir -p /app/nfs /app/logs

### Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir --no-deps datatrove
### INSTALL PLAYWRIGHT
# Install the Playwright Python package
RUN pip install playwright
# Download the required browsers and their system dependencies
RUN playwright install chromium
RUN playwright install-deps chromium
# --------------------------------

# Copy source code
COPY . .

# Editable install (required for internal imports)
RUN pip install -e .

EXPOSE 8501

# Healthcheck targeting the internal Streamlit endpoint
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# ENTRYPOINT with tini to avoid zombie processes from detached tasks
ENTRYPOINT ["/usr/bin/tini", "--"]

# Production-optimized command
CMD ["streamlit", "run", "dashboard.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--server.runOnSave=false", \
     "--browser.gatherUsageStats=false", \
     "--server.enableCORS=false"]
