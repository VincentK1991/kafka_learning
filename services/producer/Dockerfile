FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    /root/.local/bin/uv --version
ENV PATH="/root/.local/bin:$PATH"

# Copy all required files first
COPY pyproject.toml /app/
COPY README.md /app/
COPY shared/ /app/shared/
COPY services/producer/ /app/services/producer/
COPY scripts/ /app/scripts/
COPY services/__init__.py /app/services/

# Create virtual environment and install dependencies with uv
RUN uv venv --python 3.12 && \
    uv sync

# Expose port
EXPOSE 8001

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

# Run the application
CMD [".venv/bin/python", "services/producer/producer_api.py"] 