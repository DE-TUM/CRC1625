FROM python:3.13-slim

# Add all code and envfiles. We also copy the ontology files for the validation
# We exclude backup files as we will employ the remote DBs
WORKDIR /app
RUN mkdir ./kg_construction_and_validation
RUN mkdir ./ontologies
COPY --exclude=*.bak ./kg_construction_and_validation ./kg_construction_and_validation
COPY ./ontologies ./ontologies
COPY ./deployment/virtuoso_deployment.env ./kg_construction_and_validation/.env
COPY ./pyproject.toml ./kg_construction_and_validation/pyproject.toml

# Deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
   ca-certificates \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /root/.local/bin/uvx /usr/local/bin/

# Setup
WORKDIR /app/kg_construction_and_validation
RUN uv sync --no-cache
ENV PYTHONUNBUFFERED=1

CMD ["uv", "run", "run_handover_workflows_webui.py"]
