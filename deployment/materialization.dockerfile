FROM python:3.13-slim

WORKDIR /app

# Add all code, ontologies for materialization and envfiles
# We exclude backup files as we will employ the remote DB
RUN mkdir ./kg_construction_and_validation
RUN mkdir ./ontologies
COPY --exclude=*.bak ./kg_construction_and_validation ./kg_construction_and_validation
COPY ./ontologies ./ontologies 
COPY ./deployment/virtuoso_deployment.env ./kg_construction_and_validation/.env
COPY ./pyproject.toml ./kg_construction_and_validation/pyproject.toml

# Deps
RUN apt update && apt install -y --no-install-recommends \
    curl \
    ca-certificates \
    build-essential \
    openjdk-21-jre-headless \
    nodejs \
    npm

# Docker-cli
# https://docs.docker.com/engine/install/debian/#install-using-the-repository
RUN apt install -y --no-install-recommends ca-certificates curl
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
RUN chmod a+r /etc/apt/keyrings/docker.asc
RUN echo "Types: deb\n\
URIs: https://download.docker.com/linux/debian\n\
Suites: $(. /etc/os-release && echo "$VERSION_CODENAME")\n\
Components: stable\n\
Signed-By: /etc/apt/keyrings/docker.asc" > /etc/apt/sources.list.d/docker.sources
# All that for this...
RUN apt update && apt install -y --no-install-recommends docker-ce-cli
RUN rm -rf /var/lib/apt/lists/*

# YARRRML-parser
RUN npm i -g @rmlio/yarrrml-parser

# uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /root/.local/bin/uvx /usr/local/bin/

# Setup
WORKDIR /app/kg_construction_and_validation
RUN uv sync --no-cache
ENV PYTHONUNBUFFERED=1

CMD ["sh", "-c", "sleep 120 && uv run main.py --db_option p"]