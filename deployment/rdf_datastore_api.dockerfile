FROM python:3.12-slim

WORKDIR /app

# Add all code and envfiles
# We exclude backup files as we will employ the remote DB. We also don't need ontology files for the API
#
# The virtuoso directory for file uploads is mounted via docker-compose
RUN mkdir ./kg_construction_and_validation
COPY --exclude=*.bak ./kg_construction_and_validation ./kg_construction_and_validation
COPY ./deployment/virtuoso_deployment.env ./kg_construction_and_validation/.env

RUN apt update && apt install -y --no-install-recommends \
    build-essential \
    openjdk-21-jre-headless \
    nodejs \
    npm

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

RUN python3 -m venv /opt/venv
RUN /opt/venv/bin/pip install --no-cache --upgrade pip setuptools
RUN /opt/venv/bin/pip install --no-cache-dir -r kg_construction_and_validation/requirements.txt

WORKDIR /app/kg_construction_and_validation

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

CMD ["python", "run_rdf_datastore_API.py", "--datastore", "virtuoso"]
