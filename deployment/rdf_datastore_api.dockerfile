FROM amd64/alpine

WORKDIR /app

# Add all code and envfiles
# We exclude backup files as we will employ the remote DB. We also don't need ontology files for the API
#
# The virtuoso directory for file uploads is mounted via docker-compose
RUN mkdir ./kg_construction_and_validation
COPY --exclude=*.bak ./kg_construction_and_validation ./kg_construction_and_validation
COPY ./deployment/virtuoso_deployment.env ./kg_construction_and_validation/.env

RUN apk add --update --no-cache python3 py3-pip build-base python3-dev musl-dev linux-headers docker && ln -sf python3 /usr/bin/python

RUN python3 -m venv /opt/venv
RUN /opt/venv/bin/pip install --no-cache --upgrade pip setuptools
RUN /opt/venv/bin/pip install --no-cache-dir -r kg_construction_and_validation/requirements.txt

WORKDIR /app/kg_construction_and_validation

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

CMD ["python", "run_rdf_datastore_API.py", "--datastore", "virtuoso"]
