FROM amd64/alpine

WORKDIR /app

# Add all code, ontologies for materialization and envfiles
# We exclude backup files as we will employ the remote DB
RUN mkdir ./kg_construction_and_validation
RUN mkdir ./ontologies
COPY --exclude=*.bak ./kg_construction_and_validation ./kg_construction_and_validation
COPY ./ontologies ./ontologies 
COPY ./deployment/virtuoso_deployment.env ./kg_construction_and_validation/.env

RUN apk add --update --no-cache python3 py3-pip build-base python3-dev musl-dev linux-headers openjdk21 npm && ln -sf python3 /usr/bin/python

RUN python3 -m venv /opt/venv
RUN /opt/venv/bin/pip install --no-cache --upgrade pip setuptools
RUN /opt/venv/bin/pip install --no-cache-dir -r kg_construction_and_validation/requirements.txt
RUN npm i -g @rmlio/yarrrml-parser

WORKDIR /app/kg_construction_and_validation

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

CMD sh -c "sleep 120 && python main.py --db_option p"
