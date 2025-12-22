#!/bin/sh

CONTEXT=$(pwd) \
DEPLOYMENT_PATH=$(pwd)/deployment \
VIRTUOSO_PATH=$(pwd)/virtuoso \
HANDOVER_WORKFLOWS_UI_DOCKERFILE=$(pwd)/deployment/webui.dockerfile \
RDF_DATASTORE_API_DOCKERFILE=$(pwd)/deployment/rdf_datastore_api.dockerfile \
MATERIALIZATION_DOCKERFILE=$(pwd)/deployment/materialization.dockerfile \
docker compose --env-file ./kg_construction_and_validation/.env -f deployment/virtuoso_deployment.yml up --build
