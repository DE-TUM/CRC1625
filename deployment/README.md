# CRC1625 Ontology and Knowledge Graph Construction system

This folder contains an example of the production deployment of the entire system via `docker-compose`.

This setup contains containers and configurations for:
- A **NiceGUI-based WebUI** for the SPARQL endpoint and the handover workflows validation system
- A **Nginx reverse proxy** for the WebUI
- The **materialization pipeline** configured to run against a remote, production DB
- An **scheduler for the materialization pipeline**, set to run every 15 minutes
- The **RDF API** for interacting with Virtuoso
- A **virtuoso** instance


## Requirements
- Set up certificates for your domain, store them inside a `certs` folder inside this folder and set them on the `nginx.conf` file. Alternatively, you can simply skip the proxy and publish the `8080` port form the WebUI.
- Set up a `virtuoso_deployment.env` file. A `virtuoso_deployment.env.example` is provided containing all connection details, without auth.
- Run `docker compose -f deployment/virtuoso_deployment.yml up --build -d` from the parent folder.