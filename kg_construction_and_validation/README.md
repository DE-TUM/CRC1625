# YARRRML mappings and workflows validation implementation for the CRC 1625's MatInf RDMS

This folder contains the following implementations:
- **A collection of SQL->RDF YARRRML mappings** used to convert the MatInf database into a Knowledge Graph
- **A fully functional API for performing experimental workflows validation** on the RDMS samples, their handovers and measurements, employing the Knowledge Graph
- **A collection of tests** for
  - The correctness of the YARRRML mappings, by comparing their results against handcrafted Knowledge Graphs with edge cases
  - The correctness of the workflow validation against manually crafted Knowledge Graph subsets with known results
- **A performance test** for the time and resource requirements of the mappings and any other additional steps (file upload, postprocessing...), employing a synthetic data generator for the MatInf database

# Table of Contents
1. [Requirements](#requirements)
2. [How to use](#Usage)
3. [Licensing](#licensing)


## Requirements
The whole Knowledge Graph creation and testing pipeline is designed to work under `Virtuoso` and `[WIP]`.

### General requirements
- `Python >= 3.13.7`: The required libraries are provided in `../requirements.txt`, using a `conda` environment with the `conda-forge` channel
- `Docker`: The scripts will set up local containers of the MSSQL DB for MatInf and, optionally, of `Oxigraph`.
- `RMLMapper` `RMLStreamer`: `.jar` executables of `RMLMapper` and `RMLStreamer` are needed. The `setup_files.sh` script can be used to retrieve and set them up.
- An `.env` file containing login and endpoint details. An example file can be found in `example.env`.

### `Virtuoso` requirements
The system requires a local `Virtuoso` docker container that:

- Employs the following [image](https://hub.docker.com/r/zenontum/virtuoso-sparql-samuel).

- Has as its container name `virtuoso_CRC_1625`

- Employs the [../virtuoso/data](../virtuoso/data) directory as the mountpoint for `/data` inside
the container. This is **required** as the system will store and attempt to upload serialized RDF triples from there.

- Exposes a SPARQL endpoint with read and write permissions at http://127.0.0.1:8891.

- Accepts ODBC requests at the `1111` port for a write-allowed user (configurable in the `.env` file).


#### The following configuration tweaks are **required**:
  - [Make virtuoso correctly treat untyped and typed xsd:string literals as the same during comparisons](https://github.com/openlink/virtuoso-opensource/issues/728#issuecomment-1937376203)

#### The following configuration tweaks are **recommended**:
  - General performance tweaks are also recommended, such as increasing its maximum allowed memory usage. The `virtuoso.ini` file we employed for all our experiments is also [available](../virtuoso/virtuoso.ini).

## Usage
The following CLI applications are offered, offering documentation with the `-h / --help` parameter:
- `run_rdf_datastore_API.py`: A lightweight HTTP API that communicates with an RDF datastore (e.g. Virtuoso) and serves requests to the rest of this system's modules. This module, alongside the underlying RDF datastore, can be located in a different system than the rest of the below modules and the SQL store. Their endpoints are controlled in the `.env` file.
- `main.py`: Executes the complete YARRRML mappings pipeline over a specified database backup. Note that the production database is not offered, but all DB backups used for testing are available.
- `run_mappings_output_test.py`: Performs a correctness test of the YARRRML mappings
- `run_handover_workflows_validation_test.py`: Performs an experimental workflows validation correctness test.
- `run_performance_test.py`: Performs a time and resource consumption for the KG creation pipeline. This script is based on a configuration file (`performance_test/runs_configuration.json`) that is already offered (and was used for the tests). If no file is provided, it will create one based on statistics of the objects in a production MatInf database dump.

The following Python modules and APIs are also available:
- `create_synthetic_records.py`: Creates a synthetic MatInf database that follows specified counts and probabilities of containing different objects.
- `workflows_validation/validation.py`: Manages the in-memory and RDF-backed representations of workflow models and their instances, and performs validation on the MatInf data using them.
