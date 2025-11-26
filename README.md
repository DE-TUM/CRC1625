<div align="center">

# Supplemental materials for Managing Materials Science Experimental Data and Workflows using Semantic Technologies

</div>

Welcome to the repository for the ontology and KG implementation for the [CRC 1625](https://www.ruhr-uni-bochum.de/crc1625/index.html.en)'s [MatInf](https://www.matinf.pro/) Research Data Management System.

This project is structured in three different main folders, each containing their respective **documentation**:
- [Knowledge Graph construction and validation](kg_construction_and_validation): SQL to RDF pipeline implementation and handover workflows validation system for MatInf databases
- [Ontologies](ontologies): CRC1625 ontology employed for representing the CRC1625 and MatInf data (`crc.ttl`), alongside all other ontologies employed (`pmd_core`, `oce` and `ChEBI`, indirectly through `oce`). Diagrams of the ontology are also present in the folder.
- [Virtuoso](virtuoso): Mountpoints for the (optional) `virtuoso` docker container.
- [Supplemental materials](supplemental_materials): This folder contains the performance test output log, all paper figures and code used to generate them, and additional figures (e.g., CPU and memory usage traces)

<div align="center">

[<img src="./kg_viz.webp" width="600" />](kg_viz.webp)

</div>

## Licensing
All code and any other supplemental materials is licensed under the GNU Affero General Public License v3.0
