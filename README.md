<div align="center">

# CRC 1625 Ontology and Knowledge Graph implementation

</div>

> [!TIP]
> **You can now access the web interface as a CRC1625 (or demo) user at [kg.crc1625.mdi.ruhr-uni-bochum.de](https://kg.crc1625.mdi.ruhr-uni-bochum.de/)**

Welcome to the repository for the ontology and KG implementation for the [CRC 1625](https://www.ruhr-uni-bochum.de/crc1625/index.html.en)'s [MatInf](https://www.matinf.pro/) Research Data Management System.

This project is structured into six different main folders, each containing their respective **documentation**:
- [Knowledge Graph construction and validation](kg_construction_and_validation): SQL to RDF pipeline implementation and handover workflows validation system for MatInf databases
- [Ontologies](ontologies): CRC1625 ontology employed for representing the CRC1625 and MatInf data (`crc.ttl`), alongside all other ontologies employed (`pmd_core`, `oce` and `ChEBI`, indirectly through `oce`). Ontology diagram and other misc. figures used in publications and presentations are also present in the folder (`ontology.drawio`).
- [Virtuoso](virtuoso): Mountpoints for the (optional) `Virtuoso` docker container.
- [Qlever](qlever): Mountpoints for the (optional) `Qlever` docker container.
- [Supplemental materials](supplemental_materials): This folder contains the performance test output log, all paper figures and code used to generate them, and additional figures (e.g., CPU and memory usage traces)
- [Deployment](deployment): Deployment example of the KGC pipeline as an automated process, RDF store and additional systems (WebUI, endpoints...) via `docker-compose`

<div align="center">

[<img src="./kg_viz.webp" width="600" />](kg_viz.webp)

</div>

## Licensing
All code and documentation is licensed under the GNU Affero General Public License v3.0. 

The CRC 1625 ontology (`crc.ttl`) is licensed under the CC BY-SA 4.0.
