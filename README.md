<div align="center">

# CRC1625 Ontology and Knowledge Graph Construction system

</div>

Welcome to the repository for the ontology and KG implementation for the [CRC 1625](https://www.ruhr-uni-bochum.de/crc1625/index.html.en)'s [MatInf](https://www.matinf.pro/) Research Data Management System.

This project is structured in three different main folders, each containing their respective **documentation**:
- [Knowledge Graph construction and validation](kg_construction_and_validation): SQL to RDF pipeline implementation and handover workflows validation system for MatInf databases
- [Ontologies](ontologies): CRC1625 ontology employed for representing the CRC1625 and MatInf data (`crc.ttl`), alongside all other ontologies employed (`pmd_core`, `oce` and `ChEBI`, indirectly through `oce`). Diagrams of the ontology are also present in the folder.
- [Virtuoso](virtuoso): Mountpoints for the (optional) `virtuoso` docker container.
- [Performance test plots](performance_test_results): Perofrmance test output files and notebook used for generating the figures in the paper.


<div align="center">

[<img src="./kg_viz.webp" width="600" />](header_image.svg)

</div>

<div align="center">
    <img src="https://img.shields.io/badge/We_love-Pommi-blue"/>
</div>
