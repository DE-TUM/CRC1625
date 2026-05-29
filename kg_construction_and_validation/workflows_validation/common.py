import os
import uuid
from dataclasses import dataclass, field

from rdflib import Namespace, URIRef, Node, Literal
from rfc3987 import match

module_dir = os.path.dirname(__file__)
prefixes = open(os.path.join(module_dir, 'queries/prefixes.sparql')).read()

dw_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/") #data-workflows#
rdf_prefix = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
rdfs_prefix = Namespace("http://www.w3.org/2000/01/rdf-schema#")

def generate_unique_identifier():
    return uuid.uuid4().hex  # TODO change to UUID 7 on python 3.14


@dataclass
class BaseWorkflowElement:
    """
    Base class for anything belonging to a workflow
    """

    """
    Unique identifier of this element
    """
    iri: URIRef = ""

    """
    User-assigned name of this element (i.e., rdfs:label)
    """
    name: str = ""

    """
    User-assigned description of this element (i.e., rdfs:comment)
    """
    description: str = ""

    """
    Misc. provenance metadata, fully configurable by the end user as a dict of predicate iri -> object literals or iris.
    
    This can be used to, e.g., indicate creation dates, creators, etc. as needed
    """
    provenance_records: dict[URIRef, list[Node]] = field(default_factory=dict)

    def __hash__(self):
        if self.iri is None:
            self.create_new_iri()

        return hash(self.iri)

    def create_new_iri(self):
        self.iri = dw_prefix[generate_unique_identifier()]

    def set_option(self, k, v):
        if k != "provenance_records" and hasattr(self, k):
            attr = getattr(self, k)
        else:  # It goes directly into the provenance records
            (p_iri, obj) = v
            if p_iri not in self.provenance_records:
                self.provenance_records[p_iri] = list()
            self.provenance_records[p_iri].append(obj)

            return

        if isinstance(attr, list):
            attr.extend(v) if isinstance(v, list) else attr.append(v)
        elif isinstance(attr, set):
            attr.update(set(v)) if isinstance(v, list) else attr.add(v)
        elif isinstance(attr, dict) and isinstance(v, dict):
            attr.update(v)
        else:
            setattr(self, k, v)


base_workflow_element_iri_to_config_key = {
    str(rdfs_prefix.label): "name",
    str(rdfs_prefix.comment): "description",
}
