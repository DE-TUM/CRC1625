import os
from rdflib import URIRef
from datastores.rdf import rdf_datastore_client

module_dir = os.path.dirname(__file__)
prefixes: str = open(os.path.join(module_dir, '../workflows_validation/queries/prefixes.sparql')).read()
get_first_handover_group_iri_from_sample_object_id = prefixes + open(
    os.path.join(module_dir, 'queries/get_first_handover_group_iri_from_sample_object_id.sparql'), 'r').read()
get_sample_object_id_from_first_handover_group_iri = prefixes + open(
    os.path.join(module_dir, 'queries/get_sample_object_id_from_first_handover_group_iri.sparql'), 'r').read()


async def get_sample_object_id_of_handover_group_iri(handover_group_iri: URIRef) -> int:
    result = await rdf_datastore_client.launch_query(get_sample_object_id_from_first_handover_group_iri.replace("{handover_group_iri}", handover_group_iri))
    results = result["results"]["bindings"]
    if len(results) == 1:
        return int(results[0]["internal_id"]["value"])
    else:
        return -1

async def get_handover_group_iri_of_sample_object_id(internal_id: int) -> URIRef:
    result = await rdf_datastore_client.launch_query(get_first_handover_group_iri_from_sample_object_id.replace("{internal_id}", str(internal_id)))
    results = result["results"]["bindings"]
    if len(results) == 1:
        return URIRef(results[0]["handover_group_iri"]["value"])
    else:
        return URIRef("")