import argparse

from datastores.rdf.rdf_datastore_api import run, DatastoreType

parser = argparse.ArgumentParser()

parser.add_argument(
    '--datastore',
    type=str,
    choices=['virtuoso', 'qlever'],
    required=True,
    help="Select the RDF datastore backend to use. Possible options: 'virtuoso', 'qlever'"
)

args = parser.parse_args()

if args.datastore == 'virtuoso':
    run(DatastoreType.VIRTUOSO)
elif args.datastore == 'qlever':
    run(DatastoreType.QLEVER)
else:
    raise ValueError("Unknown RDF datastore type selected. Possible options: 'virtuoso', 'qlever'")


