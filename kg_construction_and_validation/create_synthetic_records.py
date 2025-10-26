"""
Script used to generate a synthetic SQL DB, with the following mechanism
    1. Create NUM_MAIN_SAMPLES samples, each containing up to MAX_PIECE_DEPTH *consecutive* pieces, with a
       CHANCE_TO_HAVE_PIECE to generate each one. They will be associated to one of NUM_SUBSTRATES random substrates,
       and have CHANCE_TO_HAVE_IDEA and CHANCE_TO_HAVE_REQUEST_FOR_SYNTHESIS to contain an idea and request for synthesis,
       respectively
    2. Each main sample and sample piece will have up to MAX_HANDOVERS_PER_SAMPLE, with a chance of CHANCE_TO_HAVE_HANDOVER
       to have each one.
    3. Each main sample and sample piece will have up to MAX_MEASUREMENTS_PER_MAIN_SAMPLE/MAX_MEASUREMENTS_PER_SAMPLE_PIECE with
       a chance of CHANCE_TO_HAVE_MEASUREMENT_IN_MAIN_SAMPLE/CHANCE_TO_HAVE_MEASUREMENT_IN_SAMPLE_PIECE, respectively. The
       measurements will be assigned at random to one of the previously generated handovers.
       The type IDs of the measurements are random, with EDX being an exception by having a CHANCE_FOR_EDX_MEASUREMENT
       before deciding to employ any other type. This is used to control the amount of EDX compositions present in the DB,
       as each EDX (csv) measurement will contain one.

    Every object will be assigned to a set of NUM_USERS, each within a one of NUM_PROJECTS within NUM_AREAS. The assignment
    of users follow logical behavior (e.g. the creator of a sample will also be the creator of the first handover, the
    creator of a handover will create the measurements associated to it, and each consecutive handover will be sent to
    a random user)

    By following the same distribution of chances of the original SQL DB, we can produce similar synthetic DBs or simulate
    a equivalent one with increased activity
"""
import logging
import random
import string
import sys
from string import Template
from datetime import datetime, timedelta

from faker import Faker
from tqdm import tqdm

from datastores.sql.sql_db import MSSQLDB

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

SQL_BATCH_SIZE = 1000
MAX_SQL_WORKERS = 8

NUM_USERS = 100
NUM_AREAS = 3
NUM_PROJECTS = 6

NUM_MAIN_SAMPLES = 1000000 # Main/Original samples, which are not pieces of any others
CHANCE_TO_HAVE_PIECE = 0.15 # Applies to both main samples and pieces
MAX_PIECE_DEPTH = 15

NUM_SUBSTRATES = 10
CHANCE_TO_HAVE_IDEA = 0.01
CHANCE_TO_HAVE_REQUEST_FOR_SYNTHESIS = 0.01

CHANCE_TO_HAVE_HANDOVER = 0.15
MAX_HANDOVERS_PER_SAMPLE = 15

CHANCE_TO_HAVE_MEASUREMENT_IN_MAIN_SAMPLE = 0.25
MAX_MEASUREMENTS_PER_MAIN_SAMPLE = 5

CHANCE_TO_HAVE_MEASUREMENT_IN_SAMPLE_PIECE = 0.25
MAX_MEASUREMENTS_PER_SAMPLE_PIECE = 5

CHANCE_FOR_EDX_MEASUREMENT = 0.05

global_object_id = 0
global_composition_id = 0
global_claim_id = 0
global_link_id = 0
global_property_int_id = 0
global_property_float_id = 0

measurements_created = 0
compositions_created = 0

valid_measurement_types = [
    15, 19, 53, 78, 79, # EDX (Excluding EDX_CSV with a composition, which we control via its own probability)
    17, 31, 44, 55, 56, 97, # XRD
    30, # XPS
    48, # LEIS
    27, 38, 39, 40, # Thickness
    24, # SEM
    14, 16, 33, # Resistance
    41, 80, 81, 82, # Bandgap
    20, # APT
    26, # TEM
    57, 58, 85, # SDC
    50, 59, 60, 86, 87, # SECCM
    47, # FIM
]

fake = Faker()

table_user = "RUB_INF.dbo.AspNetUsers"
headers_user = "Id, UserName, NormalizedUserName, Email, NormalizedEmail, EmailConfirmed, PasswordHash, SecurityStamp, ConcurrencyStamp, PhoneNumber, PhoneNumberConfirmed, TwoFactorEnabled, LockoutEnd, LockoutEnabled, AccessFailedCount"
record_user = Template("${user_id}, ${user_name}, ${user_name}, ${user_name}@example.com, ${user_name}@example.com, 1, , , , , 0, 0, , 0, 0")

table_claim = "RUB_INF.dbo.AspNetUserClaims"
headers_claim = "Id, UserId, ClaimType, ClaimValue"
record_claim = Template("${claim_id}, ${user_id}, ${claim_type}, ${claim_value}")

table_object_info = "RUB_INF.dbo.ObjectInfo"
headers_object_info = "ObjectId, TenantId,[_created],[_createdBy],[_updated],[_updatedBy],TypeId,RubricId,SortCode,AccessControl,IsPublished,ExternalId,ObjectName,ObjectNameUrl,ObjectFilePath,ObjectFileHash,ObjectDescription"
record_object_info = Template("${object_id},1,${date},${user_id},${date},${user_id},${type_id},0,0,0,1,${object_id},${object_id},${object_id},${object_id},${object_id},${object_id}")

table_sample = "RUB_INF.dbo.Sample"
headers_sample = "SampleId, ElemNumber, Elements"
record_sample = Template("${object_id}, ${n_elements}, ${elements}")

table_link_object = "RUB_INF.dbo.ObjectLinkObject"
headers_link_object = "ObjectLinkObjectId, ObjectId,LinkedObjectId,SortCode,[_created],[_createdBy],[_updated],[_updatedBy],LinkTypeObjectId"
record_link_object = Template("${link_id}, ${src}, ${dst}, 0, ${date}, ${user_id}, ${date}, ${user_id}, 0")

table_handover = "RUB_INF.dbo.Handover"
headers_handover = "HandoverId, SampleObjectId, DestinationUserId, DestinationConfirmed, DestinationComments, Json, Amount, MeasurementUnit"
record_handover = Template("${handover_id}, ${sample_id}, ${dest_user}, ${confirmed_date}, Dummy handover comments, , 0, ")

table_property_int = "RUB_INF.dbo.PropertyInt"
headers_property_int = "PropertyIntId, ObjectId, SortCode, [_created], [_createdBy], [_updated], [_updatedBy], [Row], Value, PropertyName, Comment, SourceObjectId"
record_property_int = Template("${property_id}, ${object_id}, 0, ${date}, ${user_id}, ${date}, ${user_id}, 0, ${value}, ${property_name}, Dummy Int Property, ${object_id}")

table_property_float = "RUB_INF.dbo.PropertyFloat"
headers_property_float = "PropertyFloatId, ObjectId, SortCode, [_created], [_createdBy], [_updated], [_updatedBy], [Row], Value, ValueEpsilon, PropertyName, Comment, SourceObjectId"
record_property_float = Template("${property_id}, ${object_id}, 0, ${date}, ${user_id}, ${date}, ${user_id}, 0, ${value}, 0.0, ${property_name}, Dummy Float Property, ${object_id}")

table_composition = "RUB_INF.dbo.Composition"
headers_composition = "CompositionId, SampleId, CompoundIndex, ElementName, ValueAbsolute, ValuePercent"
record_composition = Template("${composition_id}, ${object_id}, ${compound_index}, ${element_name}, ${value_absolute}, ${value_percent}")

def apply_replacements(replacements, record: Template):
    return record.substitute(replacements)

def create_projects_list():
    project_letters = list(string.ascii_uppercase)[:NUM_AREAS]

    projects = []
    projects_per_letter = int(NUM_PROJECTS / len(project_letters))
    created_projects = 0
    for project_letter in project_letters:
        for project_number in range(1, projects_per_letter + 1):
            projects.append(project_letter + str(project_number))
            created_projects += 1
            if created_projects == NUM_PROJECTS:
                break
        if created_projects == NUM_PROJECTS:
            break

    return projects


def get_user_records(user_id, user_name, first_name, last_name, project):
    """
    Adds a user and their claims
    """
    global global_claim_id

    replacements = {
        "user_name": user_name,
        "user_id": user_id,
    }

    user_record = apply_replacements(replacements, record_user)

    claim_records = []

    global_claim_id += 1
    replacements = {
        "claim_id": global_claim_id,
        "user_id": user_id,
        "claim_type": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
        "claim_value": user_name,
    }
    claim_records.append(apply_replacements(replacements, record_claim))

    global_claim_id += 1
    replacements = {
        "claim_id": global_claim_id,
        "user_id": user_id,
        "claim_type": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname",
        "claim_value": first_name,
    }
    claim_records.append(apply_replacements(replacements, record_claim))

    global_claim_id += 1
    replacements = {
        "claim_id": global_claim_id,
        "user_id": user_id,
        "claim_type": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname",
        "claim_value": last_name,
    }
    claim_records.append(apply_replacements(replacements, record_claim))

    global_claim_id += 1
    replacements = {
        "claim_id": global_claim_id,
        "user_id": user_id,
        "claim_type": "Project",
        "claim_value": project,
    }
    claim_records.append(apply_replacements(replacements, record_claim))

    return user_record, claim_records


def create_users_and_projects():
    logging.info("Creating users...")

    projects = create_projects_list()

    all_user_records = []
    all_user_claims = []
    added_user_names = set()  # They must be unique too

    for user_id in range(1, NUM_USERS + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()
        user_name = " ".join([first_name, last_name])
        while user_name in added_user_names:  # Prevent duplicates
            first_name = fake.first_name()
            last_name = fake.last_name()
            user_name = " ".join([first_name, last_name])

        added_user_names.add(user_name)

        (user_record, user_claim_records) = get_user_records(user_id,
                                                             user_name,
                                                             first_name,
                                                             last_name,
                                                             random.choice(projects))
        all_user_records.append(user_record)
        all_user_claims += user_claim_records

    logging.info("Executing transactions...")
    sql_db = MSSQLDB()
    sql_db.execute_bulk_insert(table_user, headers_user, all_user_records)
    sql_db.execute_bulk_insert(table_claim, headers_claim, all_user_claims)


def generate_random_chemical_formula():
    """
    Get a random chemical formula string alongside its number of elements, as expected by the RDMS
    Note: the ordering of elements may be nonsensical
    """

    # We only consider the established 9 metals to be studied
    allowed_elements = ["Ag", "Au", "Cu", "Ir", "Pd", "Pt", "Re", "Rh", "Ru"]

    # And we also assume we are always working with quinary systems (otherwise, choosing
    # random numbers of elements can introduce too much variability)
    random_elements = random.sample(allowed_elements, 5) #random.randint(2, 5))

    # In the DB, elements are in the form of '-Au-Pd-...-'
    elements_string = "-"+"-".join(random_elements)+"-"
    return len(random_elements), elements_string


def create_samples_and_pieces():
    global global_object_id
    global global_link_id

    object_info_records = []
    sample_records = []
    link_records = []
    # Traces of pieces
    samples_and_pieces = []
    samples_and_pieces_created = 0

    substrates = []

    # Create substrates
    for _ in (pbar := tqdm(range(1, NUM_SUBSTRATES + 1))):
        pbar.set_description(f"Generating substrate")
        global_object_id += 1

        year = random.randint(2025, 2050)
        month = random.randint(1, 12)
        creation_date = datetime(year, month, 1)

        replacements = {
            "object_id": global_object_id,
            "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": random.choice(range(1, NUM_USERS + 1)),
            "type_id": 5
        }
        object_info_records.append(apply_replacements(replacements, record_object_info))

        substrates.append(global_object_id)

    # Create samples and pieces
    for _ in (pbar := tqdm(range(1, NUM_MAIN_SAMPLES + 1))):
        samples_and_pieces_created += 1
        global_object_id += 1

        pbar.set_description(f"Generating random sample")

        creator_id = random.choice(range(1, NUM_USERS + 1))
        year = random.randint(2025, 2050)
        month = random.randint(1, 12)
        creation_date = datetime(year, month, 1)

        idea_id = None
        if random.random() < CHANCE_TO_HAVE_IDEA:
            # Create an idea, and link all samples and pieces to it
            replacements = {
                "object_id": global_object_id,
                "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "user_id": creator_id,
                "type_id": 89
            }
            object_info_records.append(apply_replacements(replacements, record_object_info))

            idea_id = global_object_id

            global_object_id += 1

        synthesis_req_id = None
        if random.random() < CHANCE_TO_HAVE_REQUEST_FOR_SYNTHESIS:
            # Create a synthesis request, and link the main sample to it
            replacements = {
                "object_id": global_object_id,
                "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "user_id": creator_id,
                "type_id": 83
            }
            object_info_records.append(apply_replacements(replacements, record_object_info))

            synthesis_req_id = global_object_id

            if idea_id is not None:
                # Also link it to the synthesis request
                global_link_id += 1
                replacements = {
                    "link_id": global_link_id,
                    "src": idea_id,
                    "dst": synthesis_req_id,
                    "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    "user_id": creator_id,
                }
                link_records.append(apply_replacements(replacements, record_link_object))

            global_object_id += 1

        # Add the main sample
        replacements = {
            "object_id": global_object_id,
            "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
            "type_id": 6
        }
        object_info_records.append(apply_replacements(replacements, record_object_info))

        # Link it with a substrate
        global_link_id += 1
        replacements = {
            "link_id": global_link_id,
            "src": global_object_id,
            "dst": random.choice(substrates),
            "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
        }
        link_records.append(apply_replacements(replacements, record_link_object))

        if idea_id is not None:
            global_link_id += 1
            replacements = {
                "link_id": global_link_id,
                "src": idea_id,
                "dst": global_object_id,
                "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "user_id": creator_id,
            }
            link_records.append(apply_replacements(replacements, record_link_object))

        if synthesis_req_id is not None:
            global_link_id += 1
            replacements = {
                "link_id": global_link_id,
                "src": synthesis_req_id,
                "dst": global_object_id,
                "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "user_id": creator_id,
            }
            link_records.append(apply_replacements(replacements, record_link_object))

        # And add a composition to it
        n_elements, formula = generate_random_chemical_formula()
        replacements = {
            "object_id": global_object_id,
            "n_elements": n_elements,
            "elements": formula,
        }
        sample_records.append(apply_replacements(replacements, record_sample))

        samples_and_pieces.append((global_object_id, creator_id, n_elements, formula, creation_date, []))

        n_attachments = 0
        while random.random() < CHANCE_TO_HAVE_PIECE and n_attachments < MAX_PIECE_DEPTH:
            samples_and_pieces_created += 1
            n_attachments += 1
            global_object_id += 1

            (_, _, _, _, _ , pieces_list) = samples_and_pieces[-1]
            pieces_list.append(global_object_id)

            # Each piece is created one day later than its preceding sample / piece
            creation_date += timedelta(days=1)

            replacements = {
                "object_id": global_object_id,
                "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "user_id": creator_id,
                "type_id": 6
            }
            object_info_records.append(apply_replacements(replacements, record_object_info))

            # Link it with a substrate
            global_link_id += 1
            replacements = {
                "link_id": global_link_id,
                "src": global_object_id,
                "dst": random.choice(substrates),
                "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "user_id": creator_id,
            }
            link_records.append(apply_replacements(replacements, record_link_object))

            if idea_id is not None:
                global_link_id += 1
                replacements = {
                    "link_id": global_link_id,
                    "src": idea_id,
                    "dst": global_object_id,
                    "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    "user_id": creator_id,
                }
                link_records.append(apply_replacements(replacements, record_link_object))

            # And add a composition to it
            replacements = {
                "object_id": global_object_id,
                "n_elements": n_elements,
                "elements": formula,
            }
            sample_records.append(apply_replacements(replacements, record_sample))

            # Link the parent sample to the new piece
            global_link_id += 1
            replacements = {
                "link_id": global_link_id,
                "src": global_object_id - 1,
                "dst": global_object_id,
                "date": creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "user_id": creator_id,
            }
            link_records.append(apply_replacements(replacements, record_link_object))

    logging.info("Executing transactions...")
    sql_db = MSSQLDB()
    sql_db.execute_bulk_insert(table_object_info, headers_object_info, object_info_records)
    sql_db.execute_bulk_insert(table_sample, headers_sample, sample_records)
    sql_db.execute_bulk_insert(table_link_object, headers_link_object, link_records)

    return samples_and_pieces_created, samples_and_pieces


def generate_volume_compositions(sample_id,
                                 sample_n_elements,
                                 sample_formula,
                                 creator_id,
                                 measurement_id,
                                 measurement_creation_date,
                                 object_info_records,
                                 object_link_records,
                                 property_int_records,
                                 property_float_records,
                                 sample_records,
                                 composition_records):
    global global_object_id
    global global_link_id
    global global_composition_id
    global global_property_int_id
    global global_property_float_id

    # 342 Composition objects attached to the sample and the measurement
    for i in range(1, 342 + 1):
        global_object_id += 1
        replacements = {
            "object_id": global_object_id,
            "date": measurement_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
            "type_id": 8 # Composition
        }
        object_info_records.append(apply_replacements(replacements, record_object_info))

        # Sample -> Composition
        global_link_id += 1
        replacements = {
            "link_id": global_link_id,
            "src": sample_id,
            "dst": global_object_id,
            "date": measurement_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
        }
        object_link_records.append(apply_replacements(replacements, record_link_object))

        # Measurement -> Composition
        global_link_id += 1
        replacements = {
            "link_id": global_link_id,
            "src": measurement_id,
            "dst": global_object_id,
            "date": measurement_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
        }
        object_link_records.append(apply_replacements(replacements, record_link_object))

        # Measurement Area int property on each composition object
        global_property_int_id += 1
        replacements = {
            "property_id": global_property_int_id,
            "object_id": global_object_id,
            "date": measurement_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
            "property_name": "Measurement Area",
            "value": i
        }
        property_int_records.append(apply_replacements(replacements, record_property_int))

        # x, y and Tolerance float properties on each composition object
        global_property_float_id += 1
        replacements = {
            "property_id": global_property_float_id,
            "object_id": global_object_id,
            "date": measurement_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
            "property_name": "x",
            "value": random.uniform(0.0, 341.99) # Doesn't matter
        }
        property_float_records.append(apply_replacements(replacements, record_property_float))
        global_property_float_id += 1
        replacements = {
            "property_id": global_property_float_id,
            "object_id": global_object_id,
            "date": measurement_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
            "property_name": "y",
            "value": random.uniform(0.0, 341.99) # Doesn't matter
        }
        property_float_records.append(apply_replacements(replacements, record_property_float))
        global_property_float_id += 1
        replacements = {
            "property_id": global_property_float_id,
            "object_id": global_object_id,
            "date": measurement_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
            "property_name": "Tolerance",
            "value": random.uniform(0.0, 1000.0) # Doesn't matter
        }
        property_float_records.append(apply_replacements(replacements, record_property_float))

        replacements = {
            "object_id": global_object_id,
            "n_elements": sample_n_elements,
            "elements": sample_formula,
        }
        sample_records.append(apply_replacements(replacements, record_sample))

        for element in sample_formula[1:-1].split('-'):
            global_composition_id += 1
            replacements = {
                "composition_id": global_composition_id,
                "object_id": global_object_id,

                # These values don't matter either
                "compound_index": 0,
                "element_name": element,
                "value_absolute": 0,
                "value_percent": 100.0 / sample_n_elements
            }
            composition_records.append(apply_replacements(replacements, record_composition))




def generate_measurement_for_handovers(sample_id,
                                       sample_n_elements,
                                       sample_formula,
                                       sample_handovers,
                                       object_info_records,
                                       object_link_records,
                                       property_int_records,
                                       property_float_records,
                                       sample_records,
                                       composition_records):
    global global_object_id
    global global_link_id
    global measurements_created
    global compositions_created

    measurements_created += 1

    # Pick a handover date at random, and make it so that the measurement is taken one second later than the handover
    (handover_creation_date, receiving_user_id) = random.choice(sample_handovers)
    handover_creation_date += timedelta(seconds=1)

    if random.random() < CHANCE_FOR_EDX_MEASUREMENT:
        type_id = 13 # It will cause the creation of a composition
    else:
        type_id = random.choice(valid_measurement_types) # It doesn't matter much which one it is

    global_object_id += 1

    replacements = {
        "object_id": global_object_id,
        "date": handover_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
        # The measurement is done by whoever received the handover,
        # so that it's attached to it
        "user_id": receiving_user_id,
        "type_id": type_id
    }
    object_info_records.append(apply_replacements(replacements, record_object_info))

    global_link_id += 1
    replacements = {
        "link_id": global_link_id,
        "src": sample_id,
        "dst": global_object_id,
        "date": handover_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
        "user_id": receiving_user_id,
    }
    object_link_records.append(apply_replacements(replacements, record_link_object))

    if type_id == 13: ## Create a composition too
        compositions_created += 1
        generate_volume_compositions(sample_id,
                                     sample_n_elements,
                                     sample_formula,
                                     receiving_user_id,
                                     global_object_id,
                                     handover_creation_date,

                                     object_info_records,
                                     object_link_records,
                                     property_int_records,
                                     property_float_records,
                                     sample_records,
                                     composition_records)

def generate_handovers_for_sample(sample_creation_date,
                                  creator_id,
                                  handover_records,
                                  object_info_records,
                                  sample_id):
    global global_object_id


    sample_handovers = []
    sample_handovers.append((sample_creation_date, creator_id))  # Represents initial work

    n_handovers = 0
    while random.random() < CHANCE_TO_HAVE_HANDOVER and n_handovers < MAX_HANDOVERS_PER_SAMPLE:
        global_object_id += 1

        receiving_user_id = random.choice(range(1, NUM_USERS + 1))

        # Similarly to samples wrt. pieces, each handover is created a
        # day after the previous one or after the sample's creation date
        sample_creation_date += timedelta(days=1)
        replacements = {
            "object_id": global_object_id,
            "date": sample_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "user_id": creator_id,
            "type_id": -1
        }
        object_info_records.append(apply_replacements(replacements, record_object_info))

        replacements = {
            "handover_id": global_object_id,
            "sample_id": sample_id,
            "dest_user": receiving_user_id,
            "confirmed_date": sample_creation_date.strftime('%Y-%m-%dT%H:%M:%S'),
        }
        handover_records.append(apply_replacements(replacements, record_handover))

        sample_handovers.append((sample_creation_date, receiving_user_id))

        n_handovers += 1

    return sample_handovers


def create_handovers_and_measurements(samples_and_pieces):
    global global_object_id
    global measurements_created
    global compositions_created

    object_info_records = []
    handover_records = []
    object_link_records = []
    property_int_records = []
    property_float_records = []
    sample_records = []
    composition_records = []

    for (sample_id, creator_id, n_elements, formula, creation_date, pieces) in \
            (pbar := tqdm(samples_and_pieces, total=len(samples_and_pieces))):
        pbar.set_description(f"Adding handovers and measurements to sample and pieces of main sample")

        sample_handovers = generate_handovers_for_sample(creation_date,
                                  creator_id,
                                  handover_records,
                                  object_info_records,
                                  sample_id)

        n_measurements = 0
        while random.random() < CHANCE_TO_HAVE_MEASUREMENT_IN_MAIN_SAMPLE and n_measurements < MAX_MEASUREMENTS_PER_MAIN_SAMPLE:
            generate_measurement_for_handovers(sample_id,
                                                n_elements,
                                                formula,
                                                sample_handovers,
                                                object_info_records,
                                                object_link_records,
                                                property_int_records,
                                                property_float_records,
                                                sample_records,
                                                composition_records)
            n_measurements += 1

        # Also for its pieces!
        for piece_id in pieces:
            piece_handovers = generate_handovers_for_sample(creation_date,
                                  creator_id,
                                  handover_records,
                                  object_info_records,
                                  piece_id)

            n_measurements = 0
            while random.random() < CHANCE_TO_HAVE_MEASUREMENT_IN_SAMPLE_PIECE and n_measurements < MAX_MEASUREMENTS_PER_SAMPLE_PIECE:
                generate_measurement_for_handovers(piece_id,
                                                   n_elements,
                                                   formula,
                                                   piece_handovers,
                                                   object_info_records,
                                                   object_link_records,
                                                   property_int_records,
                                                   property_float_records,
                                                   sample_records,
                                                   composition_records)
                n_measurements += 1

    logging.info("Executing transactions...")
    sql_db = MSSQLDB()
    sql_db.execute_bulk_insert(table_object_info, headers_object_info, object_info_records)
    sql_db.execute_bulk_insert(table_handover, headers_handover, handover_records)
    sql_db.execute_bulk_insert(table_link_object, headers_link_object, object_link_records)
    sql_db.execute_bulk_insert(table_property_int, headers_property_int, property_int_records)
    sql_db.execute_bulk_insert(table_property_float, headers_property_float, property_float_records)
    sql_db.execute_bulk_insert(table_sample, headers_sample, sample_records)
    sql_db.execute_bulk_insert(table_composition, headers_composition, composition_records)


def create_synthetic_records(
    num_users: int,
    num_areas: int,
    num_projects: int,
    num_main_samples: int,
    chance_to_have_piece: float,
    max_piece_depth: int,
    num_substrates: int,
    chance_to_have_idea: float,
    chance_to_have_request_for_synthesis: float,
    chance_to_have_handover: float,
    max_handovers_per_sample: int,
    chance_to_have_measurement_in_main_sample: float,
    max_measurements_per_main_sample: int,
    chance_to_have_measurement_in_sample_piece: float,
    max_measurements_per_sample_piece: int,
    chance_for_EDX_measurement: float
):
    """
    Creates a synthetic SQL database following the indicated distribution of probabilities and maximum values (see the
    module's documentation for more information)
    """
    global global_object_id
    global global_composition_id
    global global_claim_id
    global global_link_id
    global global_property_int_id
    global global_property_float_id
    global measurements_created
    global compositions_created

    global NUM_USERS, NUM_AREAS, NUM_PROJECTS
    global NUM_MAIN_SAMPLES, CHANCE_TO_HAVE_PIECE, MAX_PIECE_DEPTH, NUM_SUBSTRATES
    global CHANCE_TO_HAVE_IDEA, CHANCE_TO_HAVE_REQUEST_FOR_SYNTHESIS
    global CHANCE_TO_HAVE_HANDOVER, MAX_HANDOVERS_PER_SAMPLE
    global CHANCE_TO_HAVE_MEASUREMENT_IN_MAIN_SAMPLE, MAX_MEASUREMENTS_PER_MAIN_SAMPLE
    global CHANCE_TO_HAVE_MEASUREMENT_IN_SAMPLE_PIECE, MAX_MEASUREMENTS_PER_SAMPLE_PIECE
    global CHANCE_FOR_EDX_MEASUREMENT

    global_object_id = 0
    global_composition_id = 0
    global_claim_id = 0
    global_link_id = 0
    global_property_int_id = 0
    global_property_float_id = 0
    measurements_created = 0
    compositions_created = 0

    NUM_USERS = num_users
    NUM_AREAS = num_areas
    NUM_PROJECTS = num_projects
    NUM_MAIN_SAMPLES = num_main_samples
    CHANCE_TO_HAVE_PIECE = chance_to_have_piece
    MAX_PIECE_DEPTH = max_piece_depth
    NUM_SUBSTRATES = num_substrates
    CHANCE_TO_HAVE_IDEA = chance_to_have_idea
    CHANCE_TO_HAVE_REQUEST_FOR_SYNTHESIS = chance_to_have_request_for_synthesis
    CHANCE_TO_HAVE_HANDOVER = chance_to_have_handover
    MAX_HANDOVERS_PER_SAMPLE = max_handovers_per_sample
    CHANCE_TO_HAVE_MEASUREMENT_IN_MAIN_SAMPLE = chance_to_have_measurement_in_main_sample
    MAX_MEASUREMENTS_PER_MAIN_SAMPLE = max_measurements_per_main_sample
    CHANCE_TO_HAVE_MEASUREMENT_IN_SAMPLE_PIECE = chance_to_have_measurement_in_sample_piece
    MAX_MEASUREMENTS_PER_SAMPLE_PIECE = max_measurements_per_sample_piece
    CHANCE_FOR_EDX_MEASUREMENT = chance_for_EDX_measurement

    create_users_and_projects()
    samples_and_pieces_created, samples_and_pieces = create_samples_and_pieces()
    sample_objects_created = global_object_id
    logging.info(f"Generated {samples_and_pieces_created} samples and pieces. Total objects created: {sample_objects_created}")

    create_handovers_and_measurements(samples_and_pieces)
    hnd_measurement_objects_created = global_object_id - sample_objects_created
    logging.info(f"Generated {measurements_created} measurements, of which there are {compositions_created} compositions. Total objects created: {hnd_measurement_objects_created}")