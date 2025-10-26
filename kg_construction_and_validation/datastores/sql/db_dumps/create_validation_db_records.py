"""
Script used to generate the SQL records for EDX compositions in the validation DB

A refined version of this script is available at create_synthetic_records.py, used
to create synthetic DBs for performance evaluations
"""

from copy import copy

insert_query_object_info = """
INSERT INTO RUB_INF.dbo.ObjectInfo (ObjectId, TenantId,[_created],[_createdBy],[_updated],[_updatedBy],TypeId,RubricId,SortCode,AccessControl,IsPublished,ExternalId,ObjectName,ObjectNameUrl,ObjectFilePath,ObjectFileHash,ObjectDescription) VALUES
    {records};
"""

insert_query_object_link_object = """
INSERT INTO RUB_INF.dbo.ObjectLinkObject (ObjectId,LinkedObjectId,SortCode,[_created],[_createdBy],[_updated],[_updatedBy],LinkTypeObjectId) VALUES
    {records};
"""

# Measurement Area
insert_query_property_int = """
INSERT INTO RUB_INF.dbo.PropertyInt (ObjectId, SortCode, [_created], [_createdBy], [_updated], [_updatedBy], [Row], Value, PropertyName, Comment, SourceObjectId) VALUES
    {records};
"""

# Composition properties (x, y, tolerance)
insert_query_property_float = """
INSERT INTO RUB_INF.dbo.PropertyFloat (ObjectId, SortCode, [_created], [_createdBy], [_updated], [_updatedBy], [Row], Value, ValueEpsilon, PropertyName, Comment, SourceObjectId) VALUES
    {records};
"""
insert_query_sample = """
INSERT INTO RUB_INF.dbo.Sample (SampleId, ElemNumber, Elements) VALUES 
    {records};
"""

# Composition elements and pcts
insert_query_composition = """
INSERT INTO RUB_INF.dbo.Composition (CompositionId, SampleId, CompoundIndex, ElementName, ValueAbsolute, ValuePercent) VALUES 
    {records};
"""

record_object_info = "({object_id},1,'{date}',{user_id},'{date}',{user_id},8,NULL,0,0,1,NULL,{object_id},{object_id},{object_id},{object_id},N'Dummy Volume Composition {object_id}')"
record_object_link_object = "({src},{dst},0,'{date}',{user_id},'{date}',{user_id},NULL)"
record_object_property_int = "({object_id}, 0, '{date}', {user_id}, '{date}', {user_id}, 0, {value}, 'MeasurementArea', 'Dummy MeasurementArea', {object_id})"
record_object_property_float = "({object_id}, 0, '{date}', {user_id}, '{date}', {user_id}, 0, {value}, 0.0, '{property_name}', 'Dummy Float Property', {object_id})"
record_sample = "({object_id}, 2, 'Ag-Pt')"
record_composition = "({composition_id}, {object_id}, {compound_index}, '{element_name}', {value_absolute}, {value_percent})"

def get_records_object_info(object_id, date, user_id, records):
    for i in range(0, 342):
        replacements = {
            "{object_id}": object_id,
            "{date}": date,
            "{user_id}": user_id,
        }
        replaced_record = copy(record_object_info)
        for key, value in replacements.items():
            replaced_record = replaced_record.replace(key, str(value))
        records.append(replaced_record)

        object_id += 1

    return records, object_id

def get_records_object_link_object(src, dst, date, user_id, records):
    for i in range(0, 342):
        replacements = {
            "{src}": src,
            "{dst}": dst,
            "{date}": date,
            "{user_id}": user_id,
        }
        replaced_record = copy(record_object_link_object)
        for key, value in replacements.items():
            replaced_record = replaced_record.replace(key, str(value))
        records.append(replaced_record)

        dst += 1

    return records, dst

def get_records_property_int(object_id, date, user_id, records):
    for i in range(0, 342):
        replacements = {
            "{object_id}": object_id,
            "{date}": date,
            "{user_id}": user_id,
            "{value}": i+1
        }
        replaced_record = copy(record_object_property_int)
        for key, value in replacements.items():
            replaced_record = replaced_record.replace(key, str(value))
        records.append(replaced_record)

        object_id += 1

    return records, object_id

def get_records_property_float(object_id, date, user_id, records):
    for i in range(0, 342):
        replacements = {
            "{object_id}": object_id,
            "{date}": date,
            "{user_id}": user_id,
            "{value}": 1,
            "{property_name}": "x"
        }
        replaced_record = copy(record_object_property_float)
        for key, value in replacements.items():
            replaced_record = replaced_record.replace(key, str(value))
        records.append(replaced_record)

        replacements = {
            "{object_id}": object_id,
            "{date}": date,
            "{user_id}": user_id,
            "{value}": 2,
            "{property_name}": "y"
        }
        replaced_record = copy(record_object_property_float)
        for key, value in replacements.items():
            replaced_record = replaced_record.replace(key, str(value))
        records.append(replaced_record)

        replacements = {
            "{object_id}": object_id,
            "{date}": date,
            "{user_id}": user_id,
            "{value}": 3,
            "{property_name}": "Tolerance"
        }
        replaced_record = copy(record_object_property_float)
        for key, value in replacements.items():
            replaced_record = replaced_record.replace(key, str(value))
        records.append(replaced_record)

        object_id += 1

    return records, object_id


def get_records_sample(object_id, records):
    for i in range(0, 342):
        replacements = {
            "{object_id}": object_id,
        }
        replaced_record = copy(record_sample)
        for key, value in replacements.items():
            replaced_record = replaced_record.replace(key, str(value))
        records.append(replaced_record)

        object_id += 1

    return (records, object_id)


def get_records_composition(composition_id, object_id, records):
    for i in range(0, 342):
        replacements = {
            "{composition_id}": composition_id,
            "{object_id}": object_id,
            "{compound_index}": 0,
            "{element_name}": "Ag",
            "{value_absolute}": 0,
            "{value_percent}": 50.0
        }
        replaced_record = copy(record_composition)
        for key, value in replacements.items():
            replaced_record = replaced_record.replace(key, str(value))
        records.append(replaced_record)

        composition_id += 1

        replacements = {
            "{composition_id}": composition_id,
            "{object_id}": object_id,
            "{compound_index}": 30,
            "{element_name}": "Pt",
            "{value_absolute}": 0,
            "{value_percent}": 50.0
        }
        replaced_record = copy(record_composition)
        for key, value in replacements.items():
            replaced_record = replaced_record.replace(key, str(value))
        records.append(replaced_record)

        object_id += 1
        composition_id += 1

    return (records, composition_id, object_id)


# Composition objects
records_object_info = []
object_id = 20
# Sample 1
(records_object_info, object_id) = get_records_object_info(object_id,
                                                    "2025-02-08 09:43:52.533",
                                                    2,
                                                    records_object_info)
# Annealed Sample 1
(records_object_info, object_id) = get_records_object_info(object_id,
                                                    "2025-02-11 09:43:52.533",
                                                    2,
                                                    records_object_info)

# Sample -> Composition object links
records_object_link_object_ml_to_comp = []
object_id = 20
# Sample 1
(records_object_link_object_ml_to_comp, object_id) = get_records_object_link_object(1, object_id, "2025-02-08 09:43:52.533", 2, records_object_link_object_ml_to_comp)

# Annealed Sample 1
(records_object_link_object_ml_to_comp, object_id) = get_records_object_link_object(2, object_id, "2025-02-08 09:43:52.533", 2, records_object_link_object_ml_to_comp)


# Measurement -> Composition object links
records_object_link_object_measurement_to_comp = []
object_id = 20
# Sample 1
(records_object_link_object_measurement_to_comp, object_id) = get_records_object_link_object(13, object_id, "2025-02-08 09:43:52.533", 2, records_object_link_object_measurement_to_comp)

# Annealed Sample 1
(records_object_link_object_measurement_to_comp, object_id) = get_records_object_link_object(17, object_id, "2025-02-08 09:43:52.533", 2, records_object_link_object_measurement_to_comp)


# Measurement Area int properties on each composition object
records_property_int = []
object_id = 20
# Sample 1
(records_property_int, object_id) = get_records_property_int(object_id, "2025-02-08 09:43:52.533", 2, records_property_int)

# Annealed Sample 1
(records_property_int, object_id) = get_records_property_int(object_id, "2025-02-08 09:43:52.533", 2, records_property_int)


# x, y and tolerance float properties on each composition object
records_property_float = []
object_id = 20
# Sample 1
(records_property_float, object_id) = get_records_property_float(object_id, "2025-02-08 09:43:52.533", 2, records_property_float)

# Annealed Sample 1
(records_property_float, object_id) = get_records_property_float(object_id, "2025-02-08 09:43:52.533", 2, records_property_float)


# Entries of the compositions in the Sample table
records_sample = []
object_id = 20
# Sample 1
(records_sample, object_id) = get_records_sample(object_id, records_sample)

# Annealed Sample 1
(records_sample, object_id) = get_records_sample(object_id, records_sample)


# And finally entries of the compositions in the Composition table
records_composition = []
composition_id = 9
object_id = 20
# Sample 1
(records_composition, composition_id, object_id) = get_records_composition(composition_id, object_id, records_composition)

# Annealed Sample 1
(records_composition, composition_id, object_id) = get_records_composition(composition_id,object_id, records_composition)



with open("object_info.sql", "w") as f:
    f.write(insert_query_object_info.replace("{records}", ",\n".join(records_object_info)))

with open("object_link_object_ml_to_comp.sql", "w") as f:
    f.write(insert_query_object_link_object.replace("{records}", ",\n".join(records_object_link_object_ml_to_comp)))

with open("object_link_object_measurement_to_comp.sql", "w") as f:
    f.write(insert_query_object_link_object.replace("{records}", ",\n".join(records_object_link_object_measurement_to_comp)))

with open("property_int.sql", "w") as f:
    f.write(insert_query_property_int.replace("{records}", ",\n".join(records_property_int)))

with open("property_float.sql", "w") as f:
    f.write(insert_query_property_float.replace("{records}", ",\n".join(records_property_float)))

with open("sample.sql", "w") as f:
    f.write(insert_query_sample.replace("{records}", ",\n".join(records_sample)))

with open("composition.sql", "w") as f:
    f.write(insert_query_composition.replace("{records}", ",\n".join(records_composition)))

