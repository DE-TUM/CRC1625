"""
Ensures that mapping declarations are consistent, in terms of:
    - Correctly parsed SQL queries
    - Perfectly matching YARRRML and SQL variable names
    - Correct YARRRML lists, in terms of numbers of elements and datatype/~iri indications

This avoids silent and very hard to debug errors, as YARRRML / rmlmapper will not emit warnings
opting in these cases, opting instead to simply not generate any triples.

In case an error is detected, it will be output during the materialization, although no errors will be triggered to
avoid false positives
"""
import logging
import re
import sys

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def validate_sql_scripts(mappings_name: str, sql_query: str):
    # Queries get shoved into one single line, so inline comments
    # destroy the query
    if "--" in sql_query:
        logging.error(f"{mappings_name}: Inline comment found in {sql_query}")


def extract_select_names(sql_query: str) -> list[str]:
    """
    (Roughly) extracts the SELECT variables of the SQL query
    """
    sql_query = re.sub(r"/\*.*?\*/", "", sql_query, flags=re.DOTALL)

    # Extract everything between SELECT and FROM
    select_part = re.search(r"select(.*?)from", sql_query, re.IGNORECASE | re.DOTALL)
    if not select_part:
        return []

    columns_raw = select_part.group(1)
    columns = []
    for col in columns_raw.split(","):
        col = col.strip()
        # Match "x AS y" or "x y"
        alias_match = re.search(r"(?:\bas\b)?\s*(\w+)$", col, re.IGNORECASE)
        if alias_match:
            columns.append(alias_match.group(1))
        else:
            columns.append(col)
    return columns

def extract_yarrrml_variables(yarrrml_mapping: str) -> list[str]:
    """
    Extracts all the varaible names used in the mapping
    """
    return re.findall(r"\$\((.*?)\)", yarrrml_mapping)

def validate_yarrrml_lists(mappings_name: str, yarrrml_mapping: str):
    matches = re.findall(r'\[.*?\]', yarrrml_mapping, re.DOTALL)
    for match in matches:
        list_of_strings = [item.strip() for item in match.strip('[]').split(',')]

        if len(list_of_strings) < 2 or len(list_of_strings) > 3:
            logging.error(f"{mappings_name}: Found a p-o list with incorrect number of entries: {list_of_strings}")
        if len(list_of_strings) == 3 and 'xsd' not in list_of_strings[2]:
            logging.error(f"{mappings_name}: Found a p-o list with non-datatype third entry: {list_of_strings}")
        elif len(list_of_strings) == 2 and '~iri' not in list_of_strings[1]:
            logging.error(f"{mappings_name}: Found a p-o list with non-iri second entry: {list_of_strings}")

def validate_variables(mappings_name: str, sql_query: str, yarrrml_mapping: str):
    """
    Ensures that there are no variables mismatches between the YARRRML mappings and their SQL queries
    """
    select_names = set(extract_select_names(sql_query))
    yarrrml_mappings = set(extract_yarrrml_variables(yarrrml_mapping))
    missing_mappings = [x for x in yarrrml_mappings if x not in select_names]
    if len(missing_mappings) > 0:
        logging.error(
            f"Non-matching mappings wrt. SQL in {mappings_name}: {missing_mappings}\nSELECT variables: {select_names}\nMapping names: {yarrrml_mappings}")

def validate_mapping(mappings_name: str, sql_query: str, yarrrml_mapping: str):
    """
    Prints an error if there are mapping variables in the YARRRML file that
    don't appear in the SQL query, in a case-sensitive way, references
    to entities not marked as iris or incorrect p-o lists

    This is needed because RML will fail silently and don't generate the
    corresponding triples (or generate them incorrectly) if this happens
    """
    validate_sql_scripts(mappings_name, sql_query)
    validate_yarrrml_lists(mappings_name, yarrrml_mapping)
    validate_variables(mappings_name, sql_query, yarrrml_mapping)


