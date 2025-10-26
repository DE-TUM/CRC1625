"""
Module used to fill the query and other optional parameters in a YARRRML mapping file
"""
import os

from datastores.sql.sql_db import MSSQLDB
from .validate_mappings_consistency import validate_mapping

module_dir = os.path.dirname(__file__)

prefixes = open(os.path.join(module_dir, 'prefixes.yml')).read()

mssql_source = """
    access: jdbc:sqlserver://localhost:1433;databaseName=RUB_INF;encrypt=true;trustServerCertificate=true
    type: mssql
    credentials:
      username: SA
      password: DebugPassword123@
    queryFormulation: mssql
    query: |
      {query}
    referenceFormulation: csv
"""

csv_source = """
    access: {csv_file}
    referenceFormulation: csv
"""

def get_sql_query(file_name,
                  custom_sql_template: dict[str, list[str]] | None,
                  replacement_i: int | None,
                  replace_with_view=True) -> str | tuple[str, str]:
    """
    Retrieves the corresponding SQL query file for a YARRRML mapping file path, and returns it as a string.

    It optionally accepts a custom_sql_template parameter, composed of replacement keys and corresponding lists of possible replacement values.
    If so, replacement_i must dictate the index of the list to pick the replacement value from.
    These two parameters are currently used to generate SQL queries discriminating against different type IDs in the RDMS
    """
    with open(file_name.replace("_templated", "").replace(".yml", ".sql"), 'r') as f:
        lines = f.readlines()

        query = lines[0] + ''.join('          ' + line if line.strip() else line for line in lines[1:])

        if custom_sql_template is not None:
            for key, val in custom_sql_template.items():
                query = query.replace(key, val[replacement_i])

        if replace_with_view:
            # Transform it into a view beforehand, to avoid launching a very complex query multiple times needlessly
            return MSSQLDB().create_view_for_query(query)
        else:
            return query


def create_untemplated_yarrrml_file(content: str,
                                    sources: str,
                                    new_output_file_name: str,
                                    custom_yml_template: dict[str, list[str]] | None,
                                    replacement_i: int | None):
    placeholders = {
        '{prefixes}': prefixes,
        '{sources}': sources,
    }
    # For as many replacements as there are, replace contents with corresponding position *in the yml mapping*
    if custom_yml_template is not None:
        for key in custom_yml_template.keys():
            placeholders[key] = custom_yml_template[key][replacement_i]

    chunk_content = content
    for key, val in placeholders.items():
        chunk_content = chunk_content.replace(key, val)

    with open(new_output_file_name, 'w') as f:
        f.write(chunk_content)


def fill_template_values(templated_yml: str,
                         output_file_name: str,
                         custom_sql_template: dict[str, list[str]] | None,
                         custom_yml_template: dict[str, list[str]] | None,
                         convert_to_csv: bool=False) -> list[str] | list[tuple[str, str, str]]:
    """
    Given a YARRRML mapping file path, fetches and replaces the templated prefixes and corresponding SQL queries, writes
    it back in a non-templated form and returns their file path(s) as a list. If the below optional parameters are
    indicated, multiple output files will be written and returned and/or the return list will be of a different shape.

    Optional parameters:
    :param custom_yml_template: Dict composed of replacement keys and corresponding lists of possible replacement values.
                               All replacement value lists must have the same size. It will iterate through each possible
                               index in the replacement lists, replacing the key with the corresponding value each time
                               (e.g. iteration 0 will replace every key with element 0 of its replacement values list, etc.).
                               This is currently used to generate SQL queries discriminating against different type IDs in the RDMS.
    :param custom_sql_template: used to replace values in the SQL query, following the same strategy as custom_yml_template (See get_sql_query documentation)
    :convert_to_csv: Signals that the source will be a CSV file instead of the SQL query directly. This is mandatory if we want to use RMLStreamer for this mapping

    :returns: If convert_to_csv = False, returns a list of str containing the untemplated YARRRML files
              If convert_to_csv = True, returns a list of (untemplated YARRRML file path, SQL query to execute, CSV file path to store the SQL query results in)
              The SQL queries are then to be executed inside the materialization job. This is exclusively used for RMLStreamer
    """
    if convert_to_csv:
        output_file_names: list[tuple[str, str, str]] = []
    else:
        output_file_names: list[str] = []

    if custom_yml_template is not None:
        number_of_replacements = None

        for key in custom_yml_template.keys():
            if number_of_replacements is not None and len(custom_yml_template[key]) != number_of_replacements:
                raise ValueError(f"Number of replacements mismatch in template {key}")
            number_of_replacements = len(custom_yml_template[key])

        csv_files_to_create = []
        for replacement_i in range(number_of_replacements):
            query = get_sql_query(templated_yml,
                                  custom_sql_template,
                                  replacement_i,
                                  replace_with_view=False)
            if convert_to_csv:
                csv_file = output_file_name.replace(".yml", f"_{replacement_i}.csv")
            else:
                csv_file = None
            csv_files_to_create.append((query, csv_file))

        for replacement_i, (query, csv_file) in enumerate(csv_files_to_create):
            with open(templated_yml, 'r') as f:
                content = f.read()

            # Without SQL views, only used to validate the mappings consistency
            sources_for_validation = mssql_source.replace('{query}', query)

            # Validate it
            validate_mapping(output_file_name, sources_for_validation, content)

            if convert_to_csv:
                new_output_file_name = output_file_name.replace(".yml", f"_{replacement_i}.yml")

                sources = csv_source.replace('{csv_file}', csv_file)

                create_untemplated_yarrrml_file(content,
                                                sources,
                                                new_output_file_name,
                                                custom_yml_template,
                                                replacement_i)

                output_file_names.append((new_output_file_name, query, csv_file))

            else: # SQL query directly
                new_output_file_name = output_file_name.replace(".yml", f"_{replacement_i}.yml")
                sources = mssql_source.replace('{query}', query)

                create_untemplated_yarrrml_file(content, sources, new_output_file_name, custom_yml_template,
                                                replacement_i)

                output_file_names.append(new_output_file_name)

    else: # It's a single .yml
        with open(templated_yml, 'r') as f:
            content = f.read()

        query = get_sql_query(templated_yml,
                              custom_sql_template,
                              None,
                              replace_with_view=False)

        # Without SQL views, only used to validate the mappings consistency
        sources_for_validation = mssql_source.replace('{query}', query)

        # Validate it
        validate_mapping(output_file_name, sources_for_validation, content)

        if convert_to_csv:
            csv_file = output_file_name.replace(".yml", ".csv")

            sources = csv_source.replace('{csv_file}', csv_file)

            create_untemplated_yarrrml_file(content, sources, output_file_name, None, None)

            output_file_names.append((output_file_name, query, csv_file))
        else:
            sources = mssql_source.replace('{query}', query)

            create_untemplated_yarrrml_file(content, sources, output_file_name, None, None)

            output_file_names.append(output_file_name)

    return output_file_names
