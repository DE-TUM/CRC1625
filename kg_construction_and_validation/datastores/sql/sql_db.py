import logging
import os
import shutil
import stat
import subprocess
import sys
import time
import uuid

import pandas as pd
import pymssql
from dotenv import load_dotenv
from sqlalchemy import create_engine

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(module_dir, '../../.env'))

MSSQL_HOST = os.environ.get("MSSQL_HOST")
MSSQL_PORT = os.environ.get("MSSQL_PORT")
MSSQL_USER = os.environ.get("MSSQL_USER")
MSSQL_PASSWORD = os.environ.get("MSSQL_PASSWORD")
MSSQL_CRC1625_DATABASE_NAME = os.environ.get("MSSQL_CRC1625_DATABASE_NAME")
MSSQL_MASTER_DATABASE_NAME = os.environ.get("MSSQL_MASTER_DATABASE_NAME")

class MSSQLDB():
    """
    Wrapper for a local MSSQL Docker container storing an instance of the CRC 1625 DB.

    Only one container is intended to be up at the same time in the same host.
    """
    DATA_DIR = os.path.join(module_dir, './db_data')
    ADDITIONAL_INDEXES_QUERY = open(os.path.join(module_dir, './additional_indexes.sql')).read()
    docker_file: str

    def _execute_query(self, query: str):
        """
        Executes a query. Does not return its result
        """
        conn = pymssql.connect(
            server=MSSQL_HOST,
            port=MSSQL_PORT,
            user=MSSQL_USER,
            password=MSSQL_PASSWORD,
            database=MSSQL_CRC1625_DATABASE_NAME,
        )
        cursor = conn.cursor()
        cursor.execute(query)

        conn.commit()
        cursor.close()
        conn.close()

    def query_to_csv(self, query: str, csv_filename: str):
        """
        Executes a query and writes results to a CSV file
        """

        df = pd.read_sql(query, create_engine('mssql+pymssql://SA:DebugPassword123%40@localhost:1433/RUB_INF'))
        str_cols = df.select_dtypes(include=['object', 'string']).columns
        df[str_cols] = df[str_cols].replace({r'[\r\n]+': ' '}, regex=True)


        df.to_csv(csv_filename, index=False, encoding='utf-8')

    def clear_data_dir(self):
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)
            # Set o+w in the folder (for the docker user)
            os.chmod(self.DATA_DIR, os.stat(self.DATA_DIR).st_mode | stat.S_IWOTH)

        for item in os.listdir(self.DATA_DIR):
            item_path = os.path.join(self.DATA_DIR, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)

    def select_and_start_db(self, db_option: str | None =None):
        """
        Selects and starts the selected SQL database container
        If `db_option` == None, then the choice will be requested via CLI
        Possible values:
            Production DB dumps:
            - 'm': Main CRC1625 DB dump (only available within the CRC)

            Validation DB dumps, containing a manually created instance for testing the mappings:
            - 'v': Validation DB dump, main test
            - 'v_1': Validation DB dump, subtest 1
            - 'v_2': Validation DB dump, subtest 2
            - 'v_3': Validation DB dump, subtest 3
            - 'v_4': Validation DB dump, subtest 3

            Other DB dumps:
            - 'c': Clear DB dump, containing no data. Used for the performance tests
        """
        options = {
            'm': 'RUB_CRC1625.bak',
            'v': 'RUB_INF_validation.bak',
            'v_1': 'initial_work_with_handovers_in_same_group.bak',
            'v_2': 'user_within_multiple_projects.bak',
            'v_3': 'isolated_handover_between_groups_in_same_project.bak',
            'v_4': 'isolated_handover_between_long_groups_in_same_project.bak',
            'v_5': 'handover_groups_with_linked_measurements_to_handovers.bak',
            'c': 'RUB_Clear.bak'
        }

        if db_option is None:
            while True:
                choice = input(
                    """Please choose the DB to use
                    Possible values:
                        Production DB dumps:
                        - 'm': Main CRC1625 DB dump (only available within the CRC)
                        
                        Validation DB dumps, containing a manually created instance for testing the mappings:
                        - 'v': Validation DB dump, main test 
                        - 'v_1': Validation DB dump, subtest 1
                        - 'v_2': Validation DB dump, subtest 2
                        - 'v_3': Validation DB dump, subtest 3
                        - 'v_4': Validation DB dump, subtest 4
                        - 'v_5': Validation DB dump, subtest 5
                        
                        Other DB dumps:
                        - 'c': Clear DB dump, containing no data. Used for the performance tests
                    """).strip().lower()

                if choice in options:
                    os.environ["MSSQL_BAK_FILE_NAME"] = options[choice]
                    break
                else:
                    logging.error(f"Invalid option '{choice}'")

        elif db_option in list(options.keys()):
            # Used by the MSSQL dockerfile
            os.environ["MSSQL_BAK_FILE_NAME"] = options[db_option]
        else:
            raise ValueError(f"An option for the database was already indicated, but the value was unknown: {db_option}")

        logging.info("Starting MSSQL container...")

        self.clear_data_dir()

        subprocess.run(
            ["docker-compose", "-f", "docker_compose_mssql.yml", "down", "--volumes", "--remove-orphans"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=module_dir
        )
        subprocess.run(
            ["docker-compose", "-f", "docker_compose_mssql.yml", "up", "--detach", "--force-recreate"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=module_dir
        )

        time.sleep(30) # Allow MSSQL to create the DB and allow connections

        # Create additional indexes for better performance
        self._execute_query(self.ADDITIONAL_INDEXES_QUERY)

    def stop_DB(self):
        """
        Stops the DB container and deletes all of its data
        """
        logging.info("Stopping and removing MSSQL container...")
        subprocess.run(
            ["docker-compose", "-f", "docker_compose_mssql.yml", "down", "--volumes", "--remove-orphans"],
            check=True,
            cwd=module_dir
        )

    def create_view_for_query(self, query: str) -> (str, str):
        """
        Creates a materialized view of the provided SQL query, returning the view name
        and the SQL query to be used to query it instead

        Not used anymore, as we ensure that mappings related to the same query are materialized together
        """

        materialized_view_name = f"view_{uuid.uuid4().hex[:8]}"
        self._execute_query(query.replace("FROM", f"INTO {materialized_view_name} FROM", 1))

        return materialized_view_name, f"SELECT * FROM {materialized_view_name}"

    def remove_views(self, view_names: list[str]):
        """
        Deletes all provided materialized views that were previously created
        via create_view_for_query
        """
        for view_name in view_names:
            self._execute_query(f"DROP TABLE {view_name};")

    def database_backup_exists(self, identifier):
        """
        Returns True if a database backup with the given identifier exists, False otherwise
        """
        return os.path.exists(os.path.join(module_dir, f'./db_dumps/{identifier}.bak'))

    def dump_database(self, identifier):
        """
        Dumps the database as a .bak file, stored in the container's backups folder
        """
        conn = pymssql.connect(
            server=MSSQL_HOST,
            port=MSSQL_PORT,
            user=MSSQL_USER,
            password=MSSQL_PASSWORD,
            database=MSSQL_CRC1625_DATABASE_NAME,
        )
        cursor = conn.cursor()

        cursor.execute(f"""
            BACKUP DATABASE RUB_INF
            TO DISK = '/var/opt/mssql/backup/{identifier}.bak'
            WITH FORMAT,
                MEDIANAME = 'SQLServerBackups',
                NAME = 'Full Backup of RUB_INF';
        """)

        cursor.close()
        conn.close()

    def restore_database(self, identifier):
        """
        Restores the database from a .bak file contained in the container's backups folder
        """
        if not self.database_backup_exists(identifier):
            return ValueError(f"The DB backup {identifier} is not present in the backups folder")

        conn = pymssql.connect(
            server=MSSQL_HOST,
            port=MSSQL_PORT,
            user=MSSQL_USER,
            password=MSSQL_PASSWORD,
            database=MSSQL_CRC1625_DATABASE_NAME,
        )
        cursor = conn.cursor()

        cursor.execute(f"""
            RESTORE DATABASE [RUB_INF] 
            FROM DISK = '/var/opt/mssql/backup/{identifier}.bak' 
            WITH MOVE 'RUB_INF' TO '/var/opt/mssql/data/RUB_INF.mdf', 
                 MOVE 'RUB_INF_log' TO '/var/opt/mssql/data/RUB_INF_log.ldf';
        """)

        cursor.close()
        conn.close()

    def execute_bulk_insert(self,
                            table: str,
                            headers : str,
                            records: str | list[str]):
        """
        Executes a BATCH INSERT on the DB, given the name of the table, a string representing
        the columns of the table in .csv format and a record or list of records as lines in a .csv

        The columns and records must follow the same order as in the DB
        """
        if isinstance(records, str):
            records = [records]

        conn = pymssql.connect(
            server=MSSQL_HOST,
            port=MSSQL_PORT,
            user=MSSQL_USER,
            password=MSSQL_PASSWORD,
            database=MSSQL_CRC1625_DATABASE_NAME,
        )
        cursor = conn.cursor()

        with open(os.path.join(module_dir, './db_dumps/bulk_insert_records.csv'), "w", newline="", encoding="utf-8") as f:
            f.write(headers)
            f.write("\n")
            for record in records:
                f.write(record.replace(" ", ""))
                f.write("\n")

        os.chmod(os.path.join(module_dir, './db_dumps/bulk_insert_records.csv'),
                 os.stat(os.path.join(module_dir, './db_dumps/bulk_insert_records.csv')).st_mode | stat.S_IROTH)

        cursor.execute(f"""
                BULK INSERT {table}
                FROM '/var/opt/mssql/backup/bulk_insert_records.csv'
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '0x0A',
                    KEEPNULLS,
                    TABLOCK
                );
            """)

        cursor.close()
        conn.close()

        os.remove(os.path.join(module_dir, './db_dumps/bulk_insert_records.csv'))


if __name__ == "__main__":
    db = MSSQLDB()

    docker_file = db.select_and_start_db()

    input("Press Enter to stop the DB...")

    db.stop_DB()

