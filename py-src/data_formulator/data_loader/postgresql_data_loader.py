import json
import logging
import pandas as pd
import duckdb

from data_formulator.data_loader.external_data_loader import ExternalDataLoader, sanitize_table_name
from typing import Dict, Any

logger = logging.getLogger(__name__)

class PostgreSQLDataLoader(ExternalDataLoader):

    @staticmethod
    def list_params() -> bool:
        params_list = [
            {"name": "user", "type": "string", "required": True, "default": "postgres", "description": ""},
            {"name": "password", "type": "string", "required": False, "default": "", "description": "leave blank for no password"},
            {"name": "host", "type": "string", "required": True, "default": "localhost", "description": ""},
            {"name": "database", "type": "string", "required": True, "default": "postgres", "description": ""},
            {"name": "port", "type": "string", "required": False, "default": "5432", "description": "PostgreSQL port number"}
        ]
        return params_list

    def __init__(self, params: Dict[str, Any], duck_db_conn: duckdb.DuckDBPyConnection):
        self.params = params
        self.duck_db_conn = duck_db_conn

        logger.info(f"Initializing PostgreSQL connection with params: {json.dumps({k: v if k != 'password' else '***' for k, v in params.items()})}")

        # Install and load the PostgreSQL extension
        try:
            self.duck_db_conn.install_extension("postgres")
            self.duck_db_conn.load_extension("postgres")
            logger.info("PostgreSQL extension installed and loaded successfully")
        except Exception as e:
            error_msg = f"Failed to install/load PostgreSQL extension: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

        # Format connection string for PostgreSQL
        host = self.params.get('host', 'localhost')
        port = self.params.get('port', '5432')
        database = self.params.get('database', 'postgres')
        user = self.params.get('user', 'postgres')
        password = self.params.get('password', '')

        # Create connection URL
        connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        logger.info(f"Connection URL (masked): postgresql://{user}:***@{host}:{port}/{database}")

        # Detach existing postgresdb connection if it exists
        try:
            self.duck_db_conn.execute("DETACH postgresdb;")
            logger.info("Detached existing postgresdb connection")
        except:
            logger.info("No existing postgresdb connection to detach")

        try:
            # Register PostgreSQL connection with connection URL
            attach_query = f"ATTACH '{connection_url}' AS postgresdb (TYPE postgres)"
            logger.info(f"Executing attach query: {attach_query.replace(password, '***')}")
            self.duck_db_conn.execute(attach_query)
            logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            error_msg = f"Failed to connect to PostgreSQL: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def list_tables(self):
        tables_df = self.duck_db_conn.execute(f"""
            SELECT table_schema, table_name
            FROM postgresdb.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            AND table_type = 'BASE TABLE'
        """).fetch_df()

        results = []

        for schema, table_name in tables_df.values:
            full_table_name = f"postgresdb.{schema}.{table_name}"

            # Get column information using DuckDB's information schema
            columns_df = self.duck_db_conn.execute(f"DESCRIBE {full_table_name}").df()
            columns = [{
                'name': row['column_name'],
                'type': row['column_type']
            } for _, row in columns_df.iterrows()]

            # Get sample data
            sample_df = self.duck_db_conn.execute(f"SELECT * FROM {full_table_name} LIMIT 10").df()
            sample_rows = json.loads(sample_df.to_json(orient="records"))

            # get row count
            row_count = self.duck_db_conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0]

            table_metadata = {
                "row_count": row_count,
                "columns": columns,
                "sample_rows": sample_rows
            }

            results.append({
                "name": full_table_name,
                "metadata": table_metadata
            })

        return results

    def ingest_data(self, table_name: str, name_as: str | None = None, size: int = 1000000):
        # Create table in the main DuckDB database from PostgreSQL data
        if name_as is None:
            name_as = table_name.split('.')[-1]

        name_as = sanitize_table_name(name_as)

        self.duck_db_conn.execute(f"""
            CREATE OR REPLACE TABLE main.{name_as} AS
            SELECT * FROM {table_name}
            LIMIT {size}
        """)

    def view_query_sample(self, query: str) -> str:
        return self.duck_db_conn.execute(query).df().head(10).to_dict(orient="records")

    def ingest_data_from_query(self, query: str, name_as: str) -> pd.DataFrame:
        # Execute the query and get results as a DataFrame
        df = self.duck_db_conn.execute(query).df()
        # Use the base class's method to ingest the DataFrame
        self.ingest_df_to_duckdb(df, name_as)