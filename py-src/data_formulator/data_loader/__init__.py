from data_formulator.data_loader.external_data_loader import ExternalDataLoader
from data_formulator.data_loader.mysql_data_loader import MySQLDataLoader
from data_formulator.data_loader.kusto_data_loader import KustoDataLoader
from data_formulator.data_loader.postgresql_data_loader import PostgreSQLDataLoader

DATA_LOADERS = {
    "mysql": MySQLDataLoader,
    "kusto": KustoDataLoader,
    "postgresql": PostgreSQLDataLoader
}

__all__ = ["ExternalDataLoader", "MySQLDataLoader", "KustoDataLoader", "PostgreSQLDataLoader", "DATA_LOADERS"]