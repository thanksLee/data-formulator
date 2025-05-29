import duckdb
import pandas as pd
from typing import Dict
import tempfile
import os
import platform
from contextlib import contextmanager
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

class DuckDBManager:
    def __init__(self, local_db_dir: str):
        # Store session db file paths
        self._db_files: Dict[str, str] = {}

        # Handle path for cross-platform compatibility
        if local_db_dir:
            # Convert to Path object and resolve to absolute path
            path = Path(local_db_dir)
            # Handle relative paths
            if not path.is_absolute():
                # Get the current working directory
                cwd = Path.cwd()
                path = cwd / path
            # Normalize path separators and resolve any symlinks
            self._local_db_dir = str(path.resolve())
        else:
            self._local_db_dir = None

        print(f"=== Initializing DuckDBManager with local_db_dir: {self._local_db_dir}")
        print(f"=== Platform: {platform.system()} {platform.release()}")

    @contextmanager
    def connection(self, session_id: str):
        """Get a DuckDB connection as a context manager that will be closed when exiting the context"""
        conn = None
        try:
            conn = self.get_connection(session_id)
            yield conn
        finally:
            if conn:
                conn.close()

    def get_connection(self, session_id: str) -> duckdb.DuckDBPyConnection:
        """Internal method to get or create a DuckDB connection for a session"""
        try:
            # Get or create the db file path for this session
            if session_id not in self._db_files or self._db_files[session_id] is None:
                # Use local_db_dir if set, otherwise use temp directory
                db_dir = self._local_db_dir if self._local_db_dir else tempfile.gettempdir()

                # Ensure directory exists with proper permissions
                try:
                    os.makedirs(db_dir, exist_ok=True)
                    # Test write permissions
                    test_file = Path(db_dir) / '.write_test'
                    test_file.touch()
                    test_file.unlink()
                except (OSError, PermissionError) as e:
                    print(f"=== Warning: Could not write to {db_dir}, falling back to temp directory")
                    db_dir = tempfile.gettempdir()

                # Create database file path
                db_file = Path(db_dir) / f"df_{session_id}.duckdb"
                db_file = str(db_file.resolve())
                print(f"=== Creating new db file: {db_file}")
                self._db_files[session_id] = db_file
            else:
                print(f"=== Using existing db file: {self._db_files[session_id]}")
                db_file = self._db_files[session_id]

            # Create a fresh connection to the database file
            conn = duckdb.connect(database=db_file)
            return conn
        except Exception as e:
            print(f"=== Error creating DuckDB connection: {str(e)}")
            raise

# Initialize the DB manager
db_manager = DuckDBManager(
    local_db_dir=os.getenv('LOCAL_DB_DIR')
)