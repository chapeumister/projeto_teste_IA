import os
import sqlite3
import tempfile
import shutil
import pytest
from pathlib import Path # Added for Path object usage
import sys # For sys.modules manipulation
from unittest.mock import MagicMock # For creating mock objects
from sports_prediction_ai.src import database_setup # Assuming database_setup.py is in src

# --- Global Mock for Kaggle API to prevent OSError and ModuleNotFound errors during test collection ---
# This code runs when conftest.py is first loaded by pytest, before test collection fully begins.
try:
    # Create a mock for the 'kaggle.api' module/object
    mock_kaggle_api_for_init = MagicMock()
    mock_kaggle_api_for_init.authenticate = MagicMock(return_value=None) # Ensure authenticate does nothing

    # Create a mock for the top-level 'kaggle' module
    mock_kaggle_module = MagicMock()
    mock_kaggle_module.api = mock_kaggle_api_for_init # kaggle.api will point to our mock
    mock_kaggle_module.KaggleApi = MagicMock(return_value=mock_kaggle_api_for_init)

    # Mock kaggle.rest and its ApiException
    mock_kaggle_rest = MagicMock()
    # Create a dummy ApiException class that inherits from Exception and can take a message.
    def dummy_api_exception_init(self, msg=None, status=0, reason=None, http_resp=None, body=None, headers=None):
        super(type(self), self).__init__(msg)
        self.status = status
        self.reason = reason
        self.http_resp = http_resp
        self.body = body
        self.headers = headers

    dummy_api_exception_class = type('ApiException', (Exception,), {
        '__init__': dummy_api_exception_init
    })
    mock_kaggle_rest.ApiException = dummy_api_exception_class
    mock_kaggle_module.rest = mock_kaggle_rest # Assign mocked 'rest' to the mocked 'kaggle' module

    # Put the entirely mocked 'kaggle' module and its sub-parts into sys.modules
    sys.modules['kaggle'] = mock_kaggle_module
    sys.modules['kaggle.api'] = mock_kaggle_api_for_init
    sys.modules['kaggle.rest'] = mock_kaggle_rest

    print("INFO: sys.modules['kaggle'], ['kaggle.api'], ['kaggle.rest'] globally mocked in conftest.py.")
except Exception as e:
    # If kaggle library isn't installed at all, these mocks might not be needed or might fail.
    # This is a defensive catch.
    print(f"WARNING: Failed to set up global Kaggle mocks in conftest.py (may be ignorable if kaggle lib not present): {e}")
# --- End Global Mock for Kaggle API ---


@pytest.fixture(scope="session", autouse=True)
def setup_dummy_kaggle_config(): # Removed request, not strictly needed if TemporaryDirectory handles cleanup
    original_config_dir = os.environ.get("KAGGLE_CONFIG_DIR")
    temp_dir_obj = tempfile.TemporaryDirectory(prefix="pytest-kaggle-session-")
    temp_dir_path = temp_dir_obj.name # Get the path string from the object

    os.environ["KAGGLE_CONFIG_DIR"] = temp_dir_path
    print(f"INFO: KAGGLE_CONFIG_DIR set to temporary directory: {temp_dir_path}")

    dummy_json_path = Path(temp_dir_path) / "kaggle.json"
    try:
        with open(dummy_json_path, "w") as f:
            f.write('{"username":"testuser","key":"testkey"}')
        print(f"INFO: Dummy kaggle.json created at {dummy_json_path}")
    except Exception as e:
        print(f"ERROR: Failed to create dummy kaggle.json at {dummy_json_path}: {e}")

    yield # Run the test session

    # Teardown
    print(f"INFO: Cleaning up temporary Kaggle config directory: {temp_dir_path}")
    # temp_dir_obj.cleanup() is implicitly called when the TemporaryDirectory object goes out of scope / is garbage collected.
    # No explicit call needed here as temp_dir_obj will be cleaned up when the fixture scope ends.

    if original_config_dir is None:
        if "KAGGLE_CONFIG_DIR" in os.environ: # Check if it was set by us
            del os.environ["KAGGLE_CONFIG_DIR"]
            print("INFO: KAGGLE_CONFIG_DIR environment variable removed.")
    else:
        os.environ["KAGGLE_CONFIG_DIR"] = original_config_dir
        print(f"INFO: KAGGLE_CONFIG_DIR restored to: {original_config_dir}")


@pytest.fixture(scope="session")
def tmp_db_session(): # Renamed to avoid conflict if a function-scoped tmp_db is needed
    """Creates a temporary directory and a file-based SQLite DB for a test session."""
    tmpdir = tempfile.mkdtemp(prefix="pytest-db-")
    db_path = os.path.join(tmpdir, database_setup.DB_NAME if hasattr(database_setup, 'DB_NAME') else "test_session.db")

    conn_fixture = None
    try:
        conn_fixture = sqlite3.connect(db_path)
        # Pass the connection to the modified create_database function
        database_setup.create_database(db_connection=conn_fixture)
        print(f"Temporary session DB created at: {db_path} with schema.")
    except Exception as e:
        print(f"Error creating temporary session DB at {db_path}: {e}")
        if conn_fixture:
            conn_fixture.close()
        shutil.rmtree(tmpdir)
        raise
    finally:
        if conn_fixture:
            conn_fixture.close()

    yield db_path

    try:
        shutil.rmtree(tmpdir)
        print(f"Temporary session DB directory removed: {tmpdir}")
    except Exception as e:
        print(f"Error removing temporary session DB directory {tmpdir}: {e}")

@pytest.fixture(scope="function")
def in_memory_db():
    """Creates an in-memory SQLite DB for a single test, applying the schema."""
    conn = sqlite3.connect(":memory:")
    try:
        # Pass the connection to the modified create_database function
        database_setup.create_database(db_connection=conn)
        print("In-memory DB created with schema for function scope.")
        yield conn
    except Exception as e:
        print(f"Error creating in-memory DB for test function: {e}")
        conn.close()
        raise
    finally:
        if conn:
            conn.close()
            print("In-memory DB connection closed for function scope.")
