"""
/// Summary : pytest fixtures for Use Case 1 unit tests. Provides a
///           session-scoped local SparkSession and puts the utilities
///           module on the import path so `revenue_logic` can be
///           imported without needing the Databricks DLT runtime.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession


UTILITIES_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "utilities",
    )
)
if UTILITIES_DIR not in sys.path:
    sys.path.insert(0, UTILITIES_DIR)


# Databricks sets DATABRICKS_RUNTIME_VERSION in every cluster environment.
# When present, we must NOT call session.stop() at teardown — stopping the
# cluster-wide SparkSession causes the notebook cell to hang for minutes
# waiting for Spark to shut down. Locally this variable is absent and we
# stop the session normally.
_ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("data-sentinels-uc1-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    if not _ON_DATABRICKS:
        session.stop()
