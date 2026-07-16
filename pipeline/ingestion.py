"""
Data Ingestion Module.

This module provides functions to load data from various sources such as CSV files,
JSON files, databases, and APIs. It includes validation and logging.
"""

import logging
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# Configure module-level logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_csv(file_path: str) -> pd.DataFrame:
    """
    Load data from a CSV file into a Pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing the loaded data.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is not a CSV file (based on extension).
        pd.errors.EmptyDataError: If the CSV file is empty.
        Exception: For other errors during loading.
    """
    path = Path(file_path)

    logger.info(f"Attempting to load CSV from {file_path}")

    # Validate file existence
    if not path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    # Validate file extension
    if path.suffix.lower() != ".csv":
        logger.error(f"Invalid file type: {file_path}. Expected a .csv file.")
        raise ValueError(f"The file {file_path} is not a CSV file.")

    try:
        # Load the CSV
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error(f"The CSV file is empty: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to load CSV from {file_path}: {e}")
        raise


def load_json(file_path: str) -> pd.DataFrame:
    """
    Load data from a JSON file into a Pandas DataFrame.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        pd.DataFrame: A DataFrame containing the loaded data.
    
    Raises:
        NotImplementedError: As this function is currently a placeholder.
    """
    logger.info(f"Attempting to load JSON from {file_path}")
    raise NotImplementedError("JSON loading is not yet implemented.")


def load_database(connection_string: str, query: str, **kwargs: Any) -> pd.DataFrame:
    """
    Load data from a database using a SQL query.

    Args:
        connection_string (str): The database connection string.
        query (str): The SQL query to execute.
        **kwargs: Additional arguments for the database connection or query execution.

    Returns:
        pd.DataFrame: A DataFrame containing the queried data.

    Raises:
        NotImplementedError: As this function is currently a placeholder.
    """
    logger.info("Attempting to load data from database.")
    raise NotImplementedError("Database loading is not yet implemented.")


def load_api(url: str, params: Optional[dict] = None, **kwargs: Any) -> pd.DataFrame:
    """
    Load data from an API endpoint.

    Args:
        url (str): The API endpoint URL.
        params (dict, optional): Query parameters for the API request.
        **kwargs: Additional arguments such as headers or authentication.

    Returns:
        pd.DataFrame: A DataFrame containing the loaded data.

    Raises:
        NotImplementedError: As this function is currently a placeholder.
    """
    logger.info(f"Attempting to load data from API endpoint: {url}")
    raise NotImplementedError("API loading is not yet implemented.")


if __name__ == "__main__":
    # Example usage demonstrating loading a CSV file.
    
    # Create a temporary CSV file for the example
    sample_csv_path = "sample_data.csv"
    try:
        logger.info(f"Creating a sample CSV file at {sample_csv_path} for testing.")
        pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "role": ["Engineer", "Analyst", "Manager"]
        }).to_csv(sample_csv_path, index=False)
        
        # Load the CSV
        df = load_csv(sample_csv_path)
        print("\nLoaded DataFrame:")
        print(df)
        
        # Test validation by trying to load a non-existent file
        try:
            load_csv("non_existent_file.csv")
        except FileNotFoundError as e:
            print(f"\nCaught expected exception: {e}")
            
    finally:
        # Clean up the temporary file
        if os.path.exists(sample_csv_path):
            os.remove(sample_csv_path)
            logger.info(f"Cleaned up sample CSV file at {sample_csv_path}.")
