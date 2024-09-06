from typing import Any, Dict, List, Optional
import os
import pandas as pd
import pymysql
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class mysql_crud_operations:
    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.database = os.getenv('DB_NAME')
        self.port = int(os.getenv('DB_PORT', 3306))
        self.use_ssl = os.getenv('USE_SSL', 'False') == 'True'
        self.ssl_ca = os.getenv('SSL_CA', None)

    def create_connection(self):
        ssl_settings = {'ca': self.ssl_ca} if self.use_ssl else None
        try:
            connection = pymysql.connect(
                host=self.host,
                user=self.user,
                passwd=self.password,
                db=self.database,
                port=self.port,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                ssl=ssl_settings
            )
            logger.info("Database connection successful!")
            return connection
        except pymysql.Error as e:
            logger.error(f"Error connecting to the database: {e}")

    def execute_query(self, query: str, params: Optional[List[Any]] = None) -> None:
        connection = self.create_connection()
        if connection is not None:
            try:
                cursor = connection.cursor()
                cursor.execute(query, params)
                connection.commit()
            except pymysql.Error as e:
                logger.error(f"An error occurred: {e}")
            finally:
                cursor.close()
                connection.close()

    def insert_record(self, table_name: str, data: Dict[str, Any]) -> None:
        keys = ", ".join(data.keys())
        values = tuple(data.values())
        placeholders = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table_name} ({keys}) VALUES ({placeholders})"
        self.execute_query(query, values)

    def find_all_records(self, table_name: str) -> None:
        query = f"SELECT * FROM {table_name}"
        connection = self.create_connection()
        if connection is not None:
            try:
                cursor = connection.cursor()
                cursor.execute(query)
                records = cursor.fetchall()
                for record in records:
                    logger.info(record)
            finally:
                cursor.close()
                connection.close()

    def find_records(self, table_name: str, conditions: Dict[str, Any]) -> None:
        condition_strings = " AND ".join([f"{k} = %s" for k in conditions.keys()])
        values = tuple(conditions.values())
        query = f"SELECT * FROM {table_name} WHERE {condition_strings}"
        self.execute_query(query, values)

    def update_records(self, table_name: str, data: Dict[str, Any], conditions: Dict[str, Any]) -> None:
        set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
        condition_clause = " AND ".join([f"{key} = %s" for key in conditions.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {condition_clause}"
        values = list(data.values()) + list(conditions.values())
        self.execute_query(query, values)

    def delete_records(self, table_name: str, conditions: Dict[str, Any]) -> None:
        condition_clause = " AND ".join([f"{key} = %s" for key in conditions.keys()])
        query = f"DELETE FROM {table_name} WHERE {condition_clause}"
        values = tuple(conditions.values())
        self.execute_query(query, values)

    def insert_in_bulk(self, datafile: str, table_name: str) -> None:
        if datafile.endswith(".csv"):
            data = pd.read_csv(datafile)
        elif datafile.endswith(".xlsx"):
            data = pd.read_excel(datafile)
        
        records = data.to_dict(orient='records')
        for record in records:
            self.insert_record(table_name, record)

