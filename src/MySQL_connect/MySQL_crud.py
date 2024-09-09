import pymysql
import pandas as pd
from typing import List, Dict, Any, Optional
import logging

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class mysql_crud_operations:
    def __init__(self, host: str, user: str, password: str, database: str, 
                 port: int, use_ssl: bool = False, ssl_ca: Optional[str] = None):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.use_ssl = use_ssl
        self.ssl_ca = ssl_ca

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
        except pymysql.MySQLError as e:
            logger.error(f"Error connecting to the database: {e}")
            return None

    def execute_query(self, query: str, params: Optional[List[Any]] = None) -> None:
        connection = self.create_connection()
        if connection is not None:
            try:
                cursor = connection.cursor()
                cursor.execute(query, params)
                connection.commit()
                logger.info(f"Query executed: {query}")
            except pymysql.MySQLError as e:
                logger.error(f"An error occurred: {e}")
            finally:
                cursor.close()
                connection.close()

    def insert_record(self, table_name: str, data: Dict[str, Any]) -> None:
        keys = ", ".join(data.keys())
        values = list(data.values())  # Fix: Convert tuple to list
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
        values = list(conditions.values())  # Fix: Convert tuple to list
        query = f"SELECT * FROM {table_name} WHERE {condition_strings}"
        self.execute_query(query, values)

    def update_records(self, table_name: str, data: Dict[str, Any], 
                       conditions: Dict[str, Any]) -> None:
        set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
        condition_clause = " AND ".join([f"{key} = %s" for key in conditions.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {condition_clause}"
        values = list(data.values()) + list(conditions.values())  # Fix: Convert to list
        self.execute_query(query, values)

    def delete_records(self, table_name: str, conditions: Dict[str, Any]) -> None:
        condition_clause = " AND ".join([f"{key} = %s" for key in conditions.keys()])
        query = f"DELETE FROM {table_name} WHERE {condition_clause}"
        values = list(conditions.values())  # Fix: Convert tuple to list
        self.execute_query(query, values)

    def insert_in_bulk(self, datafile: str, table_name: str) -> None:
        if datafile.endswith(".csv"):
            data = pd.read_csv(datafile)
        elif datafile.endswith(".xlsx"):
            data = pd.read_excel(datafile)
        
        records = data.to_dict(orient='records')
        query = f"INSERT INTO {table_name} ({', '.join(records[0].keys())}) " \
                f"VALUES ({', '.join(['%s'] * len(records[0]))})"
        values = [list(record.values()) for record in records]  # Fix: Convert to list
        
        connection = self.create_connection()
        if connection is not None:
            try:
                cursor = connection.cursor()
                cursor.executemany(query, values)
                connection.commit()
                logger.info(f"Bulk insert completed for {len(records)} records.")
            finally:
                cursor.close()
                connection.close()
