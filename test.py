import unittest
from src.MySQL_crud import MySQLCRUDOperations
import pymysql


class TestMySQLCRUDOperations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up a connection to a test database
        cls.db = MySQLCRUDOperations(host='localhost', user='test', password='test', database='testdb')

    @classmethod
    def tearDownClass(cls):
        # Close database connection
        cls.db.close_connection()

    def setUp(self):
        # Create tables or setup database state before each test
        self.db.execute_query("CREATE TEMPORARY TABLE test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))")

    def tearDown(self):
        # Drop tables or clean database state after each test
        self.db.execute_query("DROP TABLE IF EXISTS test_table")

    def test_insert_record(self):
        # Test insertion of a record
        self.db.insert_record('test_table', {'name': 'John'})
        result = self.db.find_records('test_table', {'name': 'John'})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'John')

    def test_update_record(self):
        # Test updating a record
        self.db.insert_record('test_table', {'name': 'John'})
        self.db.update_records('test_table', {'name': 'Jane'}, {'name': 'John'})
        result = self.db.find_records('test_table', {'name': 'Jane'})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Jane')

    def test_delete_record(self):
        # Test deletion of a record
        self.db.insert_record('test_table', {'name': 'John'})
        self.db.delete_records('test_table', {'name': 'John'})
        result = self.db.find_records('test_table', {'name': 'John'})
        self.assertEqual(len(result), 0)

    def test_insert_duplicate_record(self):
        # Test error handling for duplicate records
        self.db.insert_record('test_table', {'name': 'John'})
        with self.assertRaises(pymysql.err.IntegrityError):
            self.db.insert_record('test_table', {'name': 'John'})
