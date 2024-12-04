import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables from .env file
load_dotenv()

# Retrieve MySQL connection details
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

class MySQLUtils:
    def __init__(self):
        """Initialize the MySQL utility class."""
        self.connection = self.connect_to_mysql()

    def connect_to_mysql(self):
        """Connect to MySQL and return the connection object."""
        try:
            connection = mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB
            )
            if connection.is_connected():
                print("Connected to MySQL successfully!")
                return connection
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            return None

    def insert_artifact(self, model_name, version, metrics, hyperparameters):
        """Insert artifact into MySQL table."""
        try:
            cursor = self.connection.cursor()
            sql = """
                INSERT INTO artifacts (model_name, version, created_at, metrics, hyperparameters)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                model_name,
                version,
                datetime.utcnow(),
                json.dumps(metrics),
                json.dumps(hyperparameters)
            ))
            self.connection.commit()
            print(f"Artifact inserted with ID: {cursor.lastrowid}")
        except Error as e:
            print(f"Error inserting artifact: {e}")

    def fetch_artifacts(self):
        """Fetch all artifacts from the table."""
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM artifacts")
            return cursor.fetchall()
        except Error as e:
            print(f"Error fetching artifacts: {e}")
            return []


