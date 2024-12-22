import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Configuration
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def get_data_from_db(query):
    """Fetch data from the database."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME  # Explicitly specify the database
        )
        if connection.is_connected():
            print(f"Connected to database: {DB_NAME}")
            # Fetch data into a Pandas DataFrame
            df = pd.read_sql(query, connection)
            return df
    except Error as e:
        print(f"Error while executing query: {query}")
        print("Error while connecting to the database:", e)
    finally:
        if connection.is_connected():
            connection.close()
            print("Database connection closed")
