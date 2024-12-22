import os
import requests
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Configuration
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Alpha Vantage API (Free API for historical data)
API_KEY = "your_alpha_vantage_api_key"
BASE_URL = "https://www.alphavantage.co/query"

def fetch_xauusd_data():
    """Fetch XAU/USD price data."""
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "XAUUSD",
        "apikey": API_KEY,
        "datatype": "json"
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if "Time Series (Daily)" in data:
            daily_data = data["Time Series (Daily)"]
            df = pd.DataFrame.from_dict(daily_data, orient="index")
            df.columns = ["open", "high", "low", "close", "volume"]
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            df = df.drop(columns=["volume"])
            df = df[df.index >= pd.Timestamp("2020-01-01")]
            return df
        else:
            print("Error in API response:", data)
            return None
    else:
        print("HTTP Error:", response.status_code)
        return None

def delete_old_rows():
    """Delete rows before 2020 from the database."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            cursor = connection.cursor()
            delete_query = "DELETE FROM xauusd_prices WHERE date < '2020-01-01';"
            cursor.execute(delete_query)
            connection.commit()
            print(f"Deleted rows before 2020 from xauusd_prices.")
    except Error as e:
        print("Error while deleting old rows:", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def save_to_mysql(df):
    """Save filtered data to MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            cursor = connection.cursor()
            # Create table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS xauusd_prices (
                    date DATE PRIMARY KEY,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT
                )
            """)
            connection.commit()
            
            # Insert data into MySQL
            for index, row in df.iterrows():
                query = """
                    INSERT INTO xauusd_prices (date, open, high, low, close)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    open = VALUES(open), high = VALUES(high),
                    low = VALUES(low), close = VALUES(close)
                """
                cursor.execute(query, (index.date(), row["open"], row["high"], row["low"], row["close"]))
            connection.commit()
            print("Data saved successfully!")
    except Error as e:
        print("Error while connecting to MySQL:", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    print("Deleting old data...")
    delete_old_rows()  # Clean up old data before insertion
    
    print("Fetching XAU/USD data...")
    data = fetch_xauusd_data()
    if data is not None:
        print("Saving filtered data to MySQL...")
        save_to_mysql(data)
    else:
        print("No data to save.")
