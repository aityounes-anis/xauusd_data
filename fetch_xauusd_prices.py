import os
import requests
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MySQL Configuration
MYSQL_HOST = os.getenv("DB_HOST")
MYSQL_USER = os.getenv("DB_USER")
MYSQL_PASSWORD = os.getenv("DB_PASSWORD")
MYSQL_DATABASE = os.getenv("DB_NAME")

# Alpha Vantage API (Free API for historical data)
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

# Fetch XAU/USD price data
def fetch_xauusd_data():
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "XAUUSD",
        "apikey": API_KEY,
        "datatype": "json",
        "outputsize": "full"
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if "Time Series (Daily)" in data:
            daily_data = data["Time Series (Daily)"]
            # Parse JSON into DataFrame
            df = pd.DataFrame.from_dict(daily_data, orient="index")
            df.columns = ["open", "high", "low", "close", "volume"]
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()

            return df
        else:
            print("Error in API response:", data)
            return None
    else:
        print("HTTP Error:", response.status_code)
        return None

# Save DataFrame to MySQL
def save_to_mysql(df):
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
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
                cursor.execute("""
                    INSERT INTO xauusd_prices (date, open, high, low, close)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    open = VALUES(open), high = VALUES(high),
                    low = VALUES(low), close = VALUES(close)
                """, (index.date(), row["open"], row["high"], row["low"], row["close"]))
            connection.commit()
            print("Data saved successfully!")
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Main Function
if __name__ == "__main__":
    print("Fetching XAU/USD data...")
    data = fetch_xauusd_data()
    if data is not None:
        print("Saving data to MySQL...")
        save_to_mysql(data)
    else:
        print("No data to save.")
