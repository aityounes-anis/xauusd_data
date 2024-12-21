import requests
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# MySQL connection details
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

# Alpha Vantage API configuration
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://www.alphavantage.co/query"
SYMBOL = "XAUUSD"

# Fetch data from Alpha Vantage
def fetch_data():
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": SYMBOL,
        "market": "USD",
        "apikey": API_KEY,
        "outputsize": "full"  # Fetch full historical data
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    if "Time Series (Daily)" in data:
        return data["Time Series (Daily)"]
    else:
        print("Error fetching data:", data)
        return None

# Store data in MySQL
def store_data(data):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    for date, stats in data.items():
        try:
            cursor.execute("""
                INSERT INTO xauusd (_date, _open, _high, _low, _close, _volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                _open=%s, _high=%s, _low=%s, _close=%s, _volume=%s
            """, (
                datetime.strptime(date, "%Y-%m-%d"),
                float(stats["1. open"]),
                float(stats["2. high"]),
                float(stats["3. low"]),
                float(stats["4. close"]),
                float(stats.get("5. volume", 0)),  # Volume might not be provided
                # Update values
                float(stats["1. open"]),
                float(stats["2. high"]),
                float(stats["3. low"]),
                float(stats["4. close"]),
                float(stats.get("5. volume", 0)),
            ))
            conn.commit()
        except Exception as e:
            print(f"Error inserting data for {date}: {e}")
    cursor.close()
    conn.close()

# Main execution
if __name__ == "__main__":
    print("Fetching data from Alpha Vantage...")
    data = fetch_data()
    if data:
        print("Data fetched successfully! Storing in database...")
        store_data(data)
        print("Data storage complete!")
    else:
        print("No data to store.")
